// Command s3db is a CLI for operating s3db databases from the shell.
//
// The primary workflow is pull → edit → push:
//
//	# Download the current state to a local file
//	s3db pull -bucket my-bucket -prefix mydb/ -o db.sqlite
//
//	# Edit with any SQLite tool
//	sqlite3 db.sqlite "UPDATE users SET name = 'bob' WHERE id = 1;"
//
//	# Push back. The seq is recorded in db.sqlite.seq by pull;
//	# push reads it automatically to detect concurrent writes.
//	s3db push -bucket my-bucket -prefix mydb/ -f db.sqlite
//
// If someone wrote to the database between pull and push, push fails with
// a seq-mismatch error. Re-pull to get their changes, re-apply your edits,
// and push again.
//
// Other commands:
//
//	s3db status   Show seq, schema version, log size
//	s3db compact  Replace snapshot+log with a fresh snapshot
//	s3db gc       Delete unreferenced blobs
//
// S3 configuration comes from standard AWS environment variables
// (AWS_REGION, AWS_ACCESS_KEY_ID, etc.) plus AWS_ENDPOINT_URL for
// S3-compatible backends like MinIO.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/psanford/s3db"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "pull":
		err = cmdPull(os.Args[2:])
	case "push":
		err = cmdPush(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "compact":
		err = cmdCompact(os.Args[2:])
	case "gc":
		err = cmdGC(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `s3db — CLI for s3db databases

Usage:
  s3db pull    -bucket B -prefix P -o FILE            Download DB to FILE
  s3db push    -bucket B -prefix P -f FILE [-force]   Upload FILE as new DB state
  s3db status  -bucket B -prefix P                    Show seq, schema, log size
  s3db compact -bucket B -prefix P                    Compact snapshot+log
  s3db gc      -bucket B -prefix P [-grace DUR]       Delete unreferenced blobs

S3 config from env: AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_SESSION_TOKEN, AWS_ENDPOINT_URL (for MinIO/R2/etc.)

Run 's3db <cmd> -h' for per-command flags.
`)
}

// --- pull --------------------------------------------------------------------

func cmdPull(args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "key prefix ending in / (required)")
	out := fs.String("o", "", "output .sqlite file path (required)")
	noSeq := fs.Bool("no-seq", false, "don't write the .seq sidecar file")
	fs.Parse(args)

	if *bucket == "" || *prefix == "" || *out == "" {
		fs.Usage()
		return errors.New("missing required flags")
	}

	ctx := context.Background()
	store := newStore(*bucket)

	info, err := s3db.Pull(ctx, store, *prefix, *out)
	if err != nil {
		return err
	}

	// Write the seq to a sidecar file so push can read it automatically.
	// This lets the pull→edit→push workflow "just work" without the user
	// having to track seq manually.
	if !*noSeq {
		seqPath := *out + ".seq"
		if err := os.WriteFile(seqPath, []byte(strconv.FormatInt(info.Seq, 10)), 0644); err != nil {
			return fmt.Errorf("write seq sidecar: %w", err)
		}
	}

	fmt.Printf("Pulled %s to %s\n", *prefix, *out)
	fmt.Printf("  seq:            %d\n", info.Seq)
	fmt.Printf("  schema_version: %d\n", info.SchemaVersion)
	fmt.Printf("  snapshot size:  %s\n", formatBytes(info.SnapshotSize))
	fmt.Printf("  log entries:    %d\n", info.LogEntries)
	return nil
}

// --- push --------------------------------------------------------------------

func cmdPush(args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "key prefix ending in / (required)")
	file := fs.String("f", "", "SQLite file to push (required)")
	seq := fs.Int64("seq", -2, "expected seq (defaults to reading <file>.seq; -1 to skip check)")
	force := fs.Bool("force", false, "skip seq check (DANGEROUS: overwrites concurrent writes)")
	fs.Parse(args)

	if *bucket == "" || *prefix == "" || *file == "" {
		fs.Usage()
		return errors.New("missing required flags")
	}

	expectedSeq := *seq
	if *force {
		expectedSeq = -1
	} else if expectedSeq == -2 {
		// Try to read from sidecar.
		seqPath := *file + ".seq"
		data, err := os.ReadFile(seqPath)
		if err != nil {
			return fmt.Errorf("no -seq given and couldn't read %s: %w\n"+
				"  (use -seq to specify manually, or -force to skip the check)", seqPath, err)
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return fmt.Errorf("parse %s: %w", seqPath, err)
		}
		expectedSeq = v
	}

	ctx := context.Background()
	store := newStore(*bucket)

	if err := s3db.Push(ctx, store, *prefix, *file, expectedSeq); err != nil {
		if errors.Is(err, s3db.ErrSeqMismatch) {
			fmt.Fprintln(os.Stderr, "\nThe database has advanced since your last pull.")
			fmt.Fprintln(os.Stderr, "Pull again to get the latest, re-apply your changes, then push.")
			fmt.Fprintln(os.Stderr, "Or use -force to overwrite (DANGEROUS).")
		}
		return err
	}

	// Update the sidecar so a subsequent push without an intervening pull
	// sees the correct state: our push didn't change seq (it's a snapshot
	// replacement), so the same seq is still valid.
	if expectedSeq >= 0 {
		seqPath := *file + ".seq"
		// Best effort — only update if it exists.
		if _, err := os.Stat(seqPath); err == nil {
			os.WriteFile(seqPath, []byte(strconv.FormatInt(expectedSeq, 10)), 0644)
		}
	}

	fmt.Printf("Pushed %s to %s\n", *file, *prefix)
	if expectedSeq >= 0 {
		fmt.Printf("  seq: %d (unchanged; push is a snapshot replacement)\n", expectedSeq)
	}
	return nil
}

// --- status ------------------------------------------------------------------

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "key prefix ending in / (required)")
	fs.Parse(args)

	if *bucket == "" || *prefix == "" {
		fs.Usage()
		return errors.New("missing required flags")
	}

	ctx := context.Background()
	store := newStore(*bucket)

	db, err := s3db.Open(ctx, store, *prefix)
	if err != nil {
		return err
	}
	defer db.Close()

	s := db.Stats()
	fmt.Printf("seq:            %d\n", s.Seq)
	fmt.Printf("schema_version: %d\n", s.SchemaVersion)
	fmt.Printf("snapshot size:  %s\n", formatBytes(s.SnapshotSize))
	fmt.Printf("log entries:    %d\n", s.LogEntries)
	fmt.Printf("log bytes:      %s\n", formatBytes(s.LogBytes))
	return nil
}

// --- compact -----------------------------------------------------------------

func cmdCompact(args []string) error {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "key prefix ending in / (required)")
	fs.Parse(args)

	if *bucket == "" || *prefix == "" {
		fs.Usage()
		return errors.New("missing required flags")
	}

	ctx := context.Background()
	store := newStore(*bucket)

	db, err := s3db.Open(ctx, store, *prefix)
	if err != nil {
		return err
	}
	defer db.Close()

	before := db.Stats()
	if err := db.Compact(ctx); err != nil {
		return err
	}
	after := db.Stats()

	fmt.Printf("Compacted %s\n", *prefix)
	fmt.Printf("  log entries:   %d → %d\n", before.LogEntries, after.LogEntries)
	fmt.Printf("  snapshot size: %s → %s\n", formatBytes(before.SnapshotSize), formatBytes(after.SnapshotSize))
	return nil
}

// --- gc ----------------------------------------------------------------------

func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (required)")
	prefix := fs.String("prefix", "", "key prefix ending in / (required)")
	grace := fs.Duration("grace", 0, "grace period for snapshot deletion (0 = use library default of 5m)")
	fs.Parse(args)

	if *bucket == "" || *prefix == "" {
		fs.Usage()
		return errors.New("missing required flags")
	}

	ctx := context.Background()
	store := newStore(*bucket)

	var opts []s3db.Option
	if *grace != 0 {
		opts = append(opts, s3db.WithGCGracePeriod(*grace))
	}

	db, err := s3db.Open(ctx, store, *prefix, opts...)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.GC(ctx); err != nil {
		return err
	}
	fmt.Printf("GC complete for %s\n", *prefix)
	return nil
}

// --- helpers -----------------------------------------------------------------

func newStore(bucket string) *s3db.S3BlobStore {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	client := awss3.New(awss3.Options{}, func(o *awss3.Options) {
		o.Region = region
		if ep := os.Getenv("AWS_ENDPOINT_URL"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
		}
		if ak := os.Getenv("AWS_ACCESS_KEY_ID"); ak != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				ak,
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				os.Getenv("AWS_SESSION_TOKEN"),
			)
		}
	})
	return s3db.NewS3BlobStore(client, bucket)
}

func formatBytes(n int64) string {
	switch {
	case n == 0:
		return "unknown"
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
