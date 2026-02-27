// Command s3db is a CLI for operating s3db databases from the shell.
//
// # Addressing
//
// All commands accept either an s3:// URL as the first positional argument:
//
//	s3db pull s3://my-bucket/mydb/ -o db.sqlite
//
// Or explicit -bucket/-prefix flags:
//
//	s3db pull -bucket my-bucket -prefix mydb/ -o db.sqlite
//
// # Creating a database
//
//	# Empty database
//	s3db init s3://my-bucket/mydb/
//
//	# From an existing SQLite file
//	s3db init s3://my-bucket/mydb/ -f seed.sqlite
//
//	# Or: push with -create (shortcut for init-then-push)
//	s3db push s3://my-bucket/mydb/ -f seed.sqlite -create
//
// # Pull/edit/push workflow
//
//	# Download current state. Writes db.sqlite and db.sqlite.seq.
//	s3db pull s3://my-bucket/mydb/ -o db.sqlite
//
//	# Edit with any SQLite tool
//	sqlite3 db.sqlite "UPDATE users SET name='bob' WHERE id=1;"
//
//	# Push back. Reads db.sqlite.seq to detect concurrent writes.
//	s3db push s3://my-bucket/mydb/ -f db.sqlite
//
// If someone wrote between pull and push, push fails with a seq-mismatch
// error. Re-pull, re-apply your edits, push again. Or use -force.
//
// # Maintenance
//
//	s3db status  s3://my-bucket/mydb/
//	s3db compact s3://my-bucket/mydb/
//	s3db gc      s3://my-bucket/mydb/ [-grace DUR]
//
// S3 configuration comes from standard AWS environment variables
// (AWS_REGION, AWS_ACCESS_KEY_ID, etc.) plus AWS_ENDPOINT_URL for
// S3-compatible backends like MinIO or R2.
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
	case "init":
		err = cmdInit(os.Args[2:])
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
  s3db init    <s3://bucket/prefix/> [-f FILE]         Create DB (empty or from FILE)
  s3db pull    <s3://bucket/prefix/> -o FILE           Download DB to FILE
  s3db push    <s3://bucket/prefix/> -f FILE [-create] [-force]
                                                       Upload FILE as new DB state
  s3db status  <s3://bucket/prefix/>                   Show seq, schema, log size
  s3db compact <s3://bucket/prefix/>                   Compact snapshot+log
  s3db gc      <s3://bucket/prefix/> [-grace DUR]      Delete unreferenced blobs

All commands also accept -bucket B -prefix P instead of an s3:// URL.

S3 config from env: AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_SESSION_TOKEN, AWS_ENDPOINT_URL (for MinIO/R2/etc.)

Run 's3db <cmd> -h' for per-command flags.
`)
}

// --- addressing --------------------------------------------------------------

// target holds a resolved bucket+prefix pair. Commands build one from either
// an s3:// URL positional arg or explicit -bucket/-prefix flags.
type target struct {
	bucket string
	prefix string
}

// parseTarget consumes the first positional argument as an s3:// URL if
// present, otherwise requires -bucket and -prefix flags. The URL form takes
// precedence; flags are ignored if a URL is given.
//
// The prefix is normalized to always end with "/".
func parseTarget(fs *flag.FlagSet, bucket, prefix *string) (target, error) {
	// Check positional args for an s3:// URL.
	if fs.NArg() > 0 {
		arg := fs.Arg(0)
		if strings.HasPrefix(arg, "s3://") {
			b, p, err := parseS3URL(arg)
			if err != nil {
				return target{}, err
			}
			return target{bucket: b, prefix: p}, nil
		}
		return target{}, fmt.Errorf("unexpected positional argument %q (did you mean s3://%s ?)", arg, arg)
	}

	// Fall back to flags.
	if *bucket == "" || *prefix == "" {
		return target{}, errors.New("missing target: provide s3://bucket/prefix/ or -bucket + -prefix")
	}
	p := *prefix
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return target{bucket: *bucket, prefix: p}, nil
}

// parseS3URL parses "s3://bucket/key/prefix/" into bucket and prefix.
// The prefix is normalized to end with "/".
//
// Bucket-root URLs (s3://bucket or s3://bucket/) are rejected — s3db
// always operates under a named prefix, and deploying at the root is
// almost never intentional.
func parseS3URL(u string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(u, "s3://")
	if !ok {
		return "", "", fmt.Errorf("not an s3:// URL: %q", u)
	}
	// rest is "bucket" or "bucket/" or "bucket/path/to/prefix/"
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("s3:// URL missing bucket: %q", u)
	}
	if prefix == "" {
		return "", "", fmt.Errorf("s3:// URL missing prefix (e.g. s3://%s/mydb/): %q", bucket, u)
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix, nil
}

// --- init --------------------------------------------------------------------

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	file := fs.String("f", "", "SQLite file to use as initial state (optional; default is empty DB)")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	if err := s3db.Init(ctx, store, tgt.prefix, *file); err != nil {
		return err
	}

	if *file != "" {
		fmt.Printf("Initialized %s from %s\n", tgt, *file)
	} else {
		fmt.Printf("Initialized %s (empty)\n", tgt)
	}
	return nil
}

// --- pull --------------------------------------------------------------------

func cmdPull(args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	out := fs.String("o", "", "output .sqlite file path (required)")
	noSeq := fs.Bool("no-seq", false, "don't write the .seq sidecar file")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}
	if *out == "" {
		return errors.New("missing -o (output path); see 's3db pull -h'")
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	info, err := s3db.Pull(ctx, store, tgt.prefix, *out)
	if err != nil {
		return err
	}

	// Write the seq to a sidecar file so push can read it automatically.
	if !*noSeq {
		seqPath := *out + ".seq"
		if err := os.WriteFile(seqPath, []byte(strconv.FormatInt(info.Seq, 10)), 0644); err != nil {
			return fmt.Errorf("write seq sidecar: %w", err)
		}
	}

	fmt.Printf("Pulled %s to %s\n", tgt, *out)
	fmt.Printf("  seq:            %d\n", info.Seq)
	fmt.Printf("  schema_version: %d\n", info.SchemaVersion)
	fmt.Printf("  snapshot size:  %s\n", formatBytes(info.SnapshotSize))
	fmt.Printf("  log entries:    %d\n", info.LogEntries)
	return nil
}

// --- push --------------------------------------------------------------------

func cmdPush(args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	file := fs.String("f", "", "SQLite file to push (required)")
	seq := fs.Int64("seq", -2, "expected seq (default: read <file>.seq; -1 to skip check)")
	force := fs.Bool("force", false, "skip seq check (DANGEROUS: overwrites concurrent writes)")
	create := fs.Bool("create", false, "create the database if it doesn't exist")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}
	if *file == "" {
		return errors.New("missing -f (file to push); see 's3db push -h'")
	}

	expectedSeq := *seq
	if *force {
		expectedSeq = -1
	} else if expectedSeq == -2 {
		// Try to read from sidecar.
		seqPath := *file + ".seq"
		data, serr := os.ReadFile(seqPath)
		if serr != nil {
			if *create {
				// No sidecar + -create is fine: either the DB doesn't
				// exist (Init will create it) or it does (Push will
				// fail with seq mismatch, which is the right signal).
				expectedSeq = 0
			} else {
				return fmt.Errorf("no -seq given and couldn't read %s: %w\n"+
					"  (use -seq to specify, -create if the DB is new, or -force to skip the check)", seqPath, serr)
			}
		} else {
			v, perr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", seqPath, perr)
			}
			expectedSeq = v
		}
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	err = s3db.Push(ctx, store, tgt.prefix, *file, expectedSeq)
	if errors.Is(err, s3db.ErrNotFound) && *create {
		// DB doesn't exist. Create it from the file.
		if ierr := s3db.Init(ctx, store, tgt.prefix, *file); ierr != nil {
			return ierr
		}
		fmt.Printf("Created %s from %s\n", tgt, *file)
		// Write sidecar at seq 0.
		seqPath := *file + ".seq"
		os.WriteFile(seqPath, []byte("0"), 0644)
		return nil
	}
	if err != nil {
		if errors.Is(err, s3db.ErrSeqMismatch) {
			fmt.Fprintln(os.Stderr, "\nThe database has advanced since your last pull.")
			fmt.Fprintln(os.Stderr, "Pull again to get the latest, re-apply your changes, then push.")
			fmt.Fprintln(os.Stderr, "Or use -force to overwrite (DANGEROUS).")
		}
		return err
	}

	// Update the sidecar.
	if expectedSeq >= 0 {
		seqPath := *file + ".seq"
		if _, err := os.Stat(seqPath); err == nil {
			os.WriteFile(seqPath, []byte(strconv.FormatInt(expectedSeq, 10)), 0644)
		}
	}

	fmt.Printf("Pushed %s to %s\n", *file, tgt)
	if expectedSeq >= 0 {
		fmt.Printf("  seq: %d (unchanged; push is a snapshot replacement)\n", expectedSeq)
	}
	return nil
}

// --- status ------------------------------------------------------------------

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	db, err := s3db.Open(ctx, store, tgt.prefix, s3db.WithSchemaUnchecked())
	if err != nil {
		return err
	}
	defer db.Close()

	s := db.Stats()
	fmt.Printf("target:         %s\n", tgt)
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
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	db, err := s3db.Open(ctx, store, tgt.prefix, s3db.WithSchemaUnchecked())
	if err != nil {
		return err
	}
	defer db.Close()

	before := db.Stats()
	if err := db.Compact(ctx); err != nil {
		return err
	}
	after := db.Stats()

	fmt.Printf("Compacted %s\n", tgt)
	fmt.Printf("  log entries:   %d → %d\n", before.LogEntries, after.LogEntries)
	fmt.Printf("  snapshot size: %s → %s\n", formatBytes(before.SnapshotSize), formatBytes(after.SnapshotSize))
	return nil
}

// --- gc ----------------------------------------------------------------------

func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	bucket := fs.String("bucket", "", "S3 bucket name (alternative to s3:// URL)")
	prefix := fs.String("prefix", "", "key prefix ending in / (alternative to s3:// URL)")
	grace := fs.Duration("grace", -1, "grace period (-1 = library default 5m, 0 = disabled)")
	fs.Parse(args)

	tgt, err := parseTarget(fs, bucket, prefix)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store := newStore(tgt.bucket)

	opts := []s3db.Option{s3db.WithSchemaUnchecked()}
	if *grace >= 0 {
		opts = append(opts, s3db.WithGCGracePeriod(*grace))
	}

	db, err := s3db.Open(ctx, store, tgt.prefix, opts...)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.GC(ctx); err != nil {
		return err
	}
	fmt.Printf("GC complete for %s\n", tgt)
	return nil
}

// --- helpers -----------------------------------------------------------------

func (t target) String() string {
	return "s3://" + t.bucket + "/" + t.prefix
}

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
