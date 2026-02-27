package s3db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
)

// Init creates a new database at the given prefix. If srcPath is non-empty,
// that SQLite file becomes the initial snapshot; otherwise an empty database
// is created.
//
// Returns an error wrapping ErrPreconditionFailed if a database already
// exists at the prefix. Safe under concurrent callers — exactly one Init
// succeeds; others see ErrPreconditionFailed.
//
// Init does NOT validate that srcPath's schema matches any migrations —
// it just uploads the bytes. If your application uses migrations, either
// Init with srcPath="" and let the first Open run them, or be aware that
// the resulting manifest will have schema_version=0.
func Init(ctx context.Context, store BlobStore, prefix, srcPath string) error {
	if !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("s3db: prefix must end with /, got %q", prefix)
	}

	manifestKey := prefix + "manifest.json"

	// Pre-check: if the manifest already exists, fail fast before doing
	// any uploads. The If-None-Match on putManifest below is the real
	// guard (covers races); this just avoids orphaning a snapshot blob
	// in the common "oops, already exists" case.
	if _, err := store.Stat(ctx, manifestKey); err == nil {
		return fmt.Errorf("init: database already exists at %s: %w", prefix, ErrPreconditionFailed)
	} else if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("init: stat manifest: %w", err)
	}

	var snapKey string
	var snapSize int64

	if srcPath != "" {
		// Initialize from the given file.
		if err := validateSQLiteFile(srcPath); err != nil {
			return fmt.Errorf("init: %w", err)
		}
		snapKey = fmt.Sprintf("%ssnapshots/snap-init-%s.sqlite", prefix, uuid.NewString())
		var err error
		snapSize, err = uploadFile(ctx, store, snapKey, srcPath)
		if err != nil {
			return fmt.Errorf("init: upload snapshot: %w", err)
		}
	} else {
		// Empty DB. Reuse bootstrap's empty-file creation, but handle
		// the manifest CAS ourselves so we can report "already exists"
		// consistently (bootstrap swallows the race and re-loads).
		snapKey = prefix + "snapshots/snap-init.sqlite"
		var err error
		snapSize, err = uploadEmptySQLite(ctx, store, snapKey)
		if err != nil {
			return fmt.Errorf("init: %w", err)
		}
	}

	m := &manifest{
		Seq:      0,
		Snapshot: blobRef{Key: snapKey, Seq: 0, Size: snapSize},
	}
	if _, err := putManifest(ctx, store, manifestKey, m, PutCondition{IfNoneMatch: true}); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			return fmt.Errorf("init: database already exists at %s (concurrent init): %w", prefix, err)
		}
		return fmt.Errorf("init: put manifest: %w", err)
	}
	return nil
}

// PullInfo describes the state of a pulled database file. Pass Seq to Push
// to guard against overwriting concurrent changes.
type PullInfo struct {
	Seq           int64
	SchemaVersion int
	SnapshotSize  int64
	LogEntries    int
}

// Pull downloads the current database state to destPath as a single SQLite
// file, with all changesets from the log applied on top of the snapshot.
// The resulting file is a fully-materialized, standalone database — you can
// open it with the sqlite3 CLI, DB Browser, or any SQLite tool.
//
// The returned PullInfo.Seq should be passed to Push to detect concurrent
// writes. Think of it as the "base revision" for an edit/merge workflow.
//
// Pull does NOT hold any locks or open connections after it returns.
func Pull(ctx context.Context, store BlobStore, prefix, destPath string) (PullInfo, error) {
	if !strings.HasSuffix(prefix, "/") {
		return PullInfo{}, fmt.Errorf("s3db: prefix must end with /, got %q", prefix)
	}

	m, _, err := loadManifest(ctx, store, prefix+"manifest.json")
	if err != nil {
		return PullInfo{}, err
	}

	// Download the snapshot...
	if err := downloadSnapshot(ctx, store, m.Snapshot.Key, m.Snapshot.Size, destPath); err != nil {
		return PullInfo{}, err
	}

	// ...and replay the log on top. Open the file briefly to apply.
	if len(m.Log) > 0 {
		conn, err := openLocalConn(destPath)
		if err != nil {
			return PullInfo{}, fmt.Errorf("pull: open for replay: %w", err)
		}
		_, aerr := applyLog(ctx, store, conn, m.Log, m.Snapshot.Seq)
		cerr := conn.Close()
		if aerr != nil {
			return PullInfo{}, aerr
		}
		if cerr != nil {
			return PullInfo{}, fmt.Errorf("pull: close after replay: %w", cerr)
		}
	}

	return PullInfo{
		Seq:           m.Seq,
		SchemaVersion: m.SchemaVersion,
		SnapshotSize:  m.Snapshot.Size,
		LogEntries:    len(m.Log),
	}, nil
}

// Push uploads srcPath as the new database state, replacing the current
// snapshot and clearing the log (like a manual compaction).
//
// expectedSeq guards against overwriting concurrent writes: it must match the
// current manifest's Seq. Pass the Seq returned by Pull. If the database has
// advanced since you pulled, Push returns ErrSeqMismatch and you should
// re-pull, re-apply your edits, and retry.
//
// To skip the seq check (DANGEROUS — any concurrent writes since your last
// pull will be silently discarded), pass expectedSeq = -1.
//
// Push does not advance Seq (it's a snapshot replacement, not a logical
// write) and does not change SchemaVersion. The pushed file should match
// the current schema — Push does NOT validate this. Pushing a file with a
// different schema will break subsequent clients.
func Push(ctx context.Context, store BlobStore, prefix, srcPath string, expectedSeq int64) error {
	if !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("s3db: prefix must end with /, got %q", prefix)
	}

	// Sanity-check that srcPath is a valid SQLite file before we upload
	// it. Better to fail here than to commit a corrupt snapshot.
	if err := validateSQLiteFile(srcPath); err != nil {
		return fmt.Errorf("push: %w", err)
	}

	manifestKey := prefix + "manifest.json"

	m, etag, err := loadManifest(ctx, store, manifestKey)
	if err != nil {
		return err
	}

	if expectedSeq >= 0 && m.Seq != expectedSeq {
		return fmt.Errorf("%w: database is at seq %d, expected %d (pull again to get concurrent changes)",
			ErrSeqMismatch, m.Seq, expectedSeq)
	}

	// Upload the file as a new snapshot.
	snapKey := fmt.Sprintf("%ssnapshots/snap-push-%s.sqlite", prefix, uuid.NewString())
	snapSize, err := uploadFile(ctx, store, snapKey, srcPath)
	if err != nil {
		return fmt.Errorf("push: upload snapshot: %w", err)
	}

	// CAS the manifest: new snapshot, empty log, seq and schema unchanged.
	newManifest := m.withSnapshot(blobRef{Key: snapKey, Seq: m.Seq, Size: snapSize})
	if _, err := putManifest(ctx, store, manifestKey, newManifest, PutCondition{IfMatch: etag}); err != nil {
		if errors.Is(err, ErrPreconditionFailed) {
			// Someone committed between our loadManifest and CAS. This
			// is the same race that expectedSeq guards against, but
			// happening in the CAS window rather than the pull→push
			// window. Report it the same way.
			return fmt.Errorf("%w: database advanced during push; retry", ErrSeqMismatch)
		}
		return fmt.Errorf("push: put manifest: %w", err)
	}

	return nil
}

// validateSQLiteFile checks that path is a readable SQLite database. It
// opens a connection and runs a quick integrity check. Returns an error if
// the file is missing, unreadable, not a SQLite file, or corrupt.
func validateSQLiteFile(path string) error {
	conn, err := sqlite.OpenConn(path, sqlite.OpenReadOnly)
	if err != nil {
		return fmt.Errorf("not a valid SQLite file: %w", err)
	}
	defer conn.Close()

	// PRAGMA quick_check is much faster than integrity_check for large
	// databases while still catching structural corruption.
	stmt, _, err := conn.PrepareTransient("PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("quick_check prepare: %w", err)
	}
	defer stmt.Finalize()

	hasRow, err := stmt.Step()
	if err != nil {
		return fmt.Errorf("quick_check: %w", err)
	}
	if !hasRow {
		return fmt.Errorf("quick_check returned no rows")
	}
	result := stmt.ColumnText(0)
	if result != "ok" {
		return fmt.Errorf("integrity check failed: %s", result)
	}
	return nil
}
