package s3db

import (
	"context"
	"fmt"
	"io"
	"os"

	"zombiezen.com/go/sqlite"
)

// downloadSnapshot streams a snapshot from the store to a local file at
// destPath. Any existing file at destPath is replaced atomically by writing
// to a temp sibling and renaming — a crash mid-download leaves the old file
// intact (or no file, if there was none).
//
// The caller is responsible for closing any SQLite connection on destPath
// before calling this, and reopening after.
func downloadSnapshot(ctx context.Context, store BlobStore, key, destPath string) error {
	rc, _, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("download snapshot %s: %w", key, err)
	}
	defer rc.Close()

	tmp, err := os.CreateTemp("", "s3db-snap-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	// If anything fails after this point, make sure the temp file is gone.
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		return fmt.Errorf("download snapshot %s: copy: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, destPath)
}

// applyLog fetches and applies every log entry with Seq > fromSeq to conn,
// in order. Used for incremental sync when the local DB is already at or
// past the manifest's snapshot seq.
//
// Log entries are small (changesets are typically KB-sized), so they are
// buffered in memory before applying — this keeps the store Get window
// short and the changeset apply logic simple.
//
// Returns the seq of the last applied entry, or fromSeq if no entries were
// applied (log was empty or all entries were at or before fromSeq).
func applyLog(ctx context.Context, store BlobStore, conn *sqlite.Conn, log []LogEntry, fromSeq int64) (int64, error) {
	seq := fromSeq
	for _, entry := range log {
		if entry.Seq <= fromSeq {
			continue
		}

		rc, _, err := store.Get(ctx, entry.Key)
		if err != nil {
			return seq, fmt.Errorf("fetch changeset seq=%d key=%s: %w", entry.Seq, entry.Key, err)
		}
		cs, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return seq, fmt.Errorf("read changeset seq=%d: %w", entry.Seq, err)
		}

		if err := applyChangeset(conn, cs, conflictAbort); err != nil {
			return seq, fmt.Errorf("apply changeset seq=%d: %w", entry.Seq, err)
		}
		seq = entry.Seq
	}
	return seq, nil
}
