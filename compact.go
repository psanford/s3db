package s3db

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Compact builds a fresh snapshot from the current state and replaces the
// manifest with one that points at it and has an empty log. The sequence
// number is unchanged — compaction doesn't advance the logical version.
//
// If a writer commits during compaction (manifest CAS fails), Compact
// retries: it re-syncs to include the new changeset, rebuilds the snapshot,
// and tries again. After maxRetries attempts it gives up with ErrConflict.
//
// Compaction never blocks writers and never loses work. If it's abandoned
// mid-way, the uploaded snapshot blob is an orphan that GC will clean up.
//
// Compact holds the DB mutex for its entire duration. For long-running
// compactions (large databases), consider calling it from a dedicated
// goroutine or scheduled Lambda rather than inline with user requests.
func (db *DB) Compact(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked(ctx)
}

// compactLocked is Compact's implementation, callable from contexts that
// already hold db.mu (e.g. the auto-compact hook in Update).
func (db *DB) compactLocked(ctx context.Context) error {
	for attempt := 0; attempt < db.cfg.maxRetries; attempt++ {
		// Ensure we're at current state.
		if err := refreshManifest(ctx, &db.cfg, &db.st, db.localPath); err != nil {
			return err
		}

		if len(db.st.manifest.Log) == 0 {
			// Nothing to compact.
			return nil
		}

		// VACUUM to defragment and shrink. This also forces a full file
		// rewrite, ensuring no WAL remnants or freelist bloat in the
		// snapshot. Must run outside a transaction.
		if err := sqlitex.Execute(db.st.conn, "VACUUM", nil); err != nil {
			return fmt.Errorf("compact: vacuum: %w", err)
		}

		// Upload the local file as the new snapshot. We close the conn
		// briefly to ensure all pages are flushed, upload, then reopen.
		// (SQLite's file can be read while a conn is open, but closing
		// is the simplest way to guarantee a consistent on-disk image
		// without WAL considerations.)
		if err := db.st.conn.Close(); err != nil {
			return fmt.Errorf("compact: close for upload: %w", err)
		}

		snapKey := fmt.Sprintf("%ssnapshots/snap-%s.sqlite", db.cfg.prefix, uuid.NewString())
		snapSize, err := uploadFile(ctx, db.cfg.store, snapKey, db.localPath)
		if err != nil {
			// Try to recover the conn before returning.
			db.reopenConn()
			return fmt.Errorf("compact: upload snapshot: %w", err)
		}

		if err := db.reopenConn(); err != nil {
			return fmt.Errorf("compact: reopen after upload: %w", err)
		}

		// CAS the manifest. Seq stays the same; snapshot points at the new
		// blob; log is empty. SchemaVersion preserved via WithSnapshot.
		newSnap := BlobRef{Key: snapKey, Seq: db.st.manifest.Seq, Size: snapSize}
		newManifest := db.st.manifest.WithSnapshot(newSnap)
		newEtag, err := putManifest(ctx, db.cfg.store, db.cfg.manifestKey, newManifest, PutCondition{IfMatch: db.st.etag})

		if err == nil {
			db.st.manifest = newManifest
			db.st.etag = newEtag
			db.st.snapshotKey = snapKey
			// localSeq already equals newManifest.Seq (we synced before).
			return nil
		}

		if !errors.Is(err, ErrPreconditionFailed) {
			return fmt.Errorf("compact: put manifest: %w", err)
		}

		// 412: a writer committed during compaction. Loop back to refresh
		// and try again including their changes. The uploaded snapshot blob
		// is now an orphan — GC will clean it up.
	}

	return fmt.Errorf("compact: %w", ErrConflict)
}

// uploadFile streams a local file to the store at the given key and returns
// its size in bytes.
func uploadFile(ctx context.Context, store BlobStore, key, path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if _, err := store.Put(ctx, key, f, NoCondition); err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// reopenConn reopens db.st.conn on db.localPath. Used after operations that
// require closing the connection (snapshot download, compaction upload).
func (db *DB) reopenConn() error {
	conn, err := openLocalConn(db.localPath)
	if err != nil {
		return err
	}
	db.st.conn = conn
	return nil
}
