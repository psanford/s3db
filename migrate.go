package s3db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite/sqlitex"
)

// runMigrations applies any pending migrations. Each migration is a forced
// compaction: run the migration's Up function on the local DB, build a
// snapshot, and CAS the manifest with the new snapshot and bumped schema
// version. This handles DDL correctly (DDL doesn't replicate through
// changesets — see DESIGN.md).
//
// Safe under concurrent callers: if two Opens race to run migration N,
// one wins the CAS; the other sees the bumped schema_version on retry
// and skips ahead. No coordination needed beyond the manifest CAS.
//
// Returns ErrSchemaTooNew if the database is ahead of this client's
// migrations — upgrade your code.
//
// Called with db.mu held.
func (db *DB) runMigrations(ctx context.Context) error {
	migs := db.opts.migrations

	// Validate version ordering.
	last := 0
	for _, m := range migs {
		if m.Version <= last {
			return fmt.Errorf("s3db: migrations must have strictly increasing versions, got %d after %d", m.Version, last)
		}
		last = m.Version
	}

	maxVer := 0
	if len(migs) > 0 {
		maxVer = migs[len(migs)-1].Version
	}

	// lastAttemptedVer tracks which migration version we last tried to
	// apply. If we try the same version again (CAS lost to a regular
	// writer, schema_version unchanged), we need a full refresh to
	// discard the dirty local state from the previous Up() run.
	lastAttemptedVer := -1

	for {
		// Re-check the manifest at the top of each loop iteration — a
		// concurrent migrator may have advanced schema_version while we
		// were working.
		m, etag, err := loadManifest(ctx, db.cfg.store, db.cfg.manifestKey)
		if err != nil {
			return err
		}

		if m.SchemaVersion > maxVer {
			return fmt.Errorf("%w: database at schema v%d, client knows up to v%d",
				ErrSchemaTooNew, m.SchemaVersion, maxVer)
		}

		if m.SchemaVersion == maxVer {
			// All migrations applied. Sync local state and return.
			db.st.manifest = m
			db.st.etag = etag
			return syncToManifest(ctx, &db.cfg, &db.st, db.localPath)
		}

		// Find the next migration to apply.
		var next *Migration
		for i := range migs {
			if migs[i].Version > m.SchemaVersion {
				next = &migs[i]
				break
			}
		}
		if next == nil {
			// Shouldn't happen given the checks above.
			return fmt.Errorf("s3db: internal error: no migration found for schema v%d → v%d",
				m.SchemaVersion, maxVer)
		}

		// A migration is retried when the CAS loser was a regular writer
		// (schema_version unchanged, same mig selected again). Force a
		// full refresh on retry to discard the dirty already-ran-Up state.
		forceRefresh := next.Version == lastAttemptedVer
		lastAttemptedVer = next.Version

		if err := db.applyMigration(ctx, m, etag, next, forceRefresh); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				// Someone else advanced the manifest. Loop back to re-check.
				continue
			}
			return err
		}
		// Migration applied; loop to check for more.
	}
}

// applyMigration runs one migration: sync to current state, run Up, build
// snapshot, CAS manifest with bumped schema_version. Returns
// ErrPreconditionFailed if the CAS loses (caller should re-check and retry
// or skip).
//
// The forceRefresh flag triggers a full snapshot re-download before running
// Up. This is REQUIRED on CAS retry (when a regular writer committed between
// our sync and CAS — see issue #1) but wasteful on the first attempt when
// local state is already clean.
func (db *DB) applyMigration(ctx context.Context, m *manifest, etag string, mig *Migration, forceRefresh bool) error {
	db.st.manifest = m
	db.st.etag = etag
	if forceRefresh {
		// Discard whatever is in the local file. If the CAS loser was a
		// regular writer (not a concurrent migrator), the snapshot key
		// is unchanged, so incremental sync would keep our dirty
		// already-ran-Up local state. Clearing snapshotKey forces the
		// needRefresh branch in syncToManifest.
		db.st.snapshotKey = ""
	}
	if err := syncToManifest(ctx, &db.cfg, &db.st, db.localPath); err != nil {
		return err
	}

	// Run the migration. Up may do DDL, DML, or both. We don't use a
	// session/changeset here — the whole DB becomes the new snapshot.
	if err := mig.Up(db.st.conn); err != nil {
		return fmt.Errorf("migration v%d (%s): %w", mig.Version, mig.Name, err)
	}

	// VACUUM to produce a clean snapshot file.
	if err := sqlitex.Execute(db.st.conn, "VACUUM", nil); err != nil {
		return fmt.Errorf("migration v%d: vacuum: %w", mig.Version, err)
	}

	// Upload the new snapshot.
	if err := db.st.conn.Close(); err != nil {
		return fmt.Errorf("migration v%d: close for upload: %w", mig.Version, err)
	}
	snapKey := fmt.Sprintf("%ssnapshots/snap-mig-%d-%s.sqlite", db.cfg.prefix, mig.Version, uuid.NewString())
	snapSize, uerr := uploadFile(ctx, db.cfg.store, snapKey, db.localPath)
	if uerr != nil {
		db.reopenConn()
		return fmt.Errorf("migration v%d: upload snapshot: %w", mig.Version, uerr)
	}
	if err := db.reopenConn(); err != nil {
		return fmt.Errorf("migration v%d: reopen: %w", mig.Version, err)
	}

	// CAS the manifest. Snapshot.Seq is the SAME as before (migrations
	// don't advance seq — seq tracks data changes, schema_version tracks
	// schema changes). Actually — if the migration did DML (data
	// backfill), those changes ARE in the snapshot but not counted in
	// seq. That's fine: seq is the changeset-log clock, and we're
	// bypassing the log entirely. A reader at seq N sees the same DATA
	// before and after migration as long as they load the right snapshot,
	// which the manifest guarantees.
	newSnap := blobRef{Key: snapKey, Seq: m.Seq, Size: snapSize}
	newManifest := &manifest{
		Seq:           m.Seq,
		SchemaVersion: mig.Version,
		Snapshot:      newSnap,
		Log:           nil,
	}
	newEtag, err := putManifest(ctx, db.cfg.store, db.cfg.manifestKey, newManifest, PutCondition{IfMatch: etag})
	if err != nil {
		return err // may be ErrPreconditionFailed — caller handles
	}

	db.st.manifest = newManifest
	db.st.etag = newEtag
	db.st.localSeq = m.Seq
	db.st.snapshotKey = snapKey
	return nil
}
