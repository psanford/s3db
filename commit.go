package s3db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// commitState holds the mutable state tracked across commit attempts.
type commitState struct {
	conn        *sqlite.Conn // local SQLite connection
	localSeq    int64        // seq that conn is currently at
	snapshotKey string       // which snapshot conn was loaded from
	manifest    *manifest    // last-seen manifest
	etag        string       // ETag of last-seen manifest
}

// commitConfig holds the immutable parameters for a commit.
type commitConfig struct {
	store       BlobStore
	prefix      string // e.g. "mydb/" — trailing slash required
	manifestKey string // prefix + "manifest.json"
	maxRetries  int    // CAS attempts before returning ErrConflict
	schemaVer   int    // this client's expected schema version
}

// syncToManifest brings st.conn up to the state described by st.manifest.
// Handles both the incremental case (same snapshot, apply log tail) and
// the full-refresh case (different snapshot or localSeq behind snapshot,
// download snapshot and replay full log).
//
// A full refresh is needed when:
//   - localSeq < snapshot.seq (compaction advanced the snapshot), or
//   - snapshotKey != manifest.snapshot.key (migration swapped the snapshot
//     without advancing seq)
//
// The full-refresh path requires closing and reopening conn, so this may
// replace st.conn.
func syncToManifest(ctx context.Context, cfg *commitConfig, st *commitState, localPath string) error {
	m := st.manifest

	needRefresh := st.localSeq < m.Snapshot.Seq || st.snapshotKey != m.Snapshot.Key
	if needRefresh {
		if err := st.conn.Close(); err != nil {
			return fmt.Errorf("sync: close for refresh: %w", err)
		}
		if err := downloadSnapshot(ctx, cfg.store, m.Snapshot.Key, m.Snapshot.Size, localPath); err != nil {
			return err
		}
		conn, err := openLocalConn(localPath)
		if err != nil {
			return fmt.Errorf("sync: reopen after refresh: %w", err)
		}
		st.conn = conn
		st.localSeq = m.Snapshot.Seq
		st.snapshotKey = m.Snapshot.Key
	}

	// applyLog returns the seq it successfully advanced to, even on error.
	// Assign before the error check so localSeq always reflects conn's
	// actual state. Otherwise a partial apply (e.g. corrupt changeset
	// midway through) would leave conn ahead of localSeq, and the next
	// sync would try to re-apply already-applied changesets — which
	// conflicts on their own before-images and wedges the DB.
	seq, err := applyLog(ctx, cfg.store, st.conn, m.Log, st.localSeq)
	st.localSeq = seq
	if err != nil {
		return err
	}
	return nil
}

// refreshManifest fetches the current manifest and syncs local state to it.
// Returns ErrSchemaMismatch/ErrSchemaTooNew if the manifest's schema version
// doesn't match cfg.schemaVer — fail fast before any sync work.
func refreshManifest(ctx context.Context, cfg *commitConfig, st *commitState, localPath string) error {
	m, etag, err := loadManifest(ctx, cfg.store, cfg.manifestKey)
	if err != nil {
		return err
	}
	if m.SchemaVersion != cfg.schemaVer {
		if m.SchemaVersion > cfg.schemaVer {
			return fmt.Errorf("%w: database at schema v%d, client at v%d",
				ErrSchemaTooNew, m.SchemaVersion, cfg.schemaVer)
		}
		return fmt.Errorf("%w: database at schema v%d, client at v%d",
			ErrSchemaMismatch, m.SchemaVersion, cfg.schemaVer)
	}
	st.manifest = m
	st.etag = etag
	return syncToManifest(ctx, cfg, st, localPath)
}

// doUpdate is the core commit loop. It assumes st already reflects current
// state (caller has refreshed the manifest and synced conn).
//
// Each iteration of the loop is one CAS attempt. The loop state machine:
//
//	needCapture=true  → SAVEPOINT, run fn, capture changeset, upload blob
//	needCapture=false → conn still has fn's changes from a previous
//	                    iteration's clean rebase; just retry CAS
//
// On 412:
//   - If rebase is clean (other writers' changesets don't conflict with
//     fn's changes), keep fn's changes, set needCapture=false, loop.
//   - If rebase conflicts, ROLLBACK fn's changes, re-sync conn, set
//     needCapture=true, loop (fn will be re-invoked).
//
// After maxRetries CAS attempts, return ErrConflict.
func doUpdate(ctx context.Context, cfg *commitConfig, st *commitState, localPath string, fn func(*sqlite.Conn) error) (err error) {
	var (
		needCapture = true
		cs          []byte
		csKey       string
		release     func(*error)
		orphan      string // previous blob orphaned by needCapture retry
	)

	// If we return early while a SAVEPOINT is open, roll it back. This
	// covers the error paths below — we set err before returning and
	// release(&err) does ROLLBACK when *err is non-nil.
	defer func() {
		if release != nil {
			release(&err)
		}
	}()

	for attempt := 0; attempt < cfg.maxRetries; attempt++ {
		if attempt > 0 {
			// Jittered exponential backoff on CAS retry. Without this,
			// concurrent writers that all get 412 retry in lockstep —
			// thundering herd where one wins and N-1 lose every round.
			// Cap the base delay so we don't blow past ctx deadlines.
			base := time.Duration(1<<min(attempt-1, 6)) * 10 * time.Millisecond // 10,20,40,...640ms cap
			jitter := time.Duration(rand.Int63n(int64(base)))
			select {
			case <-time.After(base/2 + jitter):
			case <-ctx.Done():
				err = ctx.Err()
				return err
			}
		}

		// Phase 1: capture if needed.
		if needCapture {
			release = sqlitex.Save(st.conn)

			cs, err = capture(st.conn, fn)
			if err != nil {
				return err // fn failed — propagate, SAVEPOINT rolls back
			}

			if cs == nil {
				// fn made no recordable changes. Nothing to commit.
				return nil // SAVEPOINT releases cleanly (err is nil)
			}

			// Best-effort cleanup of the previous orphan. The old blob
			// was uploaded but never committed (rebase conflict forced
			// recapture). Deleting it now prevents orphan accumulation
			// under contention; if the delete fails, GC will catch it.
			if orphan != "" {
				_ = cfg.store.Delete(ctx, orphan)
				orphan = ""
			}

			epoch := st.manifest.epoch()
			csKey = fmt.Sprintf("%schangesets/%s/cs-%s.bin", cfg.prefix, epoch, uuid.NewString())
			if _, uerr := cfg.store.Put(ctx, csKey, bytes.NewReader(cs), NoCondition); uerr != nil {
				err = fmt.Errorf("upload changeset: %w", uerr)
				return err
			}

			needCapture = false
		}

		// Phase 2: CAS the manifest.
		nextSeq := st.manifest.Seq + 1
		newManifest := st.manifest.appendLog(logEntry{Key: csKey, Seq: nextSeq, Size: int64(len(cs))})
		newEtag, perr := putManifest(ctx, cfg.store, cfg.manifestKey, newManifest, PutCondition{IfMatch: st.etag})

		if perr == nil {
			// Committed. Release the SAVEPOINT (fn's changes + any rebased
			// changesets stay on conn), update state.
			st.manifest = newManifest
			st.etag = newEtag
			st.localSeq = nextSeq
			release(&err) // err is nil → RELEASE
			release = nil
			return nil
		}

		if !errors.Is(perr, ErrPreconditionFailed) {
			err = fmt.Errorf("put manifest: %w", perr)
			return err
		}

		// Phase 3: 412 — someone else committed. Refresh and attempt rebase.
		m2, etag2, lerr := loadManifest(ctx, cfg.store, cfg.manifestKey)
		if lerr != nil {
			err = lerr
			return err
		}

		// If the snapshot advanced past our localSeq (compaction happened),
		// we can't rebase in place — we need a full refresh, which means
		// closing conn, which means losing the SAVEPOINT. Roll back and
		// start over.
		if m2.Snapshot.Seq > st.localSeq || m2.Snapshot.Key != st.snapshotKey {
			if rerr := rollbackAndResync(ctx, cfg, st, localPath, m2, etag2, &release); rerr != nil {
				err = rerr
				return err
			}
			orphan = csKey // will be deleted on next capture
			needCapture = true
			continue
		}

		// Try to apply other writers' changesets on top of conn. conn
		// currently has: base-up-to-localSeq + fn's-changes (in SAVEPOINT).
		// Applying their changesets on top will conflict if and only if
		// they touched rows fn touched — which is exactly when our
		// uploaded changeset would conflict during replay. So a clean
		// rebase here proves our changeset is safe to append after theirs.
		rebased := true
		for _, e := range m2.Log {
			if e.Seq <= st.localSeq {
				continue
			}
			if aerr := applyMissingEntry(ctx, cfg.store, st.conn, e); aerr != nil {
				var ce *errChangesetConflict
				if errors.As(aerr, &ce) {
					rebased = false
					break
				}
				err = aerr
				return err
			}
		}

		if rebased {
			// Clean rebase. conn now has base + fn's changes + their
			// changes. Our already-uploaded changeset is still valid
			// (it describes fn's changes against the original before-state,
			// which is unchanged). Retry CAS on the new manifest.
			st.manifest = m2
			st.etag = etag2
			st.localSeq = m2.Seq
			// needCapture stays false — keep the open SAVEPOINT and csKey.
			continue
		}

		// Rebase conflicted. Roll back fn's changes (and the partial rebase),
		// sync to the new manifest, and re-run fn from scratch next iteration.
		if rerr := rollbackAndResync(ctx, cfg, st, localPath, m2, etag2, &release); rerr != nil {
			err = rerr
			return err
		}
		orphan = csKey // will be deleted on next capture
		needCapture = true
	}

	// Exhausted retries. Ensure any open SAVEPOINT is rolled back by
	// setting err before the deferred release fires.
	err = ErrConflict
	return err
}

// rollbackAndResync rolls back the current SAVEPOINT (discarding fn's local
// changes and any partial rebase), updates st to the given manifest, and
// syncs conn to match. *release is nilled so the deferred release in
// doUpdate doesn't double-fire.
//
// Returns an error if the post-rollback sync fails. The caller MUST abort
// the commit in that case — st.manifest/etag have been updated but conn
// has NOT been synced, so continuing would risk committing a changeset
// whose before-image doesn't match the state at its predecessor seq.
func rollbackAndResync(ctx context.Context, cfg *commitConfig, st *commitState, localPath string, m *manifest, etag string, release *func(*error)) error {
	rollbackErr := errors.New("rollback")
	(*release)(&rollbackErr)
	*release = nil

	st.manifest = m
	st.etag = etag
	return syncToManifest(ctx, cfg, st, localPath)
}

// applyMissingEntry fetches and applies one log entry. Separated for error
// context and to buffer the changeset before applying (changesets are small;
// buffering keeps the store connection open for the minimum time).
func applyMissingEntry(ctx context.Context, store BlobStore, conn *sqlite.Conn, e logEntry) error {
	rc, _, err := store.Get(ctx, e.Key)
	if err != nil {
		return fmt.Errorf("fetch changeset seq=%d: %w", e.Seq, err)
	}
	defer rc.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(rc); err != nil {
		return fmt.Errorf("read changeset seq=%d: %w", e.Seq, err)
	}

	return applyChangeset(conn, buf.Bytes(), conflictAbort)
}
