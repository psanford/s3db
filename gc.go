package s3db

import (
	"context"
	"fmt"
	"strings"
)

// GC deletes unreachable blobs: epoch prefixes whose changesets have all
// been subsumed by the current snapshot, and snapshots that are no longer
// referenced by the current manifest.
//
// An epoch is deletable when none of its changesets appear in the current
// manifest's log. In practice this happens one or two compaction cycles
// after the epoch closes — two if a straggler write landed in the epoch
// during compaction. See DESIGN.md.
//
// Orphaned blobs (uploaded but never committed due to crash or lost CAS)
// are cleaned up automatically as part of epoch prefix deletion — they
// live under the same prefix as committed changesets and get swept
// together.
//
// GC is safe to run concurrently with writers. It only deletes prefixes
// that are not referenced by the current manifest, and the manifest is
// the source of truth — a writer cannot resurrect a deleted epoch
// (compaction opens a new epoch; writers never go back).
func (db *DB) GC(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Refresh to ensure we're checking against the current manifest.
	if err := refreshManifest(ctx, &db.cfg, &db.st, db.localPath); err != nil {
		return err
	}
	m := db.st.manifest

	// Collect the set of changeset keys that are live (in the current log).
	liveChangesets := make(map[string]struct{}, len(m.Log))
	for _, e := range m.Log {
		liveChangesets[e.Key] = struct{}{}
	}
	currentEpoch := m.Epoch()

	// Sweep changeset epochs. List all keys under changesets/, group by
	// epoch prefix, delete any epoch that (a) is not the current epoch
	// and (b) has no keys in the live set.
	csPrefix := db.cfg.prefix + "changesets/"
	keys, err := db.cfg.store.List(ctx, csPrefix)
	if err != nil {
		return fmt.Errorf("gc: list changesets: %w", err)
	}

	epochs := groupByEpoch(keys, csPrefix)
	for epoch, epochKeys := range epochs {
		if epoch == currentEpoch {
			// Never delete the current epoch — it may receive writes
			// after our manifest snapshot.
			continue
		}
		// Check if any key in this epoch is live.
		hasLive := false
		for _, k := range epochKeys {
			if _, ok := liveChangesets[k]; ok {
				hasLive = true
				break
			}
		}
		if hasLive {
			continue
		}
		// Entire epoch is garbage.
		epochPrefix := csPrefix + epoch + "/"
		if err := db.cfg.store.DeletePrefix(ctx, epochPrefix); err != nil {
			return fmt.Errorf("gc: delete epoch %s: %w", epoch, err)
		}
	}

	// Sweep old snapshots. Any snapshot that is not the current one is
	// garbage. (A more conservative policy would add a grace period to
	// protect in-flight readers holding an old manifest, but since Open
	// downloads the snapshot before using it, and the snapshot download
	// is atomic from S3's perspective, the window is very small. If this
	// becomes a concern, add a time-based check on object LastModified.)
	snapPrefix := db.cfg.prefix + "snapshots/"
	snapKeys, err := db.cfg.store.List(ctx, snapPrefix)
	if err != nil {
		return fmt.Errorf("gc: list snapshots: %w", err)
	}
	for _, k := range snapKeys {
		if k == m.Snapshot.Key {
			continue
		}
		if err := db.cfg.store.Delete(ctx, k); err != nil {
			return fmt.Errorf("gc: delete snapshot %s: %w", k, err)
		}
	}

	return nil
}

// groupByEpoch groups changeset keys by their epoch (the path segment after
// the changesets/ prefix). Keys that don't follow the expected structure
// are silently skipped.
func groupByEpoch(keys []string, csPrefix string) map[string][]string {
	out := make(map[string][]string)
	for _, k := range keys {
		rest := strings.TrimPrefix(k, csPrefix)
		// rest is "epoch/cs-xyz.bin"
		i := strings.Index(rest, "/")
		if i <= 0 {
			continue
		}
		epoch := rest[:i]
		out[epoch] = append(out[epoch], k)
	}
	return out
}
