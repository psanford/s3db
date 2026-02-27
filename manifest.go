package s3db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
)

// manifest is the single source of truth for the database state. It points
// to an immutable snapshot and an ordered log of changesets to apply on top.
// The manifest is the only object written with contention; everything it
// references is write-once.
//
// Invariants (enforced by Validate):
//   - Snapshot.Key is non-empty
//   - Log entries have strictly increasing Seq, starting at Snapshot.Seq+1
//   - Seq equals Snapshot.Seq if Log is empty, else the last Log entry's Seq
type manifest struct {
	// Seq is the logical version of the database. It increases by exactly 1
	// with each committed write. It never has gaps and never goes backward.
	Seq int64 `json:"seq"`

	// SchemaVersion tracks applied migrations. Writers must match this
	// exactly; a mismatch returns ErrSchemaMismatch.
	SchemaVersion int `json:"schema_version"`

	// Snapshot is the base full-database file. Its Seq is the highest
	// sequence number whose changes are materialized in the snapshot.
	Snapshot blobRef `json:"snapshot"`

	// Log is the ordered list of changesets to apply on top of Snapshot
	// to reach the current state at Seq.
	Log []logEntry `json:"log"`
}

// blobRef points to an immutable blob in the store.
type blobRef struct {
	Key  string `json:"key"`
	Seq  int64  `json:"seq"`
	Size int64  `json:"size,omitempty"` // bytes; 0 = unknown (old manifests, or size not recorded)
}

// logEntry is one changeset in the log.
type logEntry struct {
	Key  string `json:"key"`
	Seq  int64  `json:"seq"`
	Size int64  `json:"size,omitempty"` // bytes; 0 = unknown
}

// epoch returns the identifier used to group changesets by their origin
// snapshot. It is the basename of the snapshot key with the extension
// stripped, e.g. "snapshots/snap-abc.sqlite" → "snap-abc".
//
// Changesets for the current epoch are written under "changesets/<epoch>/".
// When a snapshot is superseded and its epoch contains no live log entries,
// the entire prefix can be deleted — see GC in DESIGN.md.
func (m *manifest) epoch() string {
	base := path.Base(m.Snapshot.Key)
	if i := strings.LastIndex(base, "."); i > 0 {
		return base[:i]
	}
	return base
}

// validate checks the manifest's internal consistency. It returns an error
// describing the first violated invariant, or nil if the manifest is valid.
func (m *manifest) validate() error {
	if m.Snapshot.Key == "" {
		return fmt.Errorf("manifest: snapshot key is empty")
	}
	if m.Snapshot.Seq < 0 {
		return fmt.Errorf("manifest: snapshot seq %d is negative", m.Snapshot.Seq)
	}
	if m.SchemaVersion < 0 {
		return fmt.Errorf("manifest: schema_version %d is negative", m.SchemaVersion)
	}

	want := m.Snapshot.Seq + 1
	for i, e := range m.Log {
		if e.Key == "" {
			return fmt.Errorf("manifest: log[%d] has empty key", i)
		}
		if e.Seq != want {
			return fmt.Errorf("manifest: log[%d] has seq %d, want %d (gap or out of order)", i, e.Seq, want)
		}
		want++
	}

	expectSeq := m.Snapshot.Seq
	if n := len(m.Log); n > 0 {
		expectSeq = m.Log[n-1].Seq
	}
	if m.Seq != expectSeq {
		return fmt.Errorf("manifest: seq %d does not match expected %d (snapshot.seq=%d, len(log)=%d)",
			m.Seq, expectSeq, m.Snapshot.Seq, len(m.Log))
	}

	return nil
}

// appendLog returns a new manifest with entry appended to the log and Seq
// advanced to entry.Seq. It does not validate; call Validate on the result
// before committing. The receiver is not modified.
func (m *manifest) appendLog(entry logEntry) *manifest {
	out := *m
	out.Log = make([]logEntry, len(m.Log)+1)
	copy(out.Log, m.Log)
	out.Log[len(m.Log)] = entry
	out.Seq = entry.Seq
	return &out
}

// withSnapshot returns a new manifest with the given snapshot and an empty
// log. Seq is set to snapshot.Seq. Used by compaction and migrations.
// The receiver is not modified.
func (m *manifest) withSnapshot(snapshot blobRef) *manifest {
	return &manifest{
		Seq:           snapshot.Seq,
		SchemaVersion: m.SchemaVersion,
		Snapshot:      snapshot,
		Log:           nil,
	}
}

// loadManifest fetches and parses the manifest from the store. It validates
// the result before returning.
func loadManifest(ctx context.Context, store BlobStore, key string) (*manifest, string, error) {
	rc, etag, err := store.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	var m manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, "", fmt.Errorf("manifest: decode: %w", err)
	}
	if err := m.validate(); err != nil {
		return nil, "", err
	}
	return &m, etag, nil
}

// putManifest validates and writes the manifest to the store. Returns the
// new ETag on success. cond is typically either IfMatch (CAS during commit)
// or IfNoneMatch (bootstrap).
func putManifest(ctx context.Context, store BlobStore, key string, m *manifest, cond PutCondition) (string, error) {
	if err := m.validate(); err != nil {
		return "", err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("manifest: marshal: %w", err)
	}
	return store.Put(ctx, key, bytes.NewReader(body), cond)
}
