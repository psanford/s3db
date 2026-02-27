package s3db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// validManifest returns a manifest that passes Validate, for use as a
// starting point in tests that want to break one specific invariant.
func validManifest() *manifest {
	return &manifest{
		Seq:           12,
		SchemaVersion: 3,
		Snapshot:      blobRef{Key: "snapshots/snap-abc.sqlite", Seq: 10},
		Log: []logEntry{
			{Key: "changesets/snap-abc/cs-1.bin", Seq: 11},
			{Key: "changesets/snap-abc/cs-2.bin", Seq: 12},
		},
	}
}

func TestManifest_Validate_OK(t *testing.T) {
	m := validManifest()
	if err := m.validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManifest_Validate_EmptyLog(t *testing.T) {
	m := &manifest{
		Seq:      5,
		Snapshot: blobRef{Key: "snapshots/snap-x.sqlite", Seq: 5},
		Log:      nil,
	}
	if err := m.validate(); err != nil {
		t.Fatalf("empty log with matching seq should be valid: %v", err)
	}
}

func TestManifest_Validate_FreshDB(t *testing.T) {
	m := &manifest{
		Seq:      0,
		Snapshot: blobRef{Key: "snapshots/snap-init.sqlite", Seq: 0},
		Log:      nil,
	}
	if err := m.validate(); err != nil {
		t.Fatalf("fresh manifest should be valid: %v", err)
	}
}

func TestManifest_Validate_EmptySnapshotKey(t *testing.T) {
	m := validManifest()
	m.Snapshot.Key = ""
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "snapshot key is empty") {
		t.Errorf("expected snapshot-key error, got %v", err)
	}
}

func TestManifest_Validate_NegativeSnapshotSeq(t *testing.T) {
	m := &manifest{
		Seq:      -1,
		Snapshot: blobRef{Key: "snapshots/snap-x.sqlite", Seq: -1},
	}
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Errorf("expected negative-seq error, got %v", err)
	}
}

func TestManifest_Validate_NegativeSchemaVersion(t *testing.T) {
	m := validManifest()
	m.SchemaVersion = -1
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Errorf("expected schema-version error, got %v", err)
	}
}

func TestManifest_Validate_LogGap(t *testing.T) {
	m := validManifest()
	m.Log[1].Seq = 13 // skip 12
	m.Seq = 13
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "gap") {
		t.Errorf("expected gap error, got %v", err)
	}
}

func TestManifest_Validate_LogOutOfOrder(t *testing.T) {
	m := validManifest()
	m.Log[0], m.Log[1] = m.Log[1], m.Log[0]
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "gap or out of order") {
		t.Errorf("expected out-of-order error, got %v", err)
	}
}

func TestManifest_Validate_LogDoesNotStartAfterSnapshot(t *testing.T) {
	m := validManifest()
	m.Log[0].Seq = 10 // same as snapshot.seq, should be snapshot.seq+1
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "want 11") {
		t.Errorf("expected log-start error, got %v", err)
	}
}

func TestManifest_Validate_EmptyLogKey(t *testing.T) {
	m := validManifest()
	m.Log[0].Key = ""
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "empty key") {
		t.Errorf("expected empty-key error, got %v", err)
	}
}

func TestManifest_Validate_SeqMismatch(t *testing.T) {
	m := validManifest()
	m.Seq = 99
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("expected seq-mismatch error, got %v", err)
	}
}

func TestManifest_Validate_SeqMismatchEmptyLog(t *testing.T) {
	m := &manifest{
		Seq:      7,
		Snapshot: blobRef{Key: "snapshots/snap-x.sqlite", Seq: 5},
		Log:      nil,
	}
	if err := m.validate(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("expected seq-mismatch error for empty log, got %v", err)
	}
}

func TestManifest_Epoch(t *testing.T) {
	cases := []struct {
		key, want string
	}{
		{"snapshots/snap-abc.sqlite", "snap-abc"},
		{"snapshots/snap-abc123.db", "snap-abc123"},
		{"snap-noext", "snap-noext"},
		{"a/b/c/snap-deep.sqlite", "snap-deep"},
		{"snap.with.dots.sqlite", "snap.with.dots"},
	}
	for _, tc := range cases {
		m := &manifest{Snapshot: blobRef{Key: tc.key}}
		if got := m.epoch(); got != tc.want {
			t.Errorf("Epoch(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestManifest_JSONRoundtrip(t *testing.T) {
	m := validManifest()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got manifest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Seq != m.Seq {
		t.Errorf("Seq = %d, want %d", got.Seq, m.Seq)
	}
	if got.SchemaVersion != m.SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", got.SchemaVersion, m.SchemaVersion)
	}
	if got.Snapshot != m.Snapshot {
		t.Errorf("Snapshot = %+v, want %+v", got.Snapshot, m.Snapshot)
	}
	if len(got.Log) != len(m.Log) {
		t.Fatalf("len(Log) = %d, want %d", len(got.Log), len(m.Log))
	}
	for i := range m.Log {
		if got.Log[i] != m.Log[i] {
			t.Errorf("Log[%d] = %+v, want %+v", i, got.Log[i], m.Log[i])
		}
	}
}

func TestManifest_AppendLog(t *testing.T) {
	m := validManifest() // seq 12, log has 11, 12

	next := m.appendLog(logEntry{Key: "changesets/snap-abc/cs-3.bin", Seq: 13})

	if next.Seq != 13 {
		t.Errorf("Seq = %d, want 13", next.Seq)
	}
	if len(next.Log) != 3 {
		t.Fatalf("len(Log) = %d, want 3", len(next.Log))
	}
	if next.Log[2].Seq != 13 {
		t.Errorf("Log[2].Seq = %d, want 13", next.Log[2].Seq)
	}
	if err := next.validate(); err != nil {
		t.Errorf("result not valid: %v", err)
	}

	// Original must be unchanged.
	if m.Seq != 12 || len(m.Log) != 2 {
		t.Errorf("original modified: seq=%d len=%d", m.Seq, len(m.Log))
	}
}

func TestManifest_AppendLog_DoesNotAlias(t *testing.T) {
	m := validManifest()
	next := m.appendLog(logEntry{Key: "x", Seq: 13})

	// Mutating next.Log should not affect m.Log.
	next.Log[0].Key = "MUTATED"
	if m.Log[0].Key == "MUTATED" {
		t.Error("AppendLog aliased the original log slice")
	}
}

func TestManifest_WithSnapshot(t *testing.T) {
	m := validManifest() // seq 12, schema 3, log has 2 entries

	newSnap := blobRef{Key: "snapshots/snap-xyz.sqlite", Seq: 12}
	next := m.withSnapshot(newSnap)

	if next.Seq != 12 {
		t.Errorf("Seq = %d, want 12", next.Seq)
	}
	if next.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3 (preserved)", next.SchemaVersion)
	}
	if next.Snapshot != newSnap {
		t.Errorf("Snapshot = %+v, want %+v", next.Snapshot, newSnap)
	}
	if len(next.Log) != 0 {
		t.Errorf("len(Log) = %d, want 0", len(next.Log))
	}
	if err := next.validate(); err != nil {
		t.Errorf("result not valid: %v", err)
	}

	// Original unchanged.
	if m.Snapshot.Key != "snapshots/snap-abc.sqlite" || len(m.Log) != 2 {
		t.Error("original modified")
	}
}

func TestLoadPutManifest_Roundtrip(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	key := "mydb/manifest.json"

	m := validManifest()
	etag1, err := putManifest(ctx, s, key, m, NoCondition)
	if err != nil {
		t.Fatalf("putManifest: %v", err)
	}

	got, etag2, err := loadManifest(ctx, s, key)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if etag2 != etag1 {
		t.Errorf("load etag = %q, want %q", etag2, etag1)
	}
	if got.Seq != m.Seq {
		t.Errorf("Seq = %d, want %d", got.Seq, m.Seq)
	}
}

func TestLoadManifest_NotFound(t *testing.T) {
	s := NewMemBlobStore()
	_, _, err := loadManifest(context.Background(), s, "missing/manifest.json")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestLoadManifest_InvalidJSON(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	s.Put(ctx, "m", strings.NewReader("not json"), NoCondition)

	_, _, err := loadManifest(ctx, s, "m")
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Errorf("expected decode error, got %v", err)
	}
}

func TestLoadManifest_InvalidManifest(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	bad := &manifest{Seq: 99, Snapshot: blobRef{Key: "x", Seq: 5}} // seq mismatch
	data, _ := json.Marshal(bad)
	s.Put(ctx, "m", bytes.NewReader(data), NoCondition)

	_, _, err := loadManifest(ctx, s, "m")
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("expected validation error on load, got %v", err)
	}
}

func TestPutManifest_InvalidRejected(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	bad := &manifest{Seq: 99, Snapshot: blobRef{Key: "x", Seq: 5}}
	_, err := putManifest(ctx, s, "m", bad, NoCondition)
	if err == nil {
		t.Error("expected validation error, got nil")
	}

	// Nothing should have been written.
	if _, _, err := s.Get(ctx, "m"); !errors.Is(err, ErrNotFound) {
		t.Error("invalid manifest was written to store")
	}
}

func TestPutManifest_CAS(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	key := "m"

	m1 := validManifest()
	etag1, _ := putManifest(ctx, s, key, m1, NoCondition)

	m2 := m1.appendLog(logEntry{Key: "changesets/snap-abc/cs-3.bin", Seq: 13})

	// CAS with correct etag succeeds.
	etag2, err := putManifest(ctx, s, key, m2, PutCondition{IfMatch: etag1})
	if err != nil {
		t.Fatalf("CAS with correct etag: %v", err)
	}

	// CAS with stale etag fails.
	m3 := m2.appendLog(logEntry{Key: "changesets/snap-abc/cs-4.bin", Seq: 14})
	_, err = putManifest(ctx, s, key, m3, PutCondition{IfMatch: etag1})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed with stale etag, got %v", err)
	}

	// CAS with current etag succeeds.
	_, err = putManifest(ctx, s, key, m3, PutCondition{IfMatch: etag2})
	if err != nil {
		t.Errorf("CAS with current etag: %v", err)
	}
}

func TestPutManifest_IfNoneMatch(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	key := "m"

	m := validManifest()

	// First write with IfNoneMatch succeeds.
	_, err := putManifest(ctx, s, key, m, PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("first IfNoneMatch write: %v", err)
	}

	// Second fails.
	_, err = putManifest(ctx, s, key, m, PutCondition{IfNoneMatch: true})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("expected ErrPreconditionFailed on second IfNoneMatch, got %v", err)
	}
}
