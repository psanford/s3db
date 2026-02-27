package s3db

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// --- GetRange --------------------------------------------------------------

func TestMemBlobStore_GetRange(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	putString(t, s, "k", "0123456789", NoCondition)

	cases := []struct {
		start, end int64
		want       string
	}{
		{0, 9, "0123456789"},   // full
		{0, 4, "01234"},        // prefix
		{5, 9, "56789"},        // suffix
		{3, 6, "3456"},         // middle
		{0, 0, "0"},            // single byte
		{9, 9, "9"},            // last byte
		{5, 100, "56789"},      // end past EOF — clamped
		{100, 200, ""},         // start past EOF — empty
	}
	for _, tc := range cases {
		rc, err := s.GetRange(ctx, "k", tc.start, tc.end)
		if err != nil {
			t.Fatalf("GetRange(%d,%d): %v", tc.start, tc.end, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != tc.want {
			t.Errorf("GetRange(%d,%d) = %q, want %q", tc.start, tc.end, got, tc.want)
		}
	}
}

func TestMemBlobStore_GetRange_NotFound(t *testing.T) {
	s := NewMemBlobStore()
	_, err := s.GetRange(context.Background(), "missing", 0, 10)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Parallel snapshot download --------------------------------------------

// countingStore wraps MemBlobStore and counts Get vs GetRange calls.
type countingStore struct {
	*MemBlobStore
	gets      atomic.Int32
	getRanges atomic.Int32
}

func (c *countingStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	c.gets.Add(1)
	return c.MemBlobStore.Get(ctx, key)
}

func (c *countingStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	c.getRanges.Add(1)
	return c.MemBlobStore.GetRange(ctx, key, start, end)
}

func TestDownloadSnapshot_SmallUsesSingleGet(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{MemBlobStore: NewMemBlobStore()}

	// Small content — well under threshold.
	content := bytes.Repeat([]byte("x"), 1024)
	cs.Put(ctx, "snap", bytes.NewReader(content), NoCondition)

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")
	if err := downloadSnapshot(ctx, cs, "snap", int64(len(content)), dstPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}

	if cs.gets.Load() != 1 {
		t.Errorf("Gets = %d, want 1", cs.gets.Load())
	}
	if cs.getRanges.Load() != 0 {
		t.Errorf("GetRanges = %d, want 0", cs.getRanges.Load())
	}

	got, _ := os.ReadFile(dstPath)
	if !bytes.Equal(got, content) {
		t.Error("downloaded content mismatch")
	}
}

func TestDownloadSnapshot_LargeUsesParallel(t *testing.T) {
	ctx := context.Background()
	cs := &countingStore{MemBlobStore: NewMemBlobStore()}

	// Content larger than threshold. Use random bytes so a misaligned
	// range request would produce a detectable mismatch.
	content := make([]byte, parallelDownloadThreshold+1024)
	rand.Read(content)
	cs.Put(ctx, "snap", bytes.NewReader(content), NoCondition)

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")
	if err := downloadSnapshot(ctx, cs, "snap", int64(len(content)), dstPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}

	if cs.gets.Load() != 0 {
		t.Errorf("Gets = %d, want 0 (should use ranges)", cs.gets.Load())
	}
	if cs.getRanges.Load() != parallelDownloadParts {
		t.Errorf("GetRanges = %d, want %d", cs.getRanges.Load(), parallelDownloadParts)
	}

	got, _ := os.ReadFile(dstPath)
	if !bytes.Equal(got, content) {
		t.Errorf("downloaded content mismatch: len(got)=%d len(want)=%d", len(got), len(content))
	}
}

func TestDownloadSnapshot_UnknownSizeUsesSingleGet(t *testing.T) {
	// size=0 means unknown — should fall back to single GET regardless
	// of actual size. This is the backward-compat path for old manifests.
	ctx := context.Background()
	cs := &countingStore{MemBlobStore: NewMemBlobStore()}

	content := make([]byte, parallelDownloadThreshold+1024)
	rand.Read(content)
	cs.Put(ctx, "snap", bytes.NewReader(content), NoCondition)

	dstPath := filepath.Join(t.TempDir(), "out.sqlite")
	if err := downloadSnapshot(ctx, cs, "snap", 0, dstPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}

	if cs.gets.Load() != 1 {
		t.Errorf("Gets = %d, want 1", cs.gets.Load())
	}
	if cs.getRanges.Load() != 0 {
		t.Errorf("GetRanges = %d, want 0", cs.getRanges.Load())
	}

	got, _ := os.ReadFile(dstPath)
	if !bytes.Equal(got, content) {
		t.Error("downloaded content mismatch")
	}
}

func TestDownloadParallel_ExactBoundaries(t *testing.T) {
	// Exact multiple of chunk size — no remainder handling.
	ctx := context.Background()
	store := NewMemBlobStore()

	const parts = 4
	content := make([]byte, 4000) // divides evenly by 4
	rand.Read(content)
	store.Put(ctx, "k", bytes.NewReader(content), NoCondition)

	tmp, _ := os.CreateTemp(t.TempDir(), "out-*")
	defer tmp.Close()

	if err := downloadParallel(ctx, store, "k", int64(len(content)), tmp, parts); err != nil {
		t.Fatalf("downloadParallel: %v", err)
	}

	tmp.Seek(0, 0)
	got, _ := io.ReadAll(tmp)
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: len(got)=%d len(want)=%d", len(got), len(content))
	}
}

func TestDownloadParallel_Remainder(t *testing.T) {
	// Size not evenly divisible — last part gets the remainder.
	ctx := context.Background()
	store := NewMemBlobStore()

	content := make([]byte, 4003) // 4 parts × 1000 + 3 extra
	rand.Read(content)
	store.Put(ctx, "k", bytes.NewReader(content), NoCondition)

	tmp, _ := os.CreateTemp(t.TempDir(), "out-*")
	defer tmp.Close()

	if err := downloadParallel(ctx, store, "k", int64(len(content)), tmp, 4); err != nil {
		t.Fatalf("downloadParallel: %v", err)
	}

	tmp.Seek(0, 0)
	got, _ := io.ReadAll(tmp)
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: len(got)=%d len(want)=%d", len(got), len(content))
	}
}

func TestDownloadParallel_FailurePropagates(t *testing.T) {
	// If one range fails, the whole download fails.
	ctx := context.Background()
	fs := newFaultyStore(NewMemBlobStore(), 0, 1)
	content := make([]byte, 4000)
	fs.inner.Put(ctx, "k", bytes.NewReader(content), NoCondition)

	// Flip failure on for the actual download.
	fs.prob = 1.0 // every GetRange fails

	tmp, _ := os.CreateTemp(t.TempDir(), "out-*")
	defer tmp.Close()

	err := downloadParallel(ctx, fs, "k", int64(len(content)), tmp, 4)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// --- Parallel changeset fetch ----------------------------------------------

func TestApplyLog_FetchesInParallel(t *testing.T) {
	// Verify that changeset fetches are concurrent by using a store that
	// blocks each Get until a quorum has started. If fetches were
	// sequential, this would deadlock (with a timeout) on the second Get.
	ctx := context.Background()
	dir := t.TempDir()
	store, m := setupLogFixture(t, dir)

	// The fixture has 3 changesets. Probe waits for all 3 to start.
	ps := newParallelismProbe(store, 3)

	localPath := filepath.Join(dir, "local.sqlite")
	downloadSnapshot(ctx, store, m.Snapshot.Key, 0, localPath)
	conn, _ := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	defer conn.Close()

	seq, err := applyLog(ctx, ps, conn, m.Log, 0)
	if err != nil {
		t.Fatalf("applyLog: %v", err)
	}
	if seq != 3 {
		t.Errorf("seq = %d, want 3", seq)
	}

	if ps.maxConcurrent.Load() < 3 {
		t.Errorf("max concurrent Gets = %d, want >= 3 (parallel fetch)", ps.maxConcurrent.Load())
	}
}

// parallelismProbe tracks the maximum concurrency level of Get calls, and
// blocks each Get until `need` Gets have started — forcing observable
// parallelism. If fetches were actually sequential, the second Get would
// deadlock waiting for a quorum that never arrives (the test would then
// time out rather than pass spuriously).
type parallelismProbe struct {
	*MemBlobStore
	need          int32
	current       atomic.Int32
	maxConcurrent atomic.Int32
	barrier       chan struct{}
	barrierOnce   sync.Once
}

func newParallelismProbe(inner *MemBlobStore, need int) *parallelismProbe {
	return &parallelismProbe{
		MemBlobStore: inner,
		need:         int32(need),
		barrier:      make(chan struct{}),
	}
}

func (p *parallelismProbe) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	n := p.current.Add(1)
	defer p.current.Add(-1)

	// Track high-water mark.
	for {
		max := p.maxConcurrent.Load()
		if n <= max || p.maxConcurrent.CompareAndSwap(max, n) {
			break
		}
	}

	// If we've hit the quorum, release everyone. Otherwise wait.
	if n >= p.need {
		p.barrierOnce.Do(func() { close(p.barrier) })
	}
	select {
	case <-p.barrier:
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}

	return p.MemBlobStore.Get(ctx, key)
}

// --- Size recording --------------------------------------------------------

func TestSize_RecordedOnWrite(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
	})

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if len(m.Log) != 1 {
		t.Fatalf("log len = %d, want 1", len(m.Log))
	}
	if m.Log[0].Size == 0 {
		t.Error("changeset size not recorded")
	}

	// Verify the size matches the actual blob.
	rc, _, _ := store.Get(ctx, m.Log[0].Key)
	body, _ := io.ReadAll(rc)
	rc.Close()
	if m.Log[0].Size != int64(len(body)) {
		t.Errorf("recorded size = %d, actual = %d", m.Log[0].Size, len(body))
	}
}

func TestSize_RecordedOnCompact(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.Snapshot.Size == 0 {
		t.Error("snapshot size not recorded after compact")
	}

	rc, _, _ := store.Get(ctx, m.Snapshot.Key)
	body, _ := io.ReadAll(rc)
	rc.Close()
	if m.Snapshot.Size != int64(len(body)) {
		t.Errorf("recorded size = %d, actual = %d", m.Snapshot.Size, len(body))
	}
}

func TestSize_RecordedOnBootstrap(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	db, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.Snapshot.Size == 0 {
		t.Error("snapshot size not recorded on bootstrap")
	}
}

func TestSize_RecordedOnMigrate(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	migs := []Migration{
		{Version: 1, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE t (id INTEGER PRIMARY KEY);`, nil)
		}},
	}

	db, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.Snapshot.Size == 0 {
		t.Error("snapshot size not recorded after migration")
	}
}
