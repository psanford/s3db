package s3db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// --- Test harness ----------------------------------------------------------

// testEnv is a complete working environment for commit tests: a store with
// an initial snapshot + manifest, and a synced local DB.
type testEnv struct {
	store     *MemBlobStore
	cfg       *commitConfig
	st        *commitState
	localPath string
}

// newTestEnv builds an environment with the test schema applied, seq=0,
// empty log. The local DB is synced and ready for doUpdate.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	store := NewMemBlobStore()

	// Build initial snapshot: just the schema, no data.
	snapPath := filepath.Join(dir, "snap-0.sqlite")
	c := openTestDB(t, snapPath)
	c.Close()
	snapKey := "mydb/snapshots/snap-0.sqlite"
	store.Put(ctx, snapKey, bytes.NewReader(snapshotBytes(t, snapPath)), NoCondition)

	// Write initial manifest.
	m := &Manifest{
		Seq:           0,
		SchemaVersion: 0,
		Snapshot:      BlobRef{Key: snapKey, Seq: 0},
		Log:           nil,
	}
	etag, err := putManifest(ctx, store, "mydb/manifest.json", m, NoCondition)
	if err != nil {
		t.Fatalf("putManifest: %v", err)
	}

	// Set up local DB.
	localPath := filepath.Join(dir, "local.sqlite")
	if err := downloadSnapshot(ctx, store, snapKey, 0, localPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}
	conn, err := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	if err != nil {
		t.Fatalf("OpenConn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		store: store,
		cfg: &commitConfig{
			store:       store,
			prefix:      "mydb/",
			manifestKey: "mydb/manifest.json",
			maxRetries:  5,
			schemaVer:   0,
		},
		st: &commitState{
			conn:        conn,
			localSeq:    0,
			snapshotKey: snapKey,
			manifest:    m,
			etag:        etag,
		},
		localPath: localPath,
	}
}

// update runs doUpdate and fails the test on error.
func (e *testEnv) update(t *testing.T, fn func(*sqlite.Conn) error) {
	t.Helper()
	if err := doUpdate(context.Background(), e.cfg, e.st, e.localPath, fn); err != nil {
		t.Fatalf("doUpdate: %v", err)
	}
}

// loadManifest re-reads the manifest from the store (bypassing env state).
func (e *testEnv) loadManifest(t *testing.T) *Manifest {
	t.Helper()
	m, _, err := loadManifest(context.Background(), e.store, e.cfg.manifestKey)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	return m
}

// --- Happy path ------------------------------------------------------------

func TestDoUpdate_HappyPath(t *testing.T) {
	e := newTestEnv(t)
	ctx := context.Background()

	e.update(t, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})

	// State should be updated.
	if e.st.localSeq != 1 {
		t.Errorf("localSeq = %d, want 1", e.st.localSeq)
	}
	if e.st.manifest.Seq != 1 {
		t.Errorf("manifest.Seq = %d, want 1", e.st.manifest.Seq)
	}
	if len(e.st.manifest.Log) != 1 {
		t.Fatalf("len(Log) = %d, want 1", len(e.st.manifest.Log))
	}

	// Manifest in store should match.
	m := e.loadManifest(t)
	if m.Seq != 1 {
		t.Errorf("stored manifest Seq = %d, want 1", m.Seq)
	}

	// Changeset blob should exist.
	if _, _, err := e.store.Get(ctx, m.Log[0].Key); err != nil {
		t.Errorf("changeset blob not in store: %v", err)
	}

	// Local conn should have the row.
	if got := queryInt(t, e.st.conn, `SELECT COUNT(*) FROM users`); got != 1 {
		t.Errorf("local row count = %d, want 1", got)
	}
}

func TestDoUpdate_Sequential(t *testing.T) {
	e := newTestEnv(t)

	for i := 1; i <= 5; i++ {
		i := i
		e.update(t, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c,
				fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'user-%d')`, i, i), nil)
		})
	}

	m := e.loadManifest(t)
	if m.Seq != 5 {
		t.Errorf("final seq = %d, want 5", m.Seq)
	}
	if len(m.Log) != 5 {
		t.Errorf("len(Log) = %d, want 5", len(m.Log))
	}
	// Verify sequence numbers are gapless.
	for i, e := range m.Log {
		if e.Seq != int64(i+1) {
			t.Errorf("Log[%d].Seq = %d, want %d", i, e.Seq, i+1)
		}
	}

	if got := queryInt(t, e.st.conn, `SELECT COUNT(*) FROM users`); got != 5 {
		t.Errorf("row count = %d, want 5", got)
	}
}

// --- No-op / empty changeset ------------------------------------------------

func TestDoUpdate_NoOp(t *testing.T) {
	e := newTestEnv(t)

	e.update(t, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT * FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(*sqlite.Stmt) error { return nil },
		})
	})

	// Manifest should be unchanged.
	m := e.loadManifest(t)
	if m.Seq != 0 {
		t.Errorf("manifest Seq = %d, want 0 (unchanged)", m.Seq)
	}
	if len(m.Log) != 0 {
		t.Errorf("len(Log) = %d, want 0", len(m.Log))
	}

	// No changeset blobs should have been written.
	keys, _ := e.store.List(context.Background(), "mydb/changesets/")
	if len(keys) != 0 {
		t.Errorf("unexpected changeset blobs: %v", keys)
	}
}

// --- fn error --------------------------------------------------------------

func TestDoUpdate_FnError(t *testing.T) {
	e := newTestEnv(t)

	wantErr := errors.New("fn boom")
	err := doUpdate(context.Background(), e.cfg, e.st, e.localPath, func(c *sqlite.Conn) error {
		// Make a change then error — change should be rolled back.
		sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected fn error, got %v", err)
	}

	// Manifest unchanged.
	m := e.loadManifest(t)
	if m.Seq != 0 {
		t.Errorf("manifest Seq = %d, want 0", m.Seq)
	}

	// Local conn should be rolled back.
	if got := queryInt(t, e.st.conn, `SELECT COUNT(*) FROM users`); got != 0 {
		t.Errorf("local row count = %d, want 0 (rolled back)", got)
	}
}

// --- Schema version guard --------------------------------------------------

func TestRefreshManifest_SchemaTooNew(t *testing.T) {
	e := newTestEnv(t)

	// Bump the stored manifest's schema version past what the client knows.
	m := e.loadManifest(t)
	m.SchemaVersion = 5
	putManifest(context.Background(), e.store, e.cfg.manifestKey, m, NoCondition)

	err := refreshManifest(context.Background(), e.cfg, e.st, e.localPath)
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Errorf("expected ErrSchemaTooNew, got %v", err)
	}
}

func TestRefreshManifest_SchemaMismatchBehind(t *testing.T) {
	e := newTestEnv(t)
	e.cfg.schemaVer = 3 // client expects v3, DB is at v0

	err := refreshManifest(context.Background(), e.cfg, e.st, e.localPath)
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

// --- Contention: clean rebase (disjoint rows) ------------------------------

// interposingStore wraps MemBlobStore and runs a hook before each Put to
// the manifest key. This lets tests inject a concurrent writer's commit
// between "our fn ran" and "our CAS fires".
type interposingStore struct {
	*MemBlobStore
	manifestKey string
	beforePut   func() // called before each Put to manifestKey; may be nil
}

func (s *interposingStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if key == s.manifestKey && s.beforePut != nil {
		s.beforePut()
	}
	return s.MemBlobStore.Put(ctx, key, body, cond)
}

// newInterposingEnv is newTestEnv but with an interposing store wired in.
func newInterposingEnv(t *testing.T) (*testEnv, *interposingStore) {
	t.Helper()
	e := newTestEnv(t)
	is := &interposingStore{
		MemBlobStore: e.store,
		manifestKey:  e.cfg.manifestKey,
	}
	e.cfg.store = is
	return e, is
}

// concurrentWrite simulates another client committing a changeset. It builds
// a fresh DB at the current manifest state, runs fn, uploads the changeset,
// and CAS-swaps the manifest. This is the "external" write path, bypassing
// doUpdate entirely.
func concurrentWrite(t *testing.T, e *testEnv, fn func(*sqlite.Conn)) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	// Fetch current manifest directly from store (not from e.st, which may
	// be mid-transaction).
	m, etag, err := loadManifest(ctx, e.store, e.cfg.manifestKey)
	if err != nil {
		t.Fatalf("concurrent: loadManifest: %v", err)
	}

	// Build DB at current state.
	localPath := filepath.Join(dir, "concurrent.sqlite")
	if err := downloadSnapshot(ctx, e.store, m.Snapshot.Key, 0, localPath); err != nil {
		t.Fatalf("concurrent: downloadSnapshot: %v", err)
	}
	conn, err := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	if err != nil {
		t.Fatalf("concurrent: OpenConn: %v", err)
	}
	defer conn.Close()
	if _, err := applyLog(ctx, e.store, conn, m.Log, m.Snapshot.Seq); err != nil {
		t.Fatalf("concurrent: applyLog: %v", err)
	}

	// Capture and commit.
	cs, err := capture(conn, func(c *sqlite.Conn) error { fn(c); return nil })
	if err != nil {
		t.Fatalf("concurrent: capture: %v", err)
	}
	csKey := fmt.Sprintf("%schangesets/%s/cs-concurrent-%d.bin", e.cfg.prefix, m.Epoch(), m.Seq+1)
	if _, err := e.store.Put(ctx, csKey, bytes.NewReader(cs), NoCondition); err != nil {
		t.Fatalf("concurrent: put changeset: %v", err)
	}
	m2 := m.AppendLog(LogEntry{Key: csKey, Seq: m.Seq + 1})
	if _, err := putManifest(ctx, e.store, e.cfg.manifestKey, m2, PutCondition{IfMatch: etag}); err != nil {
		t.Fatalf("concurrent: put manifest: %v", err)
	}
}

func TestDoUpdate_CleanRebase(t *testing.T) {
	e, is := newInterposingEnv(t)

	var fnCalls atomic.Int32

	// Inject a concurrent write before our FIRST manifest CAS.
	// It inserts row 2; we insert row 1. Disjoint → clean rebase.
	once := sync.Once{}
	is.beforePut = func() {
		once.Do(func() {
			concurrentWrite(t, e, func(c *sqlite.Conn) {
				mustExec(t, c, `INSERT INTO users (id, name) VALUES (2, 'bob')`)
			})
		})
	}

	e.update(t, func(c *sqlite.Conn) error {
		fnCalls.Add(1)
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
	})

	// fn should have been called exactly once — rebase was clean.
	if n := fnCalls.Load(); n != 1 {
		t.Errorf("fn called %d times, want 1 (clean rebase)", n)
	}

	// Final state: both rows, our changeset at seq 2.
	m := e.loadManifest(t)
	if m.Seq != 2 {
		t.Errorf("final seq = %d, want 2", m.Seq)
	}
	if got := queryInt(t, e.st.conn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}
	if got := queryString(t, e.st.conn, `SELECT name FROM users WHERE id = 1`); got != "alice" {
		t.Errorf("user 1 = %q, want alice", got)
	}
	if got := queryString(t, e.st.conn, `SELECT name FROM users WHERE id = 2`); got != "bob" {
		t.Errorf("user 2 = %q, want bob", got)
	}

	// Verify replay from scratch also produces correct state.
	verifyReplay(t, e, map[int]string{1: "alice", 2: "bob"})
}

// --- Contention: conflicting rebase (same row) -----------------------------

func TestDoUpdate_ConflictRebase(t *testing.T) {
	e, is := newInterposingEnv(t)

	// Seed a row both writers will update.
	e.update(t, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})

	var fnCalls atomic.Int32

	// Inject a concurrent UPDATE to the SAME row before our first CAS.
	once := sync.Once{}
	is.beforePut = func() {
		once.Do(func() {
			concurrentWrite(t, e, func(c *sqlite.Conn) {
				mustExec(t, c, `UPDATE users SET balance = 200 WHERE id = 1`)
			})
		})
	}

	e.update(t, func(c *sqlite.Conn) error {
		fnCalls.Add(1)
		return sqlitex.Execute(c, `UPDATE users SET balance = 999 WHERE id = 1`, nil)
	})

	// fn should have been called TWICE — rebase failed, re-executed.
	if n := fnCalls.Load(); n != 2 {
		t.Errorf("fn called %d times, want 2 (conflict → re-execute)", n)
	}

	// Final balance should be 999 (our second attempt won).
	if got := queryInt(t, e.st.conn, `SELECT balance FROM users WHERE id = 1`); got != 999 {
		t.Errorf("balance = %d, want 999", got)
	}

	// Replay should also show 999.
	verifyReplay(t, e, map[int]string{1: "alice"})
}

// --- Retry exhaustion ------------------------------------------------------

func TestDoUpdate_RetryExhaustion(t *testing.T) {
	e, is := newInterposingEnv(t)
	e.cfg.maxRetries = 3

	// Seed a row.
	e.update(t, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 0)`, nil)
	})

	// Every time we try to CAS, a concurrent writer conflicts with us.
	var concurrentCount atomic.Int32
	is.beforePut = func() {
		n := concurrentCount.Add(1)
		concurrentWrite(t, e, func(c *sqlite.Conn) {
			mustExec(t, c, fmt.Sprintf(`UPDATE users SET balance = %d WHERE id = 1`, n*1000))
		})
	}

	err := doUpdate(context.Background(), e.cfg, e.st, e.localPath, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = 1 WHERE id = 1`, nil)
	})

	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Local conn should be rolled back to a consistent state (not our
	// uncommitted write).
	bal := queryInt(t, e.st.conn, `SELECT balance FROM users WHERE id = 1`)
	if bal == 1 {
		t.Error("local balance = 1 (our uncommitted value leaked)")
	}
}

// --- Changeset under correct epoch prefix ----------------------------------

func TestDoUpdate_ChangesetInEpochPrefix(t *testing.T) {
	e := newTestEnv(t)

	e.update(t, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})

	m := e.loadManifest(t)
	csKey := m.Log[0].Key

	// Epoch is derived from snapshot key "mydb/snapshots/snap-0.sqlite" → "snap-0"
	wantPrefix := "mydb/changesets/snap-0/"
	if !strings.HasPrefix(csKey, wantPrefix) {
		t.Errorf("changeset key %q not under expected epoch prefix %q", csKey, wantPrefix)
	}
}

// --- Replay verification helper --------------------------------------------

// verifyReplay reconstructs the DB from scratch using the store's manifest
// and verifies the user rows match the expected map of id→name. This catches
// bugs where the local conn diverges from what replay would produce.
func verifyReplay(t *testing.T, e *testEnv, wantUsers map[int]string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	m := e.loadManifest(t)
	replayPath := filepath.Join(dir, "replay.sqlite")
	if err := downloadSnapshot(ctx, e.store, m.Snapshot.Key, 0, replayPath); err != nil {
		t.Fatalf("replay: downloadSnapshot: %v", err)
	}
	conn, err := sqlite.OpenConn(replayPath, sqlite.OpenReadWrite)
	if err != nil {
		t.Fatalf("replay: OpenConn: %v", err)
	}
	defer conn.Close()
	if _, err := applyLog(ctx, e.store, conn, m.Log, m.Snapshot.Seq); err != nil {
		t.Fatalf("replay: applyLog: %v", err)
	}

	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users`); int(got) != len(wantUsers) {
		t.Errorf("replay row count = %d, want %d", got, len(wantUsers))
	}
	for id, name := range wantUsers {
		q := fmt.Sprintf(`SELECT name FROM users WHERE id = %d`, id)
		if got := queryString(t, conn, q); got != name {
			t.Errorf("replay user %d = %q, want %q", id, got, name)
		}
	}
}

// --- Concurrent real contention (not simulated) ----------------------------

func TestDoUpdate_ConcurrentIncrements(t *testing.T) {
	// N goroutines, each running M increments through the real commit loop.
	// Each gets its own env (separate local DB) but they share the store.
	// Final counter must equal N*M — no lost updates.
	const workers = 8
	const incrementsPerWorker = 5

	ctx := context.Background()
	dir := t.TempDir()

	// Shared store, seeded with schema + a counter row.
	store := NewMemBlobStore()
	snapPath := filepath.Join(dir, "snap-0.sqlite")
	sc := openTestDB(t, snapPath)
	mustExec(t, sc, `INSERT INTO users (id, name, balance) VALUES (1, 'counter', 0)`)
	sc.Close()
	snapKey := "mydb/snapshots/snap-0.sqlite"
	store.Put(ctx, snapKey, bytes.NewReader(snapshotBytes(t, snapPath)), NoCondition)

	m := &Manifest{Seq: 0, Snapshot: BlobRef{Key: snapKey, Seq: 0}}
	putManifest(ctx, store, "mydb/manifest.json", m, NoCondition)

	// Build one env per worker — separate local DBs, shared store.
	mkEnv := func(id int) *testEnv {
		localPath := filepath.Join(dir, fmt.Sprintf("worker-%d.sqlite", id))
		downloadSnapshot(ctx, store, snapKey, 0, localPath)
		conn, _ := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
		t.Cleanup(func() { conn.Close() })

		mm, etag, _ := loadManifest(ctx, store, "mydb/manifest.json")
		return &testEnv{
			store: store,
			cfg: &commitConfig{
				store: store, prefix: "mydb/", manifestKey: "mydb/manifest.json",
				maxRetries: 100, // high — we expect lots of contention
			},
			st:        &commitState{conn: conn, localSeq: 0, snapshotKey: snapKey, manifest: mm, etag: etag},
			localPath: localPath,
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e := mkEnv(id)
			for j := 0; j < incrementsPerWorker; j++ {
				// Refresh before each update (simulates a fresh Lambda
				// invocation).
				if err := refreshManifest(ctx, e.cfg, e.st, e.localPath); err != nil {
					t.Error(err)
					return
				}
				err := doUpdate(ctx, e.cfg, e.st, e.localPath, func(c *sqlite.Conn) error {
					return sqlitex.Execute(c, `UPDATE users SET balance = balance + 1 WHERE id = 1`, nil)
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify via fresh replay.
	finalM, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	wantSeq := int64(workers * incrementsPerWorker)
	if finalM.Seq != wantSeq {
		t.Errorf("final seq = %d, want %d", finalM.Seq, wantSeq)
	}

	replayPath := filepath.Join(dir, "replay.sqlite")
	downloadSnapshot(ctx, store, finalM.Snapshot.Key, 0, replayPath)
	rc, _ := sqlite.OpenConn(replayPath, sqlite.OpenReadWrite)
	defer rc.Close()
	applyLog(ctx, store, rc, finalM.Log, finalM.Snapshot.Seq)

	if got := queryInt(t, rc, `SELECT balance FROM users WHERE id = 1`); got != wantSeq {
		t.Errorf("final counter = %d, want %d — lost updates!", got, wantSeq)
	}
}
