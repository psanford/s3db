package s3db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// --- Test fixtures ---------------------------------------------------------
//
// These helpers build SQLite databases and changesets for test setup.
// They are not the production session-capture code (that's Stage 4); they
// exist only to produce known fixtures for testing the apply/sync paths.

// testSchema defines a simple table with an explicit PRIMARY KEY (required
// by the session extension).
const testSchema = `
	CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		balance INTEGER NOT NULL DEFAULT 0
	);
`

// openTestDB creates a fresh SQLite file with testSchema applied and returns
// a connection. The caller must close the connection.
func openTestDB(t *testing.T, path string) *sqlite.Conn {
	t.Helper()
	conn, err := sqlite.OpenConn(path, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		t.Fatalf("OpenConn: %v", err)
	}
	if err := sqlitex.ExecuteScript(conn, testSchema, nil); err != nil {
		conn.Close()
		t.Fatalf("apply schema: %v", err)
	}
	return conn
}

// mustExec runs a statement and fails the test on error.
func mustExec(t *testing.T, conn *sqlite.Conn, sql string) {
	t.Helper()
	if err := sqlitex.Execute(conn, sql, nil); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// queryInt runs a single-row single-column query and returns the int result.
func queryInt(t *testing.T, conn *sqlite.Conn, sql string) int64 {
	t.Helper()
	var result int64
	err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			result = stmt.ColumnInt64(0)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return result
}

// queryString is like queryInt but for text columns.
func queryString(t *testing.T, conn *sqlite.Conn, sql string) string {
	t.Helper()
	var result string
	err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			result = stmt.ColumnText(0)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return result
}

// captureChangeset runs fn inside a session on conn and returns the resulting
// changeset bytes. This is a test-only miniature of what Stage 4 will build
// properly.
func captureChangeset(t *testing.T, conn *sqlite.Conn, fn func()) []byte {
	t.Helper()
	sess, err := conn.CreateSession("")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer sess.Delete()

	// Attach all tables.
	if err := sess.Attach(""); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	fn()

	var buf bytes.Buffer
	if err := sess.WriteChangeset(&buf); err != nil {
		t.Fatalf("WriteChangeset: %v", err)
	}
	return buf.Bytes()
}

// snapshotBytes returns the full content of the SQLite file at path.
// The caller should close any write connection first so the file is flushed.
func snapshotBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data
}

// --- applyChangeset tests --------------------------------------------------

func TestApplyChangeset_Insert(t *testing.T) {
	dir := t.TempDir()

	// Build a changeset that inserts two rows.
	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
		mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`)
	})
	srcConn.Close()

	// Apply to a fresh DB.
	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}

	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}
	if got := queryString(t, dstConn, `SELECT name FROM users WHERE id = 1`); got != "alice" {
		t.Errorf("user 1 name = %q, want alice", got)
	}
	if got := queryInt(t, dstConn, `SELECT balance FROM users WHERE id = 2`); got != 200 {
		t.Errorf("user 2 balance = %d, want 200", got)
	}
}

func TestApplyChangeset_Update(t *testing.T) {
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `UPDATE users SET balance = 150 WHERE id = 1`)
	})
	srcConn.Close()

	// Apply to a DB that has the same initial state.
	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}

	if got := queryInt(t, dstConn, `SELECT balance FROM users WHERE id = 1`); got != 150 {
		t.Errorf("balance = %d, want 150", got)
	}
}

func TestApplyChangeset_Delete(t *testing.T) {
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `DELETE FROM users WHERE id = 1`)
	})
	srcConn.Close()

	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}

	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users`); got != 0 {
		t.Errorf("row count = %d, want 0", got)
	}
}

func TestApplyChangeset_Empty(t *testing.T) {
	dir := t.TempDir()
	dstConn := openTestDB(t, filepath.Join(dir, "dst.sqlite"))
	defer dstConn.Close()

	if err := applyChangeset(dstConn, nil, conflictAbort); err != nil {
		t.Errorf("empty changeset should be no-op, got %v", err)
	}
	if err := applyChangeset(dstConn, []byte{}, conflictAbort); err != nil {
		t.Errorf("empty changeset should be no-op, got %v", err)
	}
}

func TestApplyChangeset_ConflictData(t *testing.T) {
	// Changeset records UPDATE from balance=100 to balance=150.
	// Target DB has balance=999 (diverged). Should abort with conflict.
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `UPDATE users SET balance = 150 WHERE id = 1`)
	})
	srcConn.Close()

	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 999)`)

	err := applyChangeset(dstConn, cs, conflictAbort)
	var ce *errChangesetConflict
	if !errors.As(err, &ce) {
		t.Fatalf("expected errChangesetConflict, got %v", err)
	}
	if ce.kind != sqlite.ChangesetData {
		t.Errorf("conflict type = %v, want ChangesetData", ce.kind)
	}

	// Verify the target was not modified (rollback happened).
	if got := queryInt(t, dstConn, `SELECT balance FROM users WHERE id = 1`); got != 999 {
		t.Errorf("balance = %d, want 999 (unchanged after abort)", got)
	}
}

func TestApplyChangeset_ConflictInsert(t *testing.T) {
	// Changeset inserts id=1. Target already has id=1. Should conflict.
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	})
	srcConn.Close()

	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (1, 'different', 0)`)

	err := applyChangeset(dstConn, cs, conflictAbort)
	var ce *errChangesetConflict
	if !errors.As(err, &ce) {
		t.Fatalf("expected errChangesetConflict, got %v", err)
	}
	if ce.kind != sqlite.ChangesetConflict {
		t.Errorf("conflict type = %v, want ChangesetConflict", ce.kind)
	}
}

func TestApplyChangeset_ConflictNotFound(t *testing.T) {
	// Changeset updates id=1. Target has no row with id=1.
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `UPDATE users SET balance = 150 WHERE id = 1`)
	})
	srcConn.Close()

	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	// Don't insert anything — target is empty.

	err := applyChangeset(dstConn, cs, conflictAbort)
	var ce *errChangesetConflict
	if !errors.As(err, &ce) {
		t.Fatalf("expected errChangesetConflict, got %v", err)
	}
	if ce.kind != sqlite.ChangesetNotFound {
		t.Errorf("conflict type = %v, want ChangesetNotFound", ce.kind)
	}
}

func TestApplyChangeset_PartialRollback(t *testing.T) {
	// Changeset has two inserts; the second conflicts. Verify the first
	// insert is rolled back too.
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	cs := captureChangeset(t, srcConn, func() {
		mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
		mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`)
	})
	srcConn.Close()

	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn := openTestDB(t, dstPath)
	defer dstConn.Close()
	// Pre-insert id=2 to cause conflict on the second change.
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (2, 'existing', 0)`)

	err := applyChangeset(dstConn, cs, conflictAbort)
	if err == nil {
		t.Fatal("expected conflict error")
	}

	// id=1 should NOT have been inserted (rolled back).
	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users WHERE id = 1`); got != 0 {
		t.Errorf("id=1 was inserted despite later conflict: count = %d", got)
	}
	// id=2 should still be the original.
	if got := queryString(t, dstConn, `SELECT name FROM users WHERE id = 2`); got != "existing" {
		t.Errorf("id=2 name = %q, want existing", got)
	}
}

// --- downloadSnapshot tests ------------------------------------------------

func TestDownloadSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := NewMemBlobStore()

	// Build a snapshot and put it in the store.
	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn := openTestDB(t, srcPath)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`)
	srcConn.Close()

	snap := snapshotBytes(t, srcPath)
	store.Put(ctx, "snapshots/snap-1.sqlite", bytes.NewReader(snap), NoCondition)

	// Download to a new path.
	dstPath := filepath.Join(dir, "dst.sqlite")
	if err := downloadSnapshot(ctx, store, "snapshots/snap-1.sqlite", dstPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}

	// Open and verify.
	dstConn, err := sqlite.OpenConn(dstPath, sqlite.OpenReadOnly)
	if err != nil {
		t.Fatalf("OpenConn: %v", err)
	}
	defer dstConn.Close()

	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}
	if got := queryString(t, dstConn, `SELECT name FROM users WHERE id = 1`); got != "alice" {
		t.Errorf("user 1 name = %q, want alice", got)
	}
}

func TestDownloadSnapshot_ReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store := NewMemBlobStore()

	dstPath := filepath.Join(dir, "db.sqlite")

	// Put two different snapshots in the store.
	path1 := filepath.Join(dir, "v1.sqlite")
	c1 := openTestDB(t, path1)
	mustExec(t, c1, `INSERT INTO users (id, name) VALUES (1, 'v1-user')`)
	c1.Close()
	store.Put(ctx, "snap-v1", bytes.NewReader(snapshotBytes(t, path1)), NoCondition)

	path2 := filepath.Join(dir, "v2.sqlite")
	c2 := openTestDB(t, path2)
	mustExec(t, c2, `INSERT INTO users (id, name) VALUES (1, 'v2-user')`)
	c2.Close()
	store.Put(ctx, "snap-v2", bytes.NewReader(snapshotBytes(t, path2)), NoCondition)

	// Download v1, then v2 over it.
	if err := downloadSnapshot(ctx, store, "snap-v1", dstPath); err != nil {
		t.Fatalf("download v1: %v", err)
	}
	if err := downloadSnapshot(ctx, store, "snap-v2", dstPath); err != nil {
		t.Fatalf("download v2: %v", err)
	}

	conn, _ := sqlite.OpenConn(dstPath, sqlite.OpenReadOnly)
	defer conn.Close()
	if got := queryString(t, conn, `SELECT name FROM users WHERE id = 1`); got != "v2-user" {
		t.Errorf("name = %q, want v2-user", got)
	}
}

func TestDownloadSnapshot_NotFound(t *testing.T) {
	store := NewMemBlobStore()
	dstPath := filepath.Join(t.TempDir(), "dst.sqlite")

	err := downloadSnapshot(context.Background(), store, "missing", dstPath)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// dstPath should not exist.
	if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
		t.Error("dest file was created despite download failure")
	}
}

// --- applyLog tests --------------------------------------------------------

// setupLogFixture builds a snapshot at seq=0 (2 initial rows) and three
// changesets at seq 1, 2, 3 in the store. Returns the store and a manifest
// describing it.
func setupLogFixture(t *testing.T, dir string) (*MemBlobStore, *Manifest) {
	t.Helper()
	ctx := context.Background()
	store := NewMemBlobStore()

	// Build snapshot: 2 initial users.
	snapPath := filepath.Join(dir, "snap.sqlite")
	conn := openTestDB(t, snapPath)
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`)

	// Changeset 1: insert carol.
	cs1 := captureChangeset(t, conn, func() {
		mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (3, 'carol', 300)`)
	})
	// Changeset 2: update alice's balance.
	cs2 := captureChangeset(t, conn, func() {
		mustExec(t, conn, `UPDATE users SET balance = 150 WHERE id = 1`)
	})
	// Changeset 3: delete bob.
	cs3 := captureChangeset(t, conn, func() {
		mustExec(t, conn, `DELETE FROM users WHERE id = 2`)
	})
	conn.Close()

	// The snapshot file now has ALL changes applied (since captureChangeset
	// applies as it records). Rebuild a clean snapshot with only the initial
	// state.
	os.Remove(snapPath)
	conn = openTestDB(t, snapPath)
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`)
	conn.Close()

	// Upload everything.
	store.Put(ctx, "snapshots/snap-0.sqlite", bytes.NewReader(snapshotBytes(t, snapPath)), NoCondition)
	store.Put(ctx, "changesets/snap-0/cs-1.bin", bytes.NewReader(cs1), NoCondition)
	store.Put(ctx, "changesets/snap-0/cs-2.bin", bytes.NewReader(cs2), NoCondition)
	store.Put(ctx, "changesets/snap-0/cs-3.bin", bytes.NewReader(cs3), NoCondition)

	m := &Manifest{
		Seq:      3,
		Snapshot: BlobRef{Key: "snapshots/snap-0.sqlite", Seq: 0},
		Log: []LogEntry{
			{Key: "changesets/snap-0/cs-1.bin", Seq: 1},
			{Key: "changesets/snap-0/cs-2.bin", Seq: 2},
			{Key: "changesets/snap-0/cs-3.bin", Seq: 3},
		},
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("fixture manifest invalid: %v", err)
	}
	return store, m
}

func TestApplyLog_Full(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, m := setupLogFixture(t, dir)

	// Download snapshot and apply full log.
	localPath := filepath.Join(dir, "local.sqlite")
	if err := downloadSnapshot(ctx, store, m.Snapshot.Key, localPath); err != nil {
		t.Fatalf("downloadSnapshot: %v", err)
	}
	conn, err := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	if err != nil {
		t.Fatalf("OpenConn: %v", err)
	}
	defer conn.Close()

	seq, err := applyLog(ctx, store, conn, m.Log, m.Snapshot.Seq)
	if err != nil {
		t.Fatalf("applyLog: %v", err)
	}
	if seq != 3 {
		t.Errorf("final seq = %d, want 3", seq)
	}

	// Verify final state: alice (balance 150), carol (balance 300). bob deleted.
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}
	if got := queryInt(t, conn, `SELECT balance FROM users WHERE id = 1`); got != 150 {
		t.Errorf("alice balance = %d, want 150", got)
	}
	if got := queryString(t, conn, `SELECT name FROM users WHERE id = 3`); got != "carol" {
		t.Errorf("user 3 = %q, want carol", got)
	}
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users WHERE id = 2`); got != 0 {
		t.Errorf("bob still present")
	}
}

func TestApplyLog_Incremental(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, m := setupLogFixture(t, dir)

	// Download snapshot and apply just cs1+cs2 (simulate local at seq=2).
	localPath := filepath.Join(dir, "local.sqlite")
	if err := downloadSnapshot(ctx, store, m.Snapshot.Key, localPath); err != nil {
		t.Fatal(err)
	}
	conn, _ := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	defer conn.Close()

	seq, err := applyLog(ctx, store, conn, m.Log[:2], 0)
	if err != nil {
		t.Fatalf("applyLog (first pass): %v", err)
	}
	if seq != 2 {
		t.Errorf("seq after first pass = %d, want 2", seq)
	}

	// Now apply the full log with fromSeq=2. Only cs3 should be applied.
	seq, err = applyLog(ctx, store, conn, m.Log, 2)
	if err != nil {
		t.Fatalf("applyLog (incremental): %v", err)
	}
	if seq != 3 {
		t.Errorf("final seq = %d, want 3", seq)
	}

	// Verify final state matches full apply.
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users WHERE id = 2`); got != 0 {
		t.Errorf("bob still present after incremental apply")
	}
}

func TestApplyLog_Empty(t *testing.T) {
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	seq, err := applyLog(context.Background(), NewMemBlobStore(), conn, nil, 5)
	if err != nil {
		t.Fatalf("applyLog: %v", err)
	}
	if seq != 5 {
		t.Errorf("seq = %d, want 5 (unchanged)", seq)
	}
}

func TestApplyLog_AllAlreadyApplied(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, m := setupLogFixture(t, dir)

	localPath := filepath.Join(dir, "local.sqlite")
	downloadSnapshot(ctx, store, m.Snapshot.Key, localPath)
	conn, _ := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	defer conn.Close()

	// Claim we're already at seq 3 — nothing should be fetched or applied.
	seq, err := applyLog(ctx, store, conn, m.Log, 3)
	if err != nil {
		t.Fatalf("applyLog: %v", err)
	}
	if seq != 3 {
		t.Errorf("seq = %d, want 3", seq)
	}

	// DB should still be at snapshot state (no changesets applied).
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("row count = %d, want 2 (snapshot state)", got)
	}
	if got := queryInt(t, conn, `SELECT balance FROM users WHERE id = 1`); got != 100 {
		t.Errorf("alice balance = %d, want 100 (snapshot state)", got)
	}
}

func TestApplyLog_MissingChangeset(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	store, m := setupLogFixture(t, dir)

	// Delete cs2 from the store.
	store.Delete(ctx, "changesets/snap-0/cs-2.bin")

	localPath := filepath.Join(dir, "local.sqlite")
	downloadSnapshot(ctx, store, m.Snapshot.Key, localPath)
	conn, _ := sqlite.OpenConn(localPath, sqlite.OpenReadWrite)
	defer conn.Close()

	seq, err := applyLog(ctx, store, conn, m.Log, 0)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	// Should have stopped after cs1.
	if seq != 1 {
		t.Errorf("seq = %d, want 1 (stopped at last successful)", seq)
	}
}
