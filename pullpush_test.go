package s3db

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestPull_FreshDB(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	seedSchema(t, store, "mydb/")

	outPath := filepath.Join(t.TempDir(), "pulled.sqlite")
	info, err := Pull(ctx, store, "mydb/", outPath)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	if info.Seq != 0 {
		t.Errorf("Seq = %d, want 0", info.Seq)
	}
	if info.LogEntries != 0 {
		t.Errorf("LogEntries = %d, want 0", info.LogEntries)
	}

	// File should be a valid SQLite DB with the schema.
	conn, err := sqlite.OpenConn(outPath, sqlite.OpenReadOnly)
	if err != nil {
		t.Fatalf("open pulled file: %v", err)
	}
	defer conn.Close()

	var hasUsers bool
	sqlitex.Execute(conn, `SELECT 1 FROM sqlite_master WHERE type='table' AND name='users'`,
		&sqlitex.ExecOptions{ResultFunc: func(*sqlite.Stmt) error { hasUsers = true; return nil }})
	if !hasUsers {
		t.Error("users table not found in pulled file")
	}
}

func TestPull_AppliesLog(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")

	// Build up some log entries.
	for i := 0; i < 3; i++ {
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (?, 'u')`,
				&sqlitex.ExecOptions{Args: []any{i}})
		})
	}
	db.Close()

	outPath := filepath.Join(t.TempDir(), "pulled.sqlite")
	info, err := Pull(ctx, store, "mydb/", outPath)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	if info.Seq != 3 {
		t.Errorf("Seq = %d, want 3", info.Seq)
	}
	if info.LogEntries != 3 {
		t.Errorf("LogEntries = %d, want 3", info.LogEntries)
	}

	// Pulled file should have all 3 rows.
	conn, err := sqlite.OpenConn(outPath, sqlite.OpenReadOnly)
	if err != nil {
		t.Fatalf("open pulled file: %v", err)
	}
	defer conn.Close()

	var count int64
	sqlitex.Execute(conn, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
		ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
	})
	if count != 3 {
		t.Errorf("row count = %d, want 3", count)
	}
}

func TestPush_Roundtrip(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	seedSchema(t, store, "mydb/")

	// Pull, edit, push.
	pullPath := filepath.Join(t.TempDir(), "db.sqlite")
	info, err := Pull(ctx, store, "mydb/", pullPath)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Edit the pulled file directly.
	conn, _ := sqlite.OpenConn(pullPath, sqlite.OpenReadWrite)
	sqlitex.Execute(conn, `INSERT INTO users (id, name, balance) VALUES (42, 'pushed', 100)`, nil)
	conn.Close()

	// Push.
	if err := Push(ctx, store, "mydb/", pullPath, info.Seq); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// A fresh Open should see the pushed data.
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	var name string
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT name FROM users WHERE id = 42`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { name = s.ColumnText(0); return nil },
		})
	})
	if name != "pushed" {
		t.Errorf("name = %q, want 'pushed'", name)
	}

	// Manifest should have empty log (push is a snapshot replacement).
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if len(m.Log) != 0 {
		t.Errorf("log len = %d, want 0 (push clears log)", len(m.Log))
	}
	// Seq should be unchanged (push doesn't advance it).
	if m.Seq != info.Seq {
		t.Errorf("seq = %d, want %d (push preserves seq)", m.Seq, info.Seq)
	}
}

func TestPush_SeqMismatch(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")

	// Pull at seq 0.
	pullPath := filepath.Join(t.TempDir(), "db.sqlite")
	info, err := Pull(ctx, store, "mydb/", pullPath)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Someone else writes — advances seq.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})
	db.Close()

	// Push with the old seq should fail.
	err = Push(ctx, store, "mydb/", pullPath, info.Seq)
	if !errors.Is(err, ErrSeqMismatch) {
		t.Errorf("expected ErrSeqMismatch, got %v", err)
	}
}

func TestPush_Force(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")

	pullPath := filepath.Join(t.TempDir(), "db.sqlite")
	if _, err := Pull(ctx, store, "mydb/", pullPath); err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// Advance seq.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'concurrent')`, nil)
	})
	db.Close()

	// Push with -1 (force) should succeed — and overwrite the concurrent write.
	if err := Push(ctx, store, "mydb/", pullPath, -1); err != nil {
		t.Fatalf("Push with force: %v", err)
	}

	// The concurrent write should be gone.
	db2 := openWithSchema(t, store, "mydb/")
	defer db2.Close()
	var count int64
	db2.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if count != 0 {
		t.Errorf("count = %d, want 0 (force push overwrote concurrent write)", count)
	}
}

func TestPush_ValidatesSQLite(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	seedSchema(t, store, "mydb/")

	// A non-SQLite file.
	bad := filepath.Join(t.TempDir(), "bad.sqlite")
	os.WriteFile(bad, []byte("not a sqlite file"), 0644)

	err := Push(ctx, store, "mydb/", bad, -1)
	if err == nil {
		t.Fatal("Push with invalid SQLite file succeeded; expected error")
	}

	// A nonexistent file.
	err = Push(ctx, store, "mydb/", "/nonexistent/path.sqlite", -1)
	if err == nil {
		t.Fatal("Push with missing file succeeded; expected error")
	}
}

func TestPush_PreservesSchemaVersion(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	// Bring DB to schema v2.
	migs := []Migration{
		{Version: 1, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE t (id INTEGER PRIMARY KEY);`, nil)
		}},
		{Version: 2, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `ALTER TABLE t ADD COLUMN x INTEGER;`, nil)
		}},
	}
	db, _ := Open(ctx, store, "mydb/", WithMigrations(migs))
	db.Close()

	pullPath := filepath.Join(t.TempDir(), "db.sqlite")
	info, _ := Pull(ctx, store, "mydb/", pullPath)
	if info.SchemaVersion != 2 {
		t.Fatalf("SchemaVersion = %d, want 2", info.SchemaVersion)
	}

	if err := Push(ctx, store, "mydb/", pullPath, info.Seq); err != nil {
		t.Fatalf("Push: %v", err)
	}

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 2 {
		t.Errorf("SchemaVersion = %d, want 2 (push must preserve)", m.SchemaVersion)
	}
}

func TestPull_PrefixValidation(t *testing.T) {
	store := NewMemBlobStore()
	_, err := Pull(context.Background(), store, "noslash", "/tmp/x")
	if err == nil {
		t.Error("expected error for prefix without trailing slash")
	}
}

func TestPush_PrefixValidation(t *testing.T) {
	store := NewMemBlobStore()
	err := Push(context.Background(), store, "noslash", "/tmp/x", 0)
	if err == nil {
		t.Error("expected error for prefix without trailing slash")
	}
}

func TestPull_NotFound(t *testing.T) {
	store := NewMemBlobStore()
	_, err := Pull(context.Background(), store, "empty/", filepath.Join(t.TempDir(), "x"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPush_ConcurrentCAS(t *testing.T) {
	// Push's seq check can pass but the CAS still fail if someone commits
	// in the window between loadManifest and putManifest. Verify this
	// returns ErrSeqMismatch rather than a generic error.
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")

	pullPath := filepath.Join(t.TempDir(), "db.sqlite")
	info, _ := Pull(ctx, store, "mydb/", pullPath)

	// Interposing store: commit a write right before the manifest PUT.
	is := &pushInterposingStore{MemBlobStore: store, writer: db}

	err := Push(ctx, is, "mydb/", pullPath, info.Seq)
	if !errors.Is(err, ErrSeqMismatch) {
		t.Errorf("expected ErrSeqMismatch from CAS window race, got %v", err)
	}
	db.Close()
}

// pushInterposingStore injects a concurrent write just before the manifest
// PUT, to trigger a CAS failure even when the seq check passed.
type pushInterposingStore struct {
	*MemBlobStore
	writer *DB
	fired  bool
}

func (s *pushInterposingStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if !s.fired && cond.IfMatch != "" {
		s.fired = true
		// This write goes through the inner store (writer.cfg.store),
		// not through us, so no recursion.
		s.writer.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (99, 'interloper')`, nil)
		})
	}
	return s.MemBlobStore.Put(ctx, key, body, cond)
}
