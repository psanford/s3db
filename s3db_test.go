package s3db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// --- Open / bootstrap ------------------------------------------------------

func TestOpen_Bootstrap(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	db, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Manifest should exist at seq 0.
	m, _, err := loadManifest(ctx, store, "mydb/manifest.json")
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if m.Seq != 0 {
		t.Errorf("initial seq = %d, want 0", m.Seq)
	}
	if m.Snapshot.Key == "" {
		t.Error("snapshot key is empty")
	}
	if len(m.Log) != 0 {
		t.Errorf("initial log len = %d, want 0", len(m.Log))
	}

	// Snapshot blob should exist.
	if _, _, err := store.Get(ctx, m.Snapshot.Key); err != nil {
		t.Errorf("snapshot blob not in store: %v", err)
	}
}

func TestOpen_Existing(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	// First open bootstraps.
	db1, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	db1.Close()

	// Second open finds existing manifest.
	db2, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer db2.Close()

	if db2.Seq() != 0 {
		t.Errorf("seq = %d, want 0", db2.Seq())
	}
}

func TestOpen_ConcurrentBootstrap(t *testing.T) {
	// N goroutines all Open simultaneously against an empty store.
	// Exactly one should bootstrap; others should find the manifest.
	// All should succeed.
	store := NewMemBlobStore()
	ctx := context.Background()

	const workers = 10
	dbs := make([]*DB, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dbs[i], errs[i] = Open(ctx, store, "mydb/")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d Open: %v", i, err)
		}
	}
	for _, db := range dbs {
		if db != nil {
			db.Close()
		}
	}

	// Exactly one manifest should exist.
	m, _, err := loadManifest(ctx, store, "mydb/manifest.json")
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if m.Seq != 0 {
		t.Errorf("seq = %d, want 0", m.Seq)
	}
}

func TestOpen_PrefixValidation(t *testing.T) {
	store := NewMemBlobStore()
	_, err := Open(context.Background(), store, "no-trailing-slash")
	if err == nil {
		t.Error("expected error for prefix without trailing slash")
	}
}

func TestOpen_WithLocalPath(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	dir := t.TempDir()
	localPath := filepath.Join(dir, "my-local.sqlite")

	db, err := Open(ctx, store, "mydb/", WithLocalPath(localPath))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// File should exist at the specified path.
	if _, err := os.Stat(localPath); err != nil {
		t.Errorf("local file not at specified path: %v", err)
	}

	db.Close()

	// File should still exist after Close (user-owned).
	if _, err := os.Stat(localPath); err != nil {
		t.Errorf("local file removed on Close despite WithLocalPath: %v", err)
	}
}

func TestOpen_TempFileCleanup(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	db, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	localPath := db.localPath
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("temp file should exist: %v", err)
	}

	db.Close()

	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Errorf("temp file should be removed on Close, stat returned: %v", err)
	}
}

// --- Update / View end-to-end ---------------------------------------------

// seedSchema writes an initial snapshot with testSchema applied, plus a
// manifest pointing at it, directly into the store. This is what the Stage 8
// migration runner will do automatically; for Stage 6 we do it by hand
// because DDL doesn't replicate through changesets (see DESIGN.md).
//
// This MUST be called before any Open against the prefix — it uses
// If-None-Match and will fail if a manifest already exists.
func seedSchema(t *testing.T, store BlobStore, prefix string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	// Build a snapshot with the schema.
	snapPath := filepath.Join(dir, "seed.sqlite")
	conn := openTestDB(t, snapPath) // applies testSchema
	conn.Close()

	snapKey := prefix + "snapshots/snap-init.sqlite"
	f, err := os.Open(snapPath)
	if err != nil {
		t.Fatalf("seed: open snapshot: %v", err)
	}
	_, err = store.Put(ctx, snapKey, f, NoCondition)
	f.Close()
	if err != nil {
		t.Fatalf("seed: put snapshot: %v", err)
	}

	m := &manifest{
		Seq:      0,
		Snapshot: blobRef{Key: snapKey, Seq: 0},
	}
	if _, err := putManifest(ctx, store, prefix+"manifest.json", m, PutCondition{IfNoneMatch: true}); err != nil {
		t.Fatalf("seed: put manifest: %v", err)
	}
}

// openWithSchema seeds the schema (if not already done) and opens a DB.
func openWithSchema(t *testing.T, store BlobStore, prefix string, opts ...Option) *DB {
	t.Helper()
	ctx := context.Background()

	// Only seed if the manifest doesn't exist yet.
	if _, err := store.Head(ctx, prefix+"manifest.json"); errors.Is(err, ErrNotFound) {
		seedSchema(t, store, prefix)
	}

	db, err := Open(ctx, store, prefix, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func TestDB_UpdateView(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Insert via Update.
	err := db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Read via View.
	var name string
	var balance int64
	err = db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT name, balance FROM users WHERE id = 1`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				name = stmt.ColumnText(0)
				balance = stmt.ColumnInt64(1)
				return nil
			},
		})
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}

	if name != "alice" {
		t.Errorf("name = %q, want alice", name)
	}
	if balance != 100 {
		t.Errorf("balance = %d, want 100", balance)
	}
}

func TestDB_ViewSeesOtherWriters(t *testing.T) {
	// Two DB instances on the same store. Writer commits; reader's View
	// should see the write.
	store := NewMemBlobStore()
	ctx := context.Background()

	writer := openWithSchema(t, store, "mydb/")
	defer writer.Close()

	reader, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open reader: %v", err)
	}
	defer reader.Close()

	// Writer commits.
	err = writer.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
	})
	if err != nil {
		t.Fatalf("writer Update: %v", err)
	}

	// Reader's View should sync and see it.
	var count int64
	err = reader.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				count = stmt.ColumnInt64(0)
				return nil
			},
		})
	})
	if err != nil {
		t.Fatalf("reader View: %v", err)
	}
	if count != 1 {
		t.Errorf("reader count = %d, want 1", count)
	}
}

func TestDB_UpdateSeesOtherWriters(t *testing.T) {
	// Two DB instances. One writes, the other's Update should see it
	// before running fn (because Update calls refreshManifest first).
	store := NewMemBlobStore()
	ctx := context.Background()

	db1 := openWithSchema(t, store, "mydb/")
	defer db1.Close()

	db2, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	// db1 writes.
	db1.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})

	// db2's Update should see alice and be able to update her.
	err = db2.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = 200 WHERE id = 1`, nil)
	})
	if err != nil {
		t.Fatalf("db2 Update: %v", err)
	}

	// db1's View should see db2's change.
	var balance int64
	db1.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT balance FROM users WHERE id = 1`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				balance = stmt.ColumnInt64(0)
				return nil
			},
		})
	})
	if balance != 200 {
		t.Errorf("balance = %d, want 200", balance)
	}
}

func TestDB_UpdateFnError(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	wantErr := errors.New("boom")
	err := db.Update(ctx, func(c *sqlite.Conn) error {
		sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected fn error, got %v", err)
	}

	// View should see no row (rolled back).
	var count int64
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				count = stmt.ColumnInt64(0)
				return nil
			},
		})
	})
	if count != 0 {
		t.Errorf("count = %d, want 0 (rolled back)", count)
	}
}

func TestDB_UpdateNoOp(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	seqBefore := db.Seq()

	err := db.Update(ctx, func(c *sqlite.Conn) error {
		// Read-only.
		return sqlitex.Execute(c, `SELECT * FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(*sqlite.Stmt) error { return nil },
		})
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if db.Seq() != seqBefore {
		t.Errorf("seq changed from %d to %d on no-op", seqBefore, db.Seq())
	}
}

// --- Multi-instance concurrency --------------------------------------------

func TestDB_ConcurrentInstances(t *testing.T) {
	// N separate DB instances (separate "Lambda invocations") all
	// incrementing a counter. No lost updates.
	store := NewMemBlobStore()
	ctx := context.Background()

	// Bootstrap + schema + seed counter via a throwaway DB.
	seed := openWithSchema(t, store, "mydb/")
	seed.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'counter', 0)`, nil)
	})
	seed.Close()

	const workers = 8
	const incrementsPerWorker = 5

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := Open(ctx, store, "mydb/", WithMaxRetries(100))
			if err != nil {
				t.Error(err)
				return
			}
			defer db.Close()

			for j := 0; j < incrementsPerWorker; j++ {
				err := db.Update(ctx, func(c *sqlite.Conn) error {
					return sqlitex.Execute(c, `UPDATE users SET balance = balance + 1 WHERE id = 1`, nil)
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Verify via a fresh DB.
	verifier, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open verifier: %v", err)
	}
	defer verifier.Close()

	var final int64
	verifier.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT balance FROM users WHERE id = 1`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				final = stmt.ColumnInt64(0)
				return nil
			},
		})
	})

	want := int64(workers * incrementsPerWorker)
	if final != want {
		t.Errorf("final counter = %d, want %d — lost updates!", final, want)
	}
}

// --- Stats -----------------------------------------------------------------

func TestStats(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Fresh: empty log.
	s := db.Stats()
	if s.Seq != 0 {
		t.Errorf("initial Seq = %d, want 0", s.Seq)
	}
	if s.LogEntries != 0 {
		t.Errorf("initial LogEntries = %d, want 0", s.LogEntries)
	}
	if s.LogBytes != 0 {
		t.Errorf("initial LogBytes = %d, want 0", s.LogBytes)
	}

	// After writes: log grows.
	for i := 1; i <= 3; i++ {
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'u')`, i), nil)
		})
	}
	s = db.Stats()
	if s.Seq != 3 {
		t.Errorf("Seq = %d, want 3", s.Seq)
	}
	if s.LogEntries != 3 {
		t.Errorf("LogEntries = %d, want 3", s.LogEntries)
	}
	if s.LogBytes == 0 {
		t.Error("LogBytes = 0, want > 0 (sizes should be recorded)")
	}

	// After compact: log empties, snapshot size changes.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s = db.Stats()
	if s.Seq != 3 {
		t.Errorf("Seq after compact = %d, want 3 (unchanged)", s.Seq)
	}
	if s.LogEntries != 0 {
		t.Errorf("LogEntries after compact = %d, want 0", s.LogEntries)
	}
	if s.SnapshotSize == 0 {
		t.Error("SnapshotSize = 0 after compact, want > 0")
	}
}

// --- Options ---------------------------------------------------------------

func TestWithMaxRetries(t *testing.T) {
	store := NewMemBlobStore()
	db, err := Open(context.Background(), store, "mydb/", WithMaxRetries(3))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if db.cfg.maxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", db.cfg.maxRetries)
	}
}

func TestWithMigrations_ValidatesOrder(t *testing.T) {
	store := NewMemBlobStore()

	// Out-of-order versions should be rejected.
	bad := []Migration{
		{Version: 2, Up: func(*sqlite.Conn) error { return nil }},
		{Version: 1, Up: func(*sqlite.Conn) error { return nil }},
	}
	_, err := Open(context.Background(), store, "mydb/", WithMigrations(bad))
	if err == nil {
		t.Error("expected error for out-of-order migration versions")
	}
}

// --- Seq reporting ---------------------------------------------------------

func TestSeq(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	seq0 := db.Seq()

	for i := 0; i < 3; i++ {
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'u')`, i+10), nil)
		})
	}

	if db.Seq() != seq0+3 {
		t.Errorf("Seq = %d, want %d", db.Seq(), seq0+3)
	}
}
