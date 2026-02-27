package s3db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// standardMigrations returns a 3-migration sequence for testing:
//
//	v1: create users table
//	v2: add created_at column
//	v3: create orders table + backfill a row (DML)
//
// counters, if non-nil, are incremented each time the corresponding Up runs.
func standardMigrations(counters *[3]atomic.Int32) []Migration {
	inc := func(i int) {
		if counters != nil {
			counters[i].Add(1)
		}
	}
	return []Migration{
		{Version: 1, Name: "init", Up: func(c *sqlite.Conn) error {
			inc(0)
			return sqlitex.ExecuteScript(c, `
				CREATE TABLE users (
					id INTEGER PRIMARY KEY,
					name TEXT NOT NULL,
					balance INTEGER NOT NULL DEFAULT 0
				);
			`, nil)
		}},
		{Version: 2, Name: "add_created_at", Up: func(c *sqlite.Conn) error {
			inc(1)
			return sqlitex.ExecuteScript(c, `
				ALTER TABLE users ADD COLUMN created_at INTEGER;
			`, nil)
		}},
		{Version: 3, Name: "add_orders", Up: func(c *sqlite.Conn) error {
			inc(2)
			return sqlitex.ExecuteScript(c, `
				CREATE TABLE orders (
					id INTEGER PRIMARY KEY,
					user_id INTEGER NOT NULL,
					amount INTEGER NOT NULL
				);
				INSERT INTO users (id, name, balance, created_at) VALUES (1, 'system', 0, 0);
			`, nil)
		}},
	}
}

func TestMigrate_FreshDB(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	var counters [3]atomic.Int32

	db, err := Open(ctx, store, "mydb/", WithMigrations(standardMigrations(&counters)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// All three migrations should have run exactly once.
	for i := range counters {
		if n := counters[i].Load(); n != 1 {
			t.Errorf("migration %d ran %d times, want 1", i+1, n)
		}
	}

	// Manifest should be at schema v3.
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", m.SchemaVersion)
	}
	if len(m.Log) != 0 {
		t.Errorf("log len = %d, want 0 (migrations are forced compactions)", len(m.Log))
	}

	// Schema should be applied. Verify via View.
	var hasCreatedAt bool
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT name FROM pragma_table_info('users') WHERE name='created_at'`,
			&sqlitex.ExecOptions{
				ResultFunc: func(*sqlite.Stmt) error { hasCreatedAt = true; return nil },
			})
	})
	if !hasCreatedAt {
		t.Error("created_at column not present (v2 not applied)")
	}

	var orderTableExists bool
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT name FROM sqlite_master WHERE type='table' AND name='orders'`,
			&sqlitex.ExecOptions{
				ResultFunc: func(*sqlite.Stmt) error { orderTableExists = true; return nil },
			})
	})
	if !orderTableExists {
		t.Error("orders table not present (v3 not applied)")
	}

	// DML backfill from v3 should be present.
	var systemUserExists int64
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users WHERE name='system'`,
			&sqlitex.ExecOptions{
				ResultFunc: func(s *sqlite.Stmt) error { systemUserExists = s.ColumnInt64(0); return nil },
			})
	})
	if systemUserExists != 1 {
		t.Error("system user not present (v3 DML backfill missing)")
	}
}

func TestMigrate_DDLReplicates(t *testing.T) {
	// The whole point of forced-compaction migrations: DDL reaches other
	// DB instances. This is the scenario that FAILED with Update-based
	// schema in Stage 6.
	store := NewMemBlobStore()
	ctx := context.Background()
	migs := standardMigrations(nil)

	// First instance runs migrations.
	db1, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open db1: %v", err)
	}
	db1.Close()

	// Second instance should find schema in place.
	db2, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	// db2 should be able to query users — table exists.
	var count int64
	err = db2.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if err != nil {
		t.Fatalf("db2 View: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1 (system user from v3)", count)
	}
}

func TestMigrate_AlreadyApplied(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	var counters [3]atomic.Int32
	migs := standardMigrations(&counters)

	// First Open runs all migrations.
	db1, _ := Open(ctx, store, "mydb/", WithMigrations(migs))
	db1.Close()

	// Reset counters.
	for i := range counters {
		counters[i].Store(0)
	}

	// Second Open should skip all migrations.
	db2, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db2.Close()

	for i := range counters {
		if n := counters[i].Load(); n != 0 {
			t.Errorf("migration %d ran %d times on second Open, want 0", i+1, n)
		}
	}
}

func TestMigrate_Partial(t *testing.T) {
	// DB is at schema v1; client knows v1,v2,v3. Only v2,v3 should run.
	store := NewMemBlobStore()
	ctx := context.Background()

	// First, get to v1 only.
	v1Only := standardMigrations(nil)[:1]
	db1, _ := Open(ctx, store, "mydb/", WithMigrations(v1Only))
	db1.Close()

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 1 {
		t.Fatalf("setup: SchemaVersion = %d, want 1", m.SchemaVersion)
	}

	// Now open with full migrations.
	var counters [3]atomic.Int32
	db2, err := Open(ctx, store, "mydb/", WithMigrations(standardMigrations(&counters)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db2.Close()

	if counters[0].Load() != 0 {
		t.Errorf("v1 ran %d times, want 0 (already applied)", counters[0].Load())
	}
	if counters[1].Load() != 1 {
		t.Errorf("v2 ran %d times, want 1", counters[1].Load())
	}
	if counters[2].Load() != 1 {
		t.Errorf("v3 ran %d times, want 1", counters[2].Load())
	}
}

func TestMigrate_SchemaTooNew(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	// Advance DB to v3.
	db1, _ := Open(ctx, store, "mydb/", WithMigrations(standardMigrations(nil)))
	db1.Close()

	// Open with only v1,v2 known — should fail.
	oldMigs := standardMigrations(nil)[:2]
	_, err := Open(ctx, store, "mydb/", WithMigrations(oldMigs))
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Errorf("expected ErrSchemaTooNew, got %v", err)
	}
}

func TestMigrate_NoMigrations(t *testing.T) {
	// Open with no migrations on a fresh DB — schema_version stays 0.
	store := NewMemBlobStore()
	ctx := context.Background()

	db, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 0 {
		t.Errorf("SchemaVersion = %d, want 0", m.SchemaVersion)
	}
}

func TestMigrate_UpError(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()

	wantErr := errors.New("migration exploded")
	migs := []Migration{
		{Version: 1, Name: "good", Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE t (id INTEGER PRIMARY KEY);`, nil)
		}},
		{Version: 2, Name: "bad", Up: func(*sqlite.Conn) error {
			return wantErr
		}},
	}

	_, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected migration error, got %v", err)
	}

	// v1 should have been committed before v2 failed.
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1 (v1 committed before v2 failed)", m.SchemaVersion)
	}
}

func TestMigrate_Concurrent(t *testing.T) {
	// N goroutines all Open with the same migrations simultaneously.
	// Each migration should run exactly once total across all goroutines.
	store := NewMemBlobStore()
	ctx := context.Background()

	var counters [3]atomic.Int32
	migs := standardMigrations(&counters)

	const workers = 8
	errs := make([]error, workers)
	dbs := make([]*DB, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dbs[i], errs[i] = Open(ctx, store, "mydb/", WithMigrations(migs))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d Open: %v", i, err)
		}
		if dbs[i] != nil {
			dbs[i].Close()
		}
	}

	// Each migration should have run exactly once. (It's possible for the
	// Up function to run more than once if two workers get past the
	// schema-version check before either commits — but only one's CAS
	// will succeed. The test asserts ≥1 and that the final schema is
	// correct. Exactly-once requires distributed locking which we're
	// deliberately avoiding.)
	for i := range counters {
		n := counters[i].Load()
		if n < 1 {
			t.Errorf("migration %d ran %d times, want ≥1", i+1, n)
		}
		t.Logf("migration v%d ran %d times", i+1, n)
	}

	// Final state should be at v3 regardless.
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 3 {
		t.Errorf("final SchemaVersion = %d, want 3", m.SchemaVersion)
	}

	// Verify the DB is usable and the DML backfill happened exactly once
	// (even if Up ran multiple times, only one snapshot got committed).
	db, _ := Open(ctx, store, "mydb/", WithMigrations(migs))
	defer db.Close()
	var count int64
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users WHERE name='system'`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if count != 1 {
		t.Errorf("system user count = %d, want 1 (backfill should commit exactly once)", count)
	}
}

func TestMigrate_UpdateRequiresMatchingSchema(t *testing.T) {
	// After migrations, Update should work. Without them, it should reject.
	store := NewMemBlobStore()
	ctx := context.Background()
	migs := standardMigrations(nil)

	// Bring DB to v3.
	db1, _ := Open(ctx, store, "mydb/", WithMigrations(migs))

	// Update should work.
	err := db1.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (2, 'alice')`, nil)
	})
	if err != nil {
		t.Fatalf("Update with matching schema: %v", err)
	}
	db1.Close()

	// A client without migrations (schemaVer=0) should be rejected.
	db2, _ := Open(ctx, store, "mydb/")
	// Open succeeds (no migrations to check), but...
	// Actually Open should fail because runMigrations checks schema_version
	// against maxVer=0 and the DB is at 3 → ErrSchemaTooNew.
	// Wait, let me check the logic... runMigrations is always called, and
	// with no migrations maxVer=0, manifest.SchemaVersion=3 > 0 → error.
	// So db2 should be nil.
	if db2 != nil {
		db2.Close()
		t.Error("Open without migrations succeeded against v3 DB; expected ErrSchemaTooNew")
	}
}

func TestMigrate_GapInVersions(t *testing.T) {
	// Migrations can skip version numbers (v1, v3, v7) — we only require
	// strictly increasing.
	store := NewMemBlobStore()
	ctx := context.Background()

	migs := []Migration{
		{Version: 1, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE a (id INTEGER PRIMARY KEY);`, nil)
		}},
		{Version: 5, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE b (id INTEGER PRIMARY KEY);`, nil)
		}},
		{Version: 10, Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `CREATE TABLE c (id INTEGER PRIMARY KEY);`, nil)
		}},
	}

	db, err := Open(ctx, store, "mydb/", WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 10 {
		t.Errorf("SchemaVersion = %d, want 10", m.SchemaVersion)
	}

	for _, tbl := range []string{"a", "b", "c"} {
		var exists bool
		db.View(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c,
				fmt.Sprintf(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='%s'`, tbl),
				&sqlitex.ExecOptions{ResultFunc: func(*sqlite.Stmt) error { exists = true; return nil }})
		})
		if !exists {
			t.Errorf("table %s not created", tbl)
		}
	}
}
