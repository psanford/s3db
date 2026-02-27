package s3db

import (
	"errors"
	"path/filepath"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestCapture_Insert(t *testing.T) {
	dir := t.TempDir()

	// Source: capture an insert.
	srcConn := openTestDB(t, filepath.Join(dir, "src.sqlite"))
	defer srcConn.Close()

	cs, err := capture(srcConn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if len(cs) == 0 {
		t.Fatal("changeset is empty")
	}

	// Verify fn's changes were applied to the source conn.
	if got := queryInt(t, srcConn, `SELECT COUNT(*) FROM users`); got != 1 {
		t.Errorf("src row count = %d, want 1", got)
	}

	// Replay onto a fresh DB.
	dstConn := openTestDB(t, filepath.Join(dir, "dst.sqlite"))
	defer dstConn.Close()

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}
	if got := queryString(t, dstConn, `SELECT name FROM users WHERE id = 1`); got != "alice" {
		t.Errorf("dst user 1 = %q, want alice", got)
	}
}

func TestCapture_MultiStatement(t *testing.T) {
	dir := t.TempDir()

	srcConn := openTestDB(t, filepath.Join(dir, "src.sqlite"))
	defer srcConn.Close()
	mustExec(t, srcConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	cs, err := capture(srcConn, func(c *sqlite.Conn) error {
		if err := sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (2, 'bob', 200)`, nil); err != nil {
			return err
		}
		if err := sqlitex.Execute(c, `UPDATE users SET balance = 150 WHERE id = 1`, nil); err != nil {
			return err
		}
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (3, 'carol', 300)`, nil)
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	// Replay onto a DB with the same initial state.
	dstConn := openTestDB(t, filepath.Join(dir, "dst.sqlite"))
	defer dstConn.Close()
	mustExec(t, dstConn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}

	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users`); got != 3 {
		t.Errorf("row count = %d, want 3", got)
	}
	if got := queryInt(t, dstConn, `SELECT balance FROM users WHERE id = 1`); got != 150 {
		t.Errorf("alice balance = %d, want 150", got)
	}
}

func TestCapture_MultiTable(t *testing.T) {
	dir := t.TempDir()

	// Schema with two tables.
	schema := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER);
	`

	srcPath := filepath.Join(dir, "src.sqlite")
	srcConn, _ := sqlite.OpenConn(srcPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	defer srcConn.Close()
	sqlitex.ExecuteScript(srcConn, schema, nil)

	cs, err := capture(srcConn, func(c *sqlite.Conn) error {
		if err := sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil); err != nil {
			return err
		}
		return sqlitex.Execute(c, `INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 500)`, nil)
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	// Replay onto a fresh DB with the same schema.
	dstPath := filepath.Join(dir, "dst.sqlite")
	dstConn, _ := sqlite.OpenConn(dstPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	defer dstConn.Close()
	sqlitex.ExecuteScript(dstConn, schema, nil)

	if err := applyChangeset(dstConn, cs, conflictAbort); err != nil {
		t.Fatalf("applyChangeset: %v", err)
	}

	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM users`); got != 1 {
		t.Errorf("users count = %d, want 1", got)
	}
	if got := queryInt(t, dstConn, `SELECT COUNT(*) FROM orders`); got != 1 {
		t.Errorf("orders count = %d, want 1", got)
	}
	if got := queryInt(t, dstConn, `SELECT amount FROM orders WHERE id = 1`); got != 500 {
		t.Errorf("order amount = %d, want 500", got)
	}
}

func TestCapture_FnError(t *testing.T) {
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	wantErr := errors.New("fn failed")
	cs, err := capture(conn, func(c *sqlite.Conn) error {
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Errorf("expected fn error to pass through unwrapped, got %v", err)
	}
	if cs != nil {
		t.Errorf("changeset should be nil on fn error, got %d bytes", len(cs))
	}
}

func TestCapture_FnErrorAfterChanges(t *testing.T) {
	// fn makes a change then returns an error. capture should return nil
	// changeset and the error. The change is still applied to conn — rolling
	// back is the caller's responsibility (via SAVEPOINT in the commit loop).
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	wantErr := errors.New("halfway failure")
	cs, err := capture(conn, func(c *sqlite.Conn) error {
		sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'alice')`, nil)
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wantErr, got %v", err)
	}
	if cs != nil {
		t.Error("changeset should be nil on fn error")
	}

	// The insert IS applied — capture doesn't roll back. This is intentional;
	// see the godoc.
	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users`); got != 1 {
		t.Errorf("expected 1 row (capture does not roll back), got %d", got)
	}
}

func TestCapture_SelectsOnly(t *testing.T) {
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	cs, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT * FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error { return nil },
		})
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if cs != nil {
		t.Errorf("SELECT-only fn should produce nil changeset, got %d bytes", len(cs))
	}
}

func TestCapture_NoOpUpdate(t *testing.T) {
	// UPDATE that doesn't actually change any value.
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`)

	cs, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = 100 WHERE id = 1`, nil)
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if cs != nil {
		t.Errorf("no-op UPDATE should produce nil changeset, got %d bytes", len(cs))
	}
}

func TestCapture_UpdateNoMatch(t *testing.T) {
	// UPDATE with a WHERE clause that matches nothing.
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	cs, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = 999 WHERE id = 42`, nil)
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if cs != nil {
		t.Errorf("UPDATE matching no rows should produce nil changeset, got %d bytes", len(cs))
	}
}

func TestCapture_WithSavepoint(t *testing.T) {
	// Demonstrate the intended pattern: caller wraps capture in a SAVEPOINT
	// so fn's changes can be rolled back.
	dir := t.TempDir()
	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	// Success path: release keeps fn's changes.
	func() {
		var err error
		release := sqlitex.Save(conn)
		defer release(&err)

		_, err = capture(conn, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'kept')`, nil)
		})
	}()

	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users WHERE id = 1`); got != 1 {
		t.Errorf("row 1 should exist after release, got count %d", got)
	}

	// Failure path: rollback discards fn's changes.
	func() {
		var err error
		release := sqlitex.Save(conn)
		defer release(&err)

		_, _ = capture(conn, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (2, 'rolled-back')`, nil)
		})

		err = errors.New("simulate retry decision") // triggers rollback
	}()

	if got := queryInt(t, conn, `SELECT COUNT(*) FROM users WHERE id = 2`); got != 0 {
		t.Errorf("row 2 should be rolled back, got count %d", got)
	}
}

func TestCapture_Roundtrip(t *testing.T) {
	// capture → apply → capture → apply should produce equivalent final
	// states regardless of which path was taken.
	dir := t.TempDir()

	conn := openTestDB(t, filepath.Join(dir, "db.sqlite"))
	defer conn.Close()

	cs1, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'alice', 100)`, nil)
	})
	if err != nil {
		t.Fatalf("capture cs1: %v", err)
	}

	cs2, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = 200 WHERE id = 1`, nil)
	})
	if err != nil {
		t.Fatalf("capture cs2: %v", err)
	}

	// Apply both to a fresh DB.
	replica := openTestDB(t, filepath.Join(dir, "replica.sqlite"))
	defer replica.Close()

	if err := applyChangeset(replica, cs1, conflictAbort); err != nil {
		t.Fatalf("apply cs1: %v", err)
	}
	if err := applyChangeset(replica, cs2, conflictAbort); err != nil {
		t.Fatalf("apply cs2: %v", err)
	}

	// Replica should match source.
	srcBal := queryInt(t, conn, `SELECT balance FROM users WHERE id = 1`)
	repBal := queryInt(t, replica, `SELECT balance FROM users WHERE id = 1`)
	if srcBal != repBal {
		t.Errorf("source balance = %d, replica = %d; should match", srcBal, repBal)
	}
	if repBal != 200 {
		t.Errorf("replica balance = %d, want 200", repBal)
	}
}

// --- PK-less table detection ------------------------------------------------

// openPKlessDB creates a database with a mix of PK and PK-less tables.
func openPKlessDB(t *testing.T, path string) *sqlite.Conn {
	t.Helper()
	conn, err := sqlite.OpenConn(path, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		t.Fatalf("OpenConn: %v", err)
	}
	mustExec(t, conn, `CREATE TABLE good (id INTEGER PRIMARY KEY, val TEXT)`)
	mustExec(t, conn, `CREATE TABLE bad (name TEXT, val TEXT)`) // no PK
	mustExec(t, conn, `CREATE TABLE also_bad (x INTEGER, y INTEGER)`) // no PK
	mustExec(t, conn, `INSERT INTO bad (name, val) VALUES ('k', 'v1')`)
	return conn
}

func TestCapture_PKlessTable_Error(t *testing.T) {
	// Modifying a PK-less table should return ErrUnrecordedChanges
	// rather than silently producing an empty changeset.
	conn := openPKlessDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	_, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE bad SET val = 'v2' WHERE name = 'k'`, nil)
	})

	var urc *ErrUnrecordedChanges
	if !errors.As(err, &urc) {
		t.Fatalf("expected ErrUnrecordedChanges, got %v", err)
	}
	if urc.Rows != 1 {
		t.Errorf("Rows = %d, want 1", urc.Rows)
	}
	// Both PK-less tables should be listed (we report all candidates,
	// not just the one that was modified — we can't tell which without
	// more machinery, and listing all is more helpful for fixing the schema).
	if len(urc.PKlessTables) != 2 {
		t.Errorf("PKlessTables = %v, want 2 tables", urc.PKlessTables)
	}
	t.Logf("error message: %v", err)
}

func TestCapture_PKlessTable_InsertError(t *testing.T) {
	// INSERT into PK-less table should also be caught.
	conn := openPKlessDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	_, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO also_bad (x, y) VALUES (1, 2)`, nil)
	})

	var urc *ErrUnrecordedChanges
	if !errors.As(err, &urc) {
		t.Fatalf("expected ErrUnrecordedChanges, got %v", err)
	}
}

func TestCapture_PKlessTable_GoodTableOK(t *testing.T) {
	// Modifying only the PK table should work fine even though PK-less
	// tables exist in the schema.
	conn := openPKlessDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	cs, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO good (id, val) VALUES (1, 'x')`, nil)
	})
	if err != nil {
		t.Fatalf("capture on PK table: %v", err)
	}
	if len(cs) == 0 {
		t.Fatal("changeset is empty")
	}
}

func TestCapture_NoOpUpdate_NoFalsePositive(t *testing.T) {
	// A no-op UPDATE on a PK table increments total_changes but the
	// session correctly produces an empty changeset (no actual value
	// change). This must NOT trigger ErrUnrecordedChanges — the check
	// only fires when PK-less tables exist in the schema. With all-PK
	// schema, empty changeset after row modification is assumed benign.
	conn := openTestDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()
	mustExec(t, conn, `INSERT INTO users (id, name, balance) VALUES (1, 'a', 100)`)

	cs, err := capture(conn, func(c *sqlite.Conn) error {
		// Same value — SQLite says 1 row affected, session says no change.
		return sqlitex.Execute(c, `UPDATE users SET balance = 100 WHERE id = 1`, nil)
	})
	if err != nil {
		t.Fatalf("no-op update on PK table should not error: %v", err)
	}
	if cs != nil {
		t.Errorf("no-op update should produce nil changeset, got %d bytes", len(cs))
	}
}

func TestCapture_PKless_NoOpUpdate_Conservative(t *testing.T) {
	// Edge case: no-op UPDATE on a PK-less table. total_changes advances,
	// changeset is empty, PK-less tables exist — so we DO error. This is
	// a false positive in the strictest sense (the change was a no-op so
	// nothing was lost), but it's the conservative choice: the user has
	// a schema problem that WILL bite them on a real change, so flag it.
	conn := openPKlessDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	_, err := capture(conn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE bad SET val = 'v1' WHERE name = 'k'`, nil) // val already 'v1'
	})

	var urc *ErrUnrecordedChanges
	if !errors.As(err, &urc) {
		t.Fatalf("expected ErrUnrecordedChanges (conservative), got %v", err)
	}
}

func TestFindPKlessTables(t *testing.T) {
	conn := openPKlessDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	got := findPKlessTables(conn)
	want := []string{"also_bad", "bad"} // alphabetical
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFindPKlessTables_AllGood(t *testing.T) {
	conn := openTestDB(t, filepath.Join(t.TempDir(), "db.sqlite"))
	defer conn.Close()

	got := findPKlessTables(conn)
	if len(got) != 0 {
		t.Errorf("got %v, want empty (test schema has PKs)", got)
	}
}

// --- Integration with Update ------------------------------------------------

func TestUpdate_PKlessTable_Error(t *testing.T) {
	// End-to-end: Update on a PK-less table should return
	// ErrUnrecordedChanges, and the local change should be rolled back
	// (SAVEPOINT handling).
	store := NewMemBlobStore()
	ctx := t.Context()

	// Seed a DB with a PK-less table via Init.
	srcPath := filepath.Join(t.TempDir(), "src.sqlite")
	conn, _ := sqlite.OpenConn(srcPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	sqlitex.ExecuteScript(conn, `
		CREATE TABLE good (id INTEGER PRIMARY KEY, val TEXT);
		CREATE TABLE bad (name TEXT, val TEXT);
		INSERT INTO bad (name, val) VALUES ('k', 'initial');
	`, nil)
	conn.Close()

	if err := Init(ctx, store, "mydb/", srcPath); err != nil {
		t.Fatalf("Init: %v", err)
	}

	db, err := Open(ctx, store, "mydb/")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Try to update the PK-less table.
	err = db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE bad SET val = 'changed' WHERE name = 'k'`, nil)
	})

	var urc *ErrUnrecordedChanges
	if !errors.As(err, &urc) {
		t.Fatalf("expected ErrUnrecordedChanges, got %v", err)
	}

	// The local change should have been rolled back (Update's SAVEPOINT
	// rolls back on error).
	var val string
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT val FROM bad WHERE name = 'k'`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { val = s.ColumnText(0); return nil },
		})
	})
	if val != "initial" {
		t.Errorf("val = %q, want 'initial' (failed Update should roll back)", val)
	}

	// Manifest should be unchanged (nothing committed).
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if len(m.Log) != 0 {
		t.Errorf("log len = %d, want 0 (nothing should have been committed)", len(m.Log))
	}
}