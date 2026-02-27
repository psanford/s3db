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
