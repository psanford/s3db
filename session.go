package s3db

import (
	"bytes"
	"fmt"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// capture runs fn against conn while recording row-level changes, and returns
// the resulting changeset blob.
//
// The session is attached to all tables in the main database. Any INSERT,
// UPDATE, or DELETE to a table with an explicit PRIMARY KEY is recorded.
// DDL (CREATE/ALTER/DROP) is not recorded — see DESIGN.md for how migrations
// handle schema changes via forced compaction.
//
// If fn returns an error, that error is returned and the changeset is nil.
// The caller is responsible for rolling back fn's effects on conn if desired;
// capture does not wrap fn in a SAVEPOINT. In the commit loop, the caller
// holds a SAVEPOINT around the entire capture→CAS→rebase cycle so that fn's
// changes can be rolled back on conflict.
//
// An empty changeset (fn did only SELECTs, or no-op UPDATEs) is returned as
// a nil slice.
//
// If fn modifies rows but the changeset is empty, capture checks for tables
// without a PRIMARY KEY and returns an error naming them. SQLite's session
// extension silently skips PK-less tables, which would otherwise cause
// silent data loss (local file changed, remote never updated).
func capture(conn *sqlite.Conn, fn func(*sqlite.Conn) error) ([]byte, error) {
	sess, err := conn.CreateSession("")
	if err != nil {
		return nil, fmt.Errorf("capture: create session: %w", err)
	}
	defer sess.Delete()

	// Attach to all tables. The empty string means "all tables in the
	// database", per SQLite's sqlite3session_attach semantics.
	if err := sess.Attach(""); err != nil {
		return nil, fmt.Errorf("capture: attach: %w", err)
	}

	// Track total_changes before/after. SQLite's total_changes() is a
	// monotone counter of rows affected by INSERT/UPDATE/DELETE on this
	// connection. If it advances but the changeset is empty, rows were
	// modified in a way the session couldn't record — almost always a
	// PK-less table.
	changesBefore := totalChanges(conn)

	if err := fn(conn); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := sess.WriteChangeset(&buf); err != nil {
		return nil, fmt.Errorf("capture: write changeset: %w", err)
	}

	if buf.Len() == 0 {
		if delta := totalChanges(conn) - changesBefore; delta > 0 {
			// Rows changed but nothing was captured. Check for the
			// usual culprit: tables without a PRIMARY KEY, which the
			// session extension silently skips.
			if pkless := findPKlessTables(conn); len(pkless) > 0 {
				return nil, &ErrUnrecordedChanges{
					Rows:         delta,
					PKlessTables: pkless,
				}
			}
			// No PK-less tables — probably no-op updates (UPDATE x
			// SET y = y). The session correctly elides these. Fall
			// through to "nothing to commit".
		}
		return nil, nil
	}
	return buf.Bytes(), nil
}

// ErrUnrecordedChanges is returned when fn modified rows that were not
// captured in the changeset. This is almost always because the modified
// table lacks a PRIMARY KEY — SQLite's session extension requires one to
// track changes, and silently skips tables without.
//
// Fix: add a PRIMARY KEY to the affected table. If the table has no
// natural key, use "id INTEGER PRIMARY KEY" (which aliases the built-in
// rowid and costs nothing).
type ErrUnrecordedChanges struct {
	Rows         int64    // how many rows were modified but not captured
	PKlessTables []string // tables in the schema without a PRIMARY KEY
}

func (e *ErrUnrecordedChanges) Error() string {
	return fmt.Sprintf("s3db: %d row(s) modified but not captured — "+
		"SQLite sessions skip tables without a PRIMARY KEY. "+
		"Tables missing a PK: %s. "+
		"Fix: ALTER TABLE or add 'id INTEGER PRIMARY KEY'.",
		e.Rows, strings.Join(e.PKlessTables, ", "))
}

// totalChanges returns SQLite's total_changes() counter — cumulative rows
// modified by INSERT/UPDATE/DELETE on this connection since open. Used to
// detect whether fn touched any rows.
func totalChanges(conn *sqlite.Conn) int64 {
	var n int64
	// Can't fail for this trivial query; if it somehow does, 0 is a
	// safe answer (we'll just miss the PK-less detection, not corrupt).
	sqlitex.Execute(conn, "SELECT total_changes()", &sqlitex.ExecOptions{
		ResultFunc: func(s *sqlite.Stmt) error { n = s.ColumnInt64(0); return nil },
	})
	return n
}

// findPKlessTables returns the names of user tables (excluding sqlite_*
// internal tables) that have no PRIMARY KEY. The session extension cannot
// record changes to such tables.
//
// A table has a PK iff pragma_table_info reports at least one column with
// pk > 0. WITHOUT ROWID tables must have an explicit PK by definition, so
// they're never in this list.
func findPKlessTables(conn *sqlite.Conn) []string {
	var tables []string
	sqlitex.Execute(conn,
		`SELECT name FROM sqlite_master
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		   AND name NOT IN (
		     SELECT DISTINCT m.name
		     FROM sqlite_master m
		     JOIN pragma_table_info(m.name) p
		     WHERE m.type = 'table' AND p.pk > 0
		   )
		 ORDER BY name`,
		&sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error {
				tables = append(tables, s.ColumnText(0))
				return nil
			},
		})
	return tables
}
