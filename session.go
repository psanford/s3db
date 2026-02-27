package s3db

import (
	"bytes"
	"fmt"

	"zombiezen.com/go/sqlite"
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

	if err := fn(conn); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := sess.WriteChangeset(&buf); err != nil {
		return nil, fmt.Errorf("capture: write changeset: %w", err)
	}

	if buf.Len() == 0 {
		return nil, nil
	}
	return buf.Bytes(), nil
}
