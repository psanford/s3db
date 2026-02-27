package s3db

import (
	"bytes"
	"fmt"

	"zombiezen.com/go/sqlite"
)

// conflictPolicy determines how applyChangeset responds when a changeset
// cannot be applied cleanly because the target database has diverged from
// the changeset's before-image.
type conflictPolicy int

const (
	// conflictAbort rolls back the entire changeset on any conflict.
	// This is the only generally-correct policy — it gives you serializable
	// isolation by falling back to full transaction re-execution.
	//
	// SQLite's ChangesetAbort action handles the rollback automatically;
	// no manual SAVEPOINT management needed.
	conflictAbort conflictPolicy = iota
)

// errChangesetConflict is returned by applyChangeset when the conflict
// handler aborted. Wrapped with the specific conflict type for diagnostics.
// This is a package-internal signal — the commit loop catches it and decides
// whether to rebase or re-execute.
type errChangesetConflict struct {
	kind sqlite.ConflictType
}

func (e *errChangesetConflict) Error() string {
	return fmt.Sprintf("changeset conflict: %s", e.kind)
}

// applyChangeset applies a changeset blob to conn. If the changeset cannot
// be applied cleanly (a row's before-image does not match, a primary key
// collides, etc.), the behavior depends on policy.
//
// Under conflictAbort, the entire changeset is rolled back by SQLite and an
// *errChangesetConflict is returned. No partial application is possible.
//
// An empty changeset is a no-op and returns nil.
func applyChangeset(conn *sqlite.Conn, cs []byte, policy conflictPolicy) error {
	if len(cs) == 0 {
		return nil
	}

	// The conflict handler runs inside SQLite's C code while iterating.
	// We can't return a Go error directly, so we stash the conflict type
	// and translate to an error after ApplyChangeset returns.
	var seen *errChangesetConflict

	handler := func(kind sqlite.ConflictType, _ *sqlite.ChangesetIterator) sqlite.ConflictAction {
		switch policy {
		case conflictAbort:
			seen = &errChangesetConflict{kind: kind}
			return sqlite.ChangesetAbort
		default:
			seen = &errChangesetConflict{kind: kind}
			return sqlite.ChangesetAbort
		}
	}

	err := conn.ApplyChangeset(bytes.NewReader(cs), nil, handler)
	if seen != nil {
		// ChangesetAbort causes ApplyChangeset to return ResultAbort.
		// Our stashed conflict error is the more useful signal upstream.
		return seen
	}
	return err
}
