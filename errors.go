package s3db

import "errors"

// Sentinel errors returned by the library.
var (
	// ErrNotFound is returned when a blob or manifest does not exist.
	ErrNotFound = errors.New("s3db: not found")

	// ErrPreconditionFailed is returned when a conditional write fails
	// because the precondition (If-Match or If-None-Match) was not met.
	// This indicates a concurrent writer modified the object.
	ErrPreconditionFailed = errors.New("s3db: precondition failed")

	// ErrConflict is returned when Update gives up after exhausting retries
	// due to repeated write contention.
	ErrConflict = errors.New("s3db: write conflict, retries exhausted")

	// ErrSchemaMismatch is returned when the caller's expected schema version
	// does not match the manifest's schema version.
	ErrSchemaMismatch = errors.New("s3db: schema version mismatch")

	// ErrSchemaTooNew is returned when the manifest's schema version is ahead
	// of the migrations this client knows about. Upgrade your code.
	ErrSchemaTooNew = errors.New("s3db: database schema is newer than this client supports")

	// errBothConditions is returned when PutCondition has both IfMatch
	// and IfNoneMatch set.
	errBothConditions = errors.New("s3db: PutCondition: both IfMatch and IfNoneMatch set")
)
