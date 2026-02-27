// Package s3db provides a concurrency-safe SQLite database backed by S3,
// using changeset logging and optimistic concurrency control. See DESIGN.md
// for the architecture.
package s3db

import (
	"context"
	"io"
	"time"
)

// BlobStore is the storage abstraction. Production uses S3; tests use an
// in-memory fake. All operations are key-scoped — the store has no concept
// of the manifest, changesets, or snapshots.
//
// Implementations must provide:
//   - Atomic Put (readers never see partial writes)
//   - Strong read-after-write consistency
//   - ETag-based compare-and-swap via PutCondition.IfMatch
//   - Write-if-not-exists via PutCondition.IfNoneMatch
type BlobStore interface {
	// Get retrieves the object at key. The caller is responsible for
	// closing the returned reader. Returns ErrNotFound if the key does
	// not exist.
	Get(ctx context.Context, key string) (body io.ReadCloser, etag string, err error)

	// GetRange retrieves bytes [start, end] inclusive of the object at key.
	// The caller is responsible for closing the returned reader. Used for
	// parallel snapshot downloads. Stores that don't support range requests
	// may return the full object (the caller reads what it needs and
	// discards the rest, which is wasteful but correct). Returns ErrNotFound
	// if the key does not exist.
	GetRange(ctx context.Context, key string, start, end int64) (body io.ReadCloser, err error)

	// Stat returns metadata for the object at key without fetching the
	// body. Returns ErrNotFound if the key does not exist.
	Stat(ctx context.Context, key string) (info BlobInfo, err error)

	// Put writes body to key, subject to cond. The body reader is drained
	// by the store. Returns the new ETag on success, ErrPreconditionFailed
	// if cond is not met.
	//
	// The body may be partially or fully consumed before an error is
	// returned. Callers that need to retry must supply a fresh reader.
	Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (etag string, err error)

	// List returns all keys with the given prefix, in lexicographic order.
	List(ctx context.Context, prefix string) (keys []string, err error)

	// Delete removes the object at key. Deleting a nonexistent key is not an error.
	Delete(ctx context.Context, key string) error

	// DeletePrefix removes all objects whose keys start with prefix.
	// Deleting an empty prefix (no matches) is not an error.
	DeletePrefix(ctx context.Context, prefix string) error
}

// BlobInfo describes an object in the store.
type BlobInfo struct {
	ETag         string
	Size         int64
	LastModified time.Time
}

// PutCondition specifies a precondition for Put. Only one of IfMatch or
// IfNoneMatch may be set; setting both is an error.
type PutCondition struct {
	// IfMatch, if non-empty, requires the current ETag of the object to equal
	// this value. Use this for compare-and-swap.
	IfMatch string

	// IfNoneMatch, if true, requires the object to not exist. Use this for
	// write-if-not-exists (e.g. claiming a unique sequence number).
	IfNoneMatch bool
}

// NoCondition is a PutCondition with no preconditions — an unconditional write.
var NoCondition = PutCondition{}
