package s3db

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/psanford/s3db/internal/etag"
)

// memEntry holds the stored body and its computed ETag. Storing the ETag
// alongside the body (rather than recomputing on every access) mirrors S3's
// behavior and makes CAS checks cheap.
type memEntry struct {
	body []byte
	etag string
}

// MemBlobStore is an in-memory BlobStore with real ETag-based CAS semantics.
// It is safe for concurrent use. It exists for testing and is not intended
// for production.
type MemBlobStore struct {
	mu      sync.RWMutex
	objects map[string]memEntry
}

// NewMemBlobStore returns an empty in-memory blob store.
func NewMemBlobStore() *MemBlobStore {
	return &MemBlobStore{
		objects: make(map[string]memEntry),
	}
}

func (m *MemBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	// bytes.Reader never mutates its backing slice, so the stored body is
	// safe from callers. No explicit copy needed.
	return io.NopCloser(bytes.NewReader(e.body)), e.etag, nil
}

func (m *MemBlobStore) Head(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.objects[key]
	if !ok {
		return "", ErrNotFound
	}
	return e.etag, nil
}

func (m *MemBlobStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// Drain the reader before taking the lock. This mirrors S3 behavior
	// (upload happens before precondition evaluation on the server) and
	// avoids holding the lock during potentially-slow reads.
	stored, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]

	if cond.IfNoneMatch && exists {
		return "", ErrPreconditionFailed
	}
	if cond.IfMatch != "" {
		if !exists {
			// S3 returns 412 for If-Match against a nonexistent object.
			return "", ErrPreconditionFailed
		}
		if existing.etag != etag.Normalize(cond.IfMatch) {
			return "", ErrPreconditionFailed
		}
	}

	newETag := etag.Compute(stored)
	m.objects[key] = memEntry{body: stored, etag: newETag}
	return newETag, nil
}

func (m *MemBlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MemBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, key)
	return nil
}

func (m *MemBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			delete(m.objects, k)
		}
	}
	return nil
}

// Compile-time check that MemBlobStore satisfies the interface.
var _ BlobStore = (*MemBlobStore)(nil)
