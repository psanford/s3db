package s3db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
)

// Test helpers — keep the stream boilerplate out of individual test bodies.

func putString(t *testing.T, s BlobStore, key, body string, cond PutCondition) string {
	t.Helper()
	etag, err := s.Put(context.Background(), key, strings.NewReader(body), cond)
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return etag
}

func getString(t *testing.T, s BlobStore, key string) (string, string) {
	t.Helper()
	rc, etag, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	return string(body), etag
}

func TestMemBlobStore_GetNotFound(t *testing.T) {
	s := NewMemBlobStore()
	_, _, err := s.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemBlobStore_HeadNotFound(t *testing.T) {
	s := NewMemBlobStore()
	_, err := s.Head(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemBlobStore_PutGet(t *testing.T) {
	s := NewMemBlobStore()

	etag := putString(t, s, "k", "hello", NoCondition)
	if etag == "" {
		t.Fatal("Put returned empty etag")
	}

	body, gotETag := getString(t, s, "k")
	if body != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
	if gotETag != etag {
		t.Errorf("Get etag = %q, want %q", gotETag, etag)
	}
}

func TestMemBlobStore_HeadMatchesGet(t *testing.T) {
	s := NewMemBlobStore()

	putETag := putString(t, s, "k", "hello", NoCondition)

	headETag, err := s.Head(context.Background(), "k")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if headETag != putETag {
		t.Errorf("Head etag = %q, want %q", headETag, putETag)
	}
}

func TestMemBlobStore_ETagIsContentHash(t *testing.T) {
	s := NewMemBlobStore()

	etag1 := putString(t, s, "a", "same content", NoCondition)
	etag2 := putString(t, s, "b", "same content", NoCondition)
	etag3 := putString(t, s, "c", "different", NoCondition)

	if etag1 != etag2 {
		t.Errorf("same content should produce same etag: %q vs %q", etag1, etag2)
	}
	if etag1 == etag3 {
		t.Errorf("different content should produce different etag")
	}
}

func TestMemBlobStore_GetIsolated(t *testing.T) {
	// Two Gets of the same key return independent readers; draining one
	// must not affect the other.
	s := NewMemBlobStore()
	putString(t, s, "k", "hello", NoCondition)

	rc1, _, _ := s.Get(context.Background(), "k")
	rc2, _, _ := s.Get(context.Background(), "k")
	defer rc1.Close()
	defer rc2.Close()

	io.ReadAll(rc1) // drain first

	body2, _ := io.ReadAll(rc2)
	if string(body2) != "hello" {
		t.Errorf("second reader affected by first: got %q", body2)
	}
}

func TestMemBlobStore_PutIsolated(t *testing.T) {
	// Mutating the caller's source buffer after Put must not affect
	// what's stored.
	s := NewMemBlobStore()

	buf := []byte("hello")
	s.Put(context.Background(), "k", bytes.NewReader(buf), NoCondition)
	buf[0] = 'X'

	body, _ := getString(t, s, "k")
	if body != "hello" {
		t.Errorf("stored data reflects caller mutation: got %q", body)
	}
}

func TestMemBlobStore_PutReaderError(t *testing.T) {
	s := NewMemBlobStore()

	wantErr := errors.New("boom")
	r := iotest.ErrReader(wantErr)

	_, err := s.Put(context.Background(), "k", r, NoCondition)
	if !errors.Is(err, wantErr) {
		t.Errorf("expected reader error to propagate, got %v", err)
	}

	// Nothing should have been stored.
	if _, _, err := s.Get(context.Background(), "k"); !errors.Is(err, ErrNotFound) {
		t.Error("partial write was stored despite reader error")
	}
}

func TestMemBlobStore_PutOverwrite(t *testing.T) {
	s := NewMemBlobStore()

	etag1 := putString(t, s, "k", "v1", NoCondition)
	etag2 := putString(t, s, "k", "v2", NoCondition)

	if etag1 == etag2 {
		t.Error("overwrite with different content should change etag")
	}

	body, _ := getString(t, s, "k")
	if body != "v2" {
		t.Errorf("body = %q, want v2", body)
	}
}

func TestMemBlobStore_IfMatch_Success(t *testing.T) {
	s := NewMemBlobStore()

	etag1 := putString(t, s, "k", "v1", NoCondition)
	etag2 := putString(t, s, "k", "v2", PutCondition{IfMatch: etag1})

	if etag2 == etag1 {
		t.Error("expected new etag after successful CAS")
	}

	body, _ := getString(t, s, "k")
	if body != "v2" {
		t.Errorf("body = %q, want v2", body)
	}
}

func TestMemBlobStore_IfMatch_StaleETag(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1 := putString(t, s, "k", "v1", NoCondition)
	putString(t, s, "k", "v2", NoCondition) // concurrent writer

	_, err := s.Put(ctx, "k", strings.NewReader("v3"), PutCondition{IfMatch: etag1})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	body, _ := getString(t, s, "k")
	if body != "v2" {
		t.Errorf("failed CAS should not have modified data: body = %q", body)
	}
}

func TestMemBlobStore_IfMatch_Nonexistent(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	_, err := s.Put(ctx, "k", strings.NewReader("v"), PutCondition{IfMatch: "deadbeef"})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed for If-Match on missing key, got %v", err)
	}
}

func TestMemBlobStore_IfMatch_QuotedETag(t *testing.T) {
	// S3 returns ETags wrapped in quotes; we should accept either form.
	s := NewMemBlobStore()

	etag1 := putString(t, s, "k", "v1", NoCondition)
	quoted := `"` + etag1 + `"`

	putString(t, s, "k", "v2", PutCondition{IfMatch: quoted})
}

func TestMemBlobStore_IfNoneMatch_Success(t *testing.T) {
	s := NewMemBlobStore()

	etag := putString(t, s, "k", "v1", PutCondition{IfNoneMatch: true})
	if etag == "" {
		t.Error("expected non-empty etag")
	}
}

func TestMemBlobStore_IfNoneMatch_Exists(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	putString(t, s, "k", "v1", NoCondition)

	_, err := s.Put(ctx, "k", strings.NewReader("v2"), PutCondition{IfNoneMatch: true})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	body, _ := getString(t, s, "k")
	if body != "v1" {
		t.Errorf("failed IfNoneMatch should not have modified data: body = %q", body)
	}
}

func TestMemBlobStore_List(t *testing.T) {
	s := NewMemBlobStore()

	putString(t, s, "a/1", "x", NoCondition)
	putString(t, s, "a/2", "x", NoCondition)
	putString(t, s, "b/1", "x", NoCondition)
	putString(t, s, "a/sub/3", "x", NoCondition)

	keys, err := s.List(context.Background(), "a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{"a/1", "a/2", "a/sub/3"}
	if !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
}

func TestMemBlobStore_ListEmpty(t *testing.T) {
	s := NewMemBlobStore()
	keys, err := s.List(context.Background(), "nothing/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected empty result, got %v", keys)
	}
}

func TestMemBlobStore_ListAll(t *testing.T) {
	s := NewMemBlobStore()

	putString(t, s, "z", "x", NoCondition)
	putString(t, s, "a", "x", NoCondition)
	putString(t, s, "m", "x", NoCondition)

	keys, _ := s.List(context.Background(), "")
	want := []string{"a", "m", "z"}
	if !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want sorted %v", keys, want)
	}
}

func TestMemBlobStore_Delete(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	putString(t, s, "k", "v", NoCondition)

	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, _, err := s.Get(ctx, "k")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestMemBlobStore_DeleteNonexistent(t *testing.T) {
	s := NewMemBlobStore()
	if err := s.Delete(context.Background(), "missing"); err != nil {
		t.Errorf("Delete of nonexistent key should succeed, got %v", err)
	}
}

func TestMemBlobStore_DeletePrefix(t *testing.T) {
	s := NewMemBlobStore()

	putString(t, s, "epoch-1/a", "x", NoCondition)
	putString(t, s, "epoch-1/b", "x", NoCondition)
	putString(t, s, "epoch-2/a", "x", NoCondition)

	if err := s.DeletePrefix(context.Background(), "epoch-1/"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	keys, _ := s.List(context.Background(), "")
	want := []string{"epoch-2/a"}
	if !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
}

func TestMemBlobStore_DeletePrefixEmpty(t *testing.T) {
	s := NewMemBlobStore()
	if err := s.DeletePrefix(context.Background(), "nothing/"); err != nil {
		t.Errorf("DeletePrefix of empty prefix should succeed, got %v", err)
	}
}

func TestMemBlobStore_ContextCancel(t *testing.T) {
	s := NewMemBlobStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := s.Get(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get: expected context.Canceled, got %v", err)
	}
	if _, err := s.Head(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Head: expected context.Canceled, got %v", err)
	}
	if _, err := s.Put(ctx, "k", strings.NewReader(""), NoCondition); !errors.Is(err, context.Canceled) {
		t.Errorf("Put: expected context.Canceled, got %v", err)
	}
	if _, err := s.List(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("List: expected context.Canceled, got %v", err)
	}
	if err := s.Delete(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete: expected context.Canceled, got %v", err)
	}
	if err := s.DeletePrefix(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("DeletePrefix: expected context.Canceled, got %v", err)
	}
}

// TestMemBlobStore_ConcurrentCAS verifies that CAS is actually atomic under
// heavy concurrent contention. N goroutines all try to increment a counter
// stored as a single byte; exactly N increments should succeed in total
// (with retries), and the final value must equal N.
func TestMemBlobStore_ConcurrentCAS(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()
	const key = "counter"

	// Initialize counter to 0.
	s.Put(ctx, key, bytes.NewReader([]byte{0}), NoCondition)

	const workers = 50
	const incrementsPerWorker = 20

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				for {
					rc, etag, err := s.Get(ctx, key)
					if err != nil {
						t.Error(err)
						return
					}
					body, _ := io.ReadAll(rc)
					rc.Close()

					newBody := []byte{body[0] + 1}
					_, err = s.Put(ctx, key, bytes.NewReader(newBody), PutCondition{IfMatch: etag})
					if err == nil {
						break // success
					}
					if !errors.Is(err, ErrPreconditionFailed) {
						t.Error(err)
						return
					}
					// retry
				}
			}
		}()
	}
	wg.Wait()

	rc, _, _ := s.Get(ctx, key)
	body, _ := io.ReadAll(rc)
	rc.Close()
	want := byte(workers * incrementsPerWorker % 256)
	if body[0] != want {
		t.Errorf("final counter = %d, want %d", body[0], want)
	}
}

// TestMemBlobStore_ConcurrentIfNoneMatch verifies that when N goroutines
// race to claim a unique key with IfNoneMatch, exactly one succeeds.
func TestMemBlobStore_ConcurrentIfNoneMatch(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	const workers = 100
	var successes atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			body := strings.NewReader(fmt.Sprintf("worker-%d", id))
			_, err := s.Put(ctx, "claimed", body, PutCondition{IfNoneMatch: true})
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrPreconditionFailed) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	if n := successes.Load(); n != 1 {
		t.Errorf("expected exactly 1 success, got %d", n)
	}
}
