package s3db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

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
	ctx := context.Background()

	etag, err := s.Put(ctx, "k", []byte("hello"), NoCondition)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if etag == "" {
		t.Fatal("Put returned empty etag")
	}

	body, gotETag, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(body) != "hello" {
		t.Errorf("body = %q, want %q", body, "hello")
	}
	if gotETag != etag {
		t.Errorf("Get etag = %q, want %q", gotETag, etag)
	}
}

func TestMemBlobStore_HeadMatchesGet(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	putETag, _ := s.Put(ctx, "k", []byte("hello"), NoCondition)

	headETag, err := s.Head(ctx, "k")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if headETag != putETag {
		t.Errorf("Head etag = %q, want %q", headETag, putETag)
	}
}

func TestMemBlobStore_ETagIsContentHash(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "a", []byte("same content"), NoCondition)
	etag2, _ := s.Put(ctx, "b", []byte("same content"), NoCondition)
	etag3, _ := s.Put(ctx, "c", []byte("different"), NoCondition)

	if etag1 != etag2 {
		t.Errorf("same content should produce same etag: %q vs %q", etag1, etag2)
	}
	if etag1 == etag3 {
		t.Errorf("different content should produce different etag")
	}
}

func TestMemBlobStore_GetReturnsCopy(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("hello"), NoCondition)

	body1, _, _ := s.Get(ctx, "k")
	body1[0] = 'X' // mutate the returned slice

	body2, _, _ := s.Get(ctx, "k")
	if string(body2) != "hello" {
		t.Errorf("stored data was mutated: got %q", body2)
	}
}

func TestMemBlobStore_PutStoresCopy(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	buf := []byte("hello")
	s.Put(ctx, "k", buf, NoCondition)
	buf[0] = 'X' // mutate after Put

	body, _, _ := s.Get(ctx, "k")
	if string(body) != "hello" {
		t.Errorf("stored data reflects caller mutation: got %q", body)
	}
}

func TestMemBlobStore_PutOverwrite(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "k", []byte("v1"), NoCondition)
	etag2, _ := s.Put(ctx, "k", []byte("v2"), NoCondition)

	if etag1 == etag2 {
		t.Error("overwrite with different content should change etag")
	}

	body, _, _ := s.Get(ctx, "k")
	if string(body) != "v2" {
		t.Errorf("body = %q, want v2", body)
	}
}

func TestMemBlobStore_IfMatch_Success(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "k", []byte("v1"), NoCondition)

	etag2, err := s.Put(ctx, "k", []byte("v2"), PutCondition{IfMatch: etag1})
	if err != nil {
		t.Fatalf("CAS with correct etag failed: %v", err)
	}
	if etag2 == etag1 {
		t.Error("expected new etag after successful CAS")
	}

	body, _, _ := s.Get(ctx, "k")
	if string(body) != "v2" {
		t.Errorf("body = %q, want v2", body)
	}
}

func TestMemBlobStore_IfMatch_StaleETag(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "k", []byte("v1"), NoCondition)
	s.Put(ctx, "k", []byte("v2"), NoCondition) // concurrent writer

	_, err := s.Put(ctx, "k", []byte("v3"), PutCondition{IfMatch: etag1})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	body, _, _ := s.Get(ctx, "k")
	if string(body) != "v2" {
		t.Errorf("failed CAS should not have modified data: body = %q", body)
	}
}

func TestMemBlobStore_IfMatch_Nonexistent(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	_, err := s.Put(ctx, "k", []byte("v"), PutCondition{IfMatch: "deadbeef"})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed for If-Match on missing key, got %v", err)
	}
}

func TestMemBlobStore_IfMatch_QuotedETag(t *testing.T) {
	// S3 returns ETags wrapped in quotes; we should accept either form.
	s := NewMemBlobStore()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "k", []byte("v1"), NoCondition)
	quoted := `"` + etag1 + `"`

	_, err := s.Put(ctx, "k", []byte("v2"), PutCondition{IfMatch: quoted})
	if err != nil {
		t.Fatalf("CAS with quoted etag failed: %v", err)
	}
}

func TestMemBlobStore_IfNoneMatch_Success(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	etag, err := s.Put(ctx, "k", []byte("v1"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("IfNoneMatch on empty key failed: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
}

func TestMemBlobStore_IfNoneMatch_Exists(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("v1"), NoCondition)

	_, err := s.Put(ctx, "k", []byte("v2"), PutCondition{IfNoneMatch: true})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}

	body, _, _ := s.Get(ctx, "k")
	if string(body) != "v1" {
		t.Errorf("failed IfNoneMatch should not have modified data: body = %q", body)
	}
}

func TestMemBlobStore_List(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	s.Put(ctx, "a/1", []byte("x"), NoCondition)
	s.Put(ctx, "a/2", []byte("x"), NoCondition)
	s.Put(ctx, "b/1", []byte("x"), NoCondition)
	s.Put(ctx, "a/sub/3", []byte("x"), NoCondition)

	keys, err := s.List(ctx, "a/")
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
	ctx := context.Background()

	s.Put(ctx, "z", []byte("x"), NoCondition)
	s.Put(ctx, "a", []byte("x"), NoCondition)
	s.Put(ctx, "m", []byte("x"), NoCondition)

	keys, _ := s.List(ctx, "")
	want := []string{"a", "m", "z"}
	if !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want sorted %v", keys, want)
	}
}

func TestMemBlobStore_Delete(t *testing.T) {
	s := NewMemBlobStore()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("v"), NoCondition)

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
	ctx := context.Background()

	s.Put(ctx, "epoch-1/a", []byte("x"), NoCondition)
	s.Put(ctx, "epoch-1/b", []byte("x"), NoCondition)
	s.Put(ctx, "epoch-2/a", []byte("x"), NoCondition)

	if err := s.DeletePrefix(ctx, "epoch-1/"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	keys, _ := s.List(ctx, "")
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
	if _, err := s.Put(ctx, "k", nil, NoCondition); !errors.Is(err, context.Canceled) {
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
	s.Put(ctx, key, []byte{0}, NoCondition)

	const workers = 50
	const incrementsPerWorker = 20

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerWorker; j++ {
				for {
					body, etag, err := s.Get(ctx, key)
					if err != nil {
						t.Error(err)
						return
					}
					newBody := []byte{body[0] + 1}
					_, err = s.Put(ctx, key, newBody, PutCondition{IfMatch: etag})
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

	body, _, _ := s.Get(ctx, key)
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
			_, err := s.Put(ctx, "claimed", []byte(fmt.Sprintf("worker-%d", id)), PutCondition{IfNoneMatch: true})
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
