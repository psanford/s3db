package s3db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// faultyStore wraps a BlobStore and injects random failures. The failure
// rate is controlled by prob (0.0–1.0). Failures are transient — retrying
// the same operation may succeed. This simulates flaky network / S3 5xx.
type faultyStore struct {
	inner BlobStore
	prob  float64
	rng   *rand.Rand
	mu    sync.Mutex
}

var errInjected = errors.New("injected failure")

func newFaultyStore(inner BlobStore, prob float64, seed int64) *faultyStore {
	return &faultyStore{
		inner: inner,
		prob:  prob,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

func (f *faultyStore) maybeFail() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rng.Float64() < f.prob {
		return errInjected
	}
	return nil
}

func (f *faultyStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := f.maybeFail(); err != nil {
		return nil, "", err
	}
	return f.inner.Get(ctx, key)
}

func (f *faultyStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return f.inner.GetRange(ctx, key, start, end)
}

func (f *faultyStore) Stat(ctx context.Context, key string) (BlobInfo, error) {
	if err := f.maybeFail(); err != nil {
		return BlobInfo{}, err
	}
	return f.inner.Stat(ctx, key)
}

func (f *faultyStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if err := f.maybeFail(); err != nil {
		return "", err
	}
	return f.inner.Put(ctx, key, body, cond)
}

func (f *faultyStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return f.inner.List(ctx, prefix)
}

func (f *faultyStore) Delete(ctx context.Context, key string) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.Delete(ctx, key)
}

func (f *faultyStore) DeletePrefix(ctx context.Context, prefix string) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.DeletePrefix(ctx, prefix)
}

var _ BlobStore = (*faultyStore)(nil)

// TestChaos_NoCorruption runs concurrent writers against a faulty store
// and verifies the final state is consistent (correct counter value) and
// that the log is replayable from scratch without errors.
//
// This is a soak test: it doesn't assert the exact number of successful
// writes (some will fail with injected errors and not be retried by the
// test harness) but it does assert that every successful write is durable
// and every unsuccessful write is fully rolled back.
func TestChaos_NoCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	ctx := context.Background()
	inner := NewMemBlobStore()

	// Seed schema + counter via a clean (non-faulty) store.
	seedSchema(t, inner, "mydb/")
	seedDB, _ := Open(ctx, inner, "mydb/")
	seedDB.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'counter', 0)`, nil)
	})
	seedDB.Close()

	// 15% failure rate. Deterministic seed for reproducibility.
	faulty := newFaultyStore(inner, 0.15, 42)

	const workers = 6
	const attemptsPerWorker = 15

	var totalSuccesses int64
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each worker retries the WHOLE Open+Update on failure —
			// simulating a Lambda retry policy at the invocation level.
			for j := 0; j < attemptsPerWorker; j++ {
				if err := attemptIncrement(ctx, faulty, "mydb/"); err != nil {
					// Injected failure or conflict — not counted, but
					// also must not corrupt state.
					continue
				}
				mu.Lock()
				totalSuccesses++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	// Verify via a fresh Open on the CLEAN store (no injected failures
	// during verification).
	verifier, err := Open(ctx, inner, "mydb/")
	if err != nil {
		t.Fatalf("Open verifier: %v", err)
	}
	defer verifier.Close()

	var counter int64
	verifier.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT balance FROM users WHERE id = 1`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { counter = s.ColumnInt64(0); return nil },
		})
	})

	t.Logf("successes = %d, counter = %d", totalSuccesses, counter)

	// The core invariant: counter == number of successful Updates.
	// Every success durably incremented; every failure was fully rolled back.
	if counter != totalSuccesses {
		t.Errorf("counter = %d, successes = %d — state diverged from successful writes",
			counter, totalSuccesses)
	}

	// Secondary check: the log must be replayable from scratch with no
	// conflicts. This catches any "committed a changeset that doesn't
	// match its predecessor's state" bugs.
	m, _, _ := loadManifest(ctx, inner, "mydb/manifest.json")
	replayPath := t.TempDir() + "/replay.sqlite"
	if err := downloadSnapshot(ctx, inner, m.Snapshot.Key, 0, replayPath); err != nil {
		t.Fatalf("replay download: %v", err)
	}
	conn, _ := sqlite.OpenConn(replayPath, sqlite.OpenReadWrite)
	defer conn.Close()
	if _, err := applyLog(ctx, inner, conn, m.Log, m.Snapshot.Seq); err != nil {
		t.Fatalf("replay failed — log is inconsistent: %v", err)
	}

	var replayCounter int64
	sqlitex.Execute(conn, `SELECT balance FROM users WHERE id = 1`, &sqlitex.ExecOptions{
		ResultFunc: func(s *sqlite.Stmt) error { replayCounter = s.ColumnInt64(0); return nil },
	})
	if replayCounter != counter {
		t.Errorf("replay counter = %d, live counter = %d — log replay diverged", replayCounter, counter)
	}
}

// attemptIncrement is one full Open → Update → Close cycle. Returns an error
// if any step fails (injected or real).
func attemptIncrement(ctx context.Context, store BlobStore, prefix string) error {
	db, err := Open(ctx, store, prefix, WithMaxRetries(20))
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `UPDATE users SET balance = balance + 1 WHERE id = 1`, nil)
	})
}

// TestChaos_OrphansGetCleaned verifies that blobs uploaded by failed
// writes (crash between blob-PUT and manifest-CAS) are eventually cleaned
// by GC after compaction.
func TestChaos_OrphansGetCleaned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	ctx := context.Background()
	inner := NewMemBlobStore()
	seedSchema(t, inner, "mydb/")

	// Seed counter.
	seedDB, _ := Open(ctx, inner, "mydb/")
	seedDB.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name, balance) VALUES (1, 'c', 0)`, nil)
	})
	seedDB.Close()

	// Use a crashingStore that fails Put to manifest.json half the time
	// AFTER the changeset blob has been successfully uploaded. This
	// specifically targets the orphan-blob window.
	crashy := &crashingStore{
		MemBlobStore: inner,
		manifestKey:  "mydb/manifest.json",
		failRate:     0.5,
		rng:          rand.New(rand.NewSource(7)),
	}

	// Run a bunch of writes through the crashy store.
	for i := 0; i < 20; i++ {
		db, err := Open(ctx, crashy, "mydb/", WithMaxRetries(5))
		if err != nil {
			continue
		}
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, `UPDATE users SET balance = balance + 1 WHERE id = 1`, nil)
		})
		db.Close()
	}

	// Count orphans: changeset blobs in the store not referenced by
	// the manifest.
	m, _, _ := loadManifest(ctx, inner, "mydb/manifest.json")
	liveCS := make(map[string]struct{})
	for _, e := range m.Log {
		liveCS[e.Key] = struct{}{}
	}
	allCS, _ := inner.List(ctx, "mydb/changesets/")

	orphansBefore := 0
	for _, k := range allCS {
		if _, ok := liveCS[k]; !ok {
			orphansBefore++
		}
	}
	t.Logf("orphans before compact+GC: %d (total blobs: %d)", orphansBefore, len(allCS))

	if orphansBefore == 0 {
		t.Skip("no orphans produced (unlucky); rerun with different seed")
	}

	// Compact and GC through the CLEAN store (no injected failures).
	cleanDB, _ := Open(ctx, inner, "mydb/", WithGCGracePeriod(0))
	if err := cleanDB.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := cleanDB.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}
	cleanDB.Close()

	// Orphans should be gone.
	allCSAfter, _ := inner.List(ctx, "mydb/changesets/")
	t.Logf("changeset blobs after compact+GC: %d", len(allCSAfter))
	if len(allCSAfter) != 0 {
		t.Errorf("%d changeset blobs remain after compact+GC, want 0: %v",
			len(allCSAfter), allCSAfter)
	}
}

// crashingStore fails manifest Puts at the specified rate, simulating
// crashes between blob upload and manifest commit.
type crashingStore struct {
	*MemBlobStore
	manifestKey string
	failRate    float64
	rng         *rand.Rand
	mu          sync.Mutex
}

func (s *crashingStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if key == s.manifestKey {
		s.mu.Lock()
		fail := s.rng.Float64() < s.failRate
		s.mu.Unlock()
		if fail {
			return "", fmt.Errorf("simulated crash before manifest commit")
		}
	}
	return s.MemBlobStore.Put(ctx, key, body, cond)
}
