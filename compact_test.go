package s3db

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// --- Compact ---------------------------------------------------------------

func TestCompact_Basic(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Make 5 writes.
	for i := 1; i <= 5; i++ {
		err := db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'u%d')`, i, i), nil)
		})
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
	}

	m1, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if len(m1.Log) != 5 {
		t.Fatalf("pre-compact log len = %d, want 5", len(m1.Log))
	}
	oldSnap := m1.Snapshot.Key

	// Compact.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	m2, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m2.Seq != m1.Seq {
		t.Errorf("compaction changed seq: %d → %d", m1.Seq, m2.Seq)
	}
	if len(m2.Log) != 0 {
		t.Errorf("post-compact log len = %d, want 0", len(m2.Log))
	}
	if m2.Snapshot.Key == oldSnap {
		t.Error("snapshot key unchanged after compaction")
	}
	if m2.Snapshot.Seq != m1.Seq {
		t.Errorf("snapshot.seq = %d, want %d", m2.Snapshot.Seq, m1.Seq)
	}

	// Data should be intact. Verify via a fresh Open + View.
	fresh, _ := Open(ctx, store, "mydb/")
	defer fresh.Close()
	var count int64
	fresh.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if count != 5 {
		t.Errorf("row count after compact = %d, want 5", count)
	}
}

func TestCompact_EmptyLog(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	m1, _, _ := loadManifest(ctx, store, "mydb/manifest.json")

	// Compact with nothing in the log should be a no-op.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	m2, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m2.Snapshot.Key != m1.Snapshot.Key {
		t.Error("compaction with empty log should not create new snapshot")
	}
}

func TestCompact_PreservesSchemaVersion(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Manually bump schema version (simulating a past migration).
	db.st.manifest.SchemaVersion = 7
	db.cfg.schemaVer = 7
	putManifest(ctx, store, "mydb/manifest.json", db.st.manifest, PutCondition{IfMatch: db.st.etag})
	db.st.etag, _ = store.Head(ctx, "mydb/manifest.json")

	// Write + compact.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if m.SchemaVersion != 7 {
		t.Errorf("SchemaVersion = %d, want 7 (preserved)", m.SchemaVersion)
	}
}

func TestCompact_ConcurrentWrite(t *testing.T) {
	// A writer commits during compaction. Compact retries and includes
	// the new write.
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Seed some data.
	for i := 1; i <= 3; i++ {
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'u')`, i), nil)
		})
	}

	// Open a second DB and wrap its manifest PUT with a one-shot hook
	// that makes it commit right before our compact CAS.
	db2, _ := Open(ctx, store, "mydb/")
	defer db2.Close()

	// We can't easily interpose on the first DB's compact, so instead:
	// make db2 commit, then compact db. Since db.Compact refreshes first,
	// it will include db2's write.
	db2.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (99, 'concurrent')`, nil)
	})

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Fresh open should see all 4 rows.
	fresh, _ := Open(ctx, store, "mydb/")
	defer fresh.Close()
	var count int64
	fresh.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if count != 4 {
		t.Errorf("row count = %d, want 4", count)
	}
}

func TestCompact_OpensNewEpoch(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})

	m1, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	oldEpoch := m1.epoch()

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Write again — changeset should go under the NEW epoch.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (2, 'y')`, nil)
	})

	m2, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	newEpoch := m2.epoch()
	if newEpoch == oldEpoch {
		t.Fatal("epoch did not change after compaction")
	}

	csKey := m2.Log[0].Key
	wantPrefix := "mydb/changesets/" + newEpoch + "/"
	if !strings.HasPrefix(csKey, wantPrefix) {
		t.Errorf("changeset key %q not under new epoch prefix %q", csKey, wantPrefix)
	}
}

// --- Auto-compact -----------------------------------------------------------

func TestAutoCompact(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	seedSchema(t, store, "mydb/")

	db, err := Open(ctx, store, "mydb/", WithAutoCompact(3))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// First two writes — log grows.
	for i := 1; i <= 2; i++ {
		db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c, fmt.Sprintf(`INSERT INTO users (id, name) VALUES (%d, 'u')`, i), nil)
		})
	}
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if len(m.Log) != 2 {
		t.Errorf("log len = %d, want 2", len(m.Log))
	}

	// Third write — should trigger auto-compact (threshold=3).
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (3, 'u')`, nil)
	})
	m, _, _ = loadManifest(ctx, store, "mydb/manifest.json")
	if len(m.Log) != 0 {
		t.Errorf("log len = %d, want 0 (auto-compacted)", len(m.Log))
	}
	if m.Seq != 3 {
		t.Errorf("seq = %d, want 3", m.Seq)
	}

	// Data intact.
	var count int64
	db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT COUNT(*) FROM users`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil },
		})
	})
	if count != 3 {
		t.Errorf("row count = %d, want 3", count)
	}
}

// --- GC --------------------------------------------------------------------

func TestGC_DeletesOldEpoch(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Write, compact, write again. Old epoch should now be garbage.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})
	m1, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	oldEpoch := m1.epoch()
	oldSnap := m1.Snapshot.Key

	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (2, 'y')`, nil)
	})

	// Pre-GC: old epoch and old snapshot should still exist.
	oldEpochPrefix := "mydb/changesets/" + oldEpoch + "/"
	if keys, _ := store.List(ctx, oldEpochPrefix); len(keys) == 0 {
		t.Fatal("old epoch already gone before GC")
	}
	if _, _, err := store.Get(ctx, oldSnap); err != nil {
		t.Fatal("old snapshot already gone before GC")
	}

	// GC.
	if err := db.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	// Old epoch gone.
	if keys, _ := store.List(ctx, oldEpochPrefix); len(keys) != 0 {
		t.Errorf("old epoch not deleted: %v", keys)
	}
	// Old snapshot gone.
	if _, _, err := store.Get(ctx, oldSnap); err == nil {
		t.Error("old snapshot not deleted")
	}
	// Current epoch and snapshot preserved.
	m2, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	if _, _, err := store.Get(ctx, m2.Snapshot.Key); err != nil {
		t.Errorf("current snapshot deleted: %v", err)
	}
	if _, _, err := store.Get(ctx, m2.Log[0].Key); err != nil {
		t.Errorf("current changeset deleted: %v", err)
	}
}

func TestGC_PreservesCurrentEpoch(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})

	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	csKey := m.Log[0].Key

	// GC with no compaction yet — current epoch must survive.
	if err := db.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	if _, _, err := store.Get(ctx, csKey); err != nil {
		t.Errorf("current epoch's changeset deleted: %v", err)
	}
}

func TestGC_CleansOrphans(t *testing.T) {
	// Simulate a crashed writer: blob uploaded, manifest never CAS'd.
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// Commit one real changeset so the epoch exists.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	epoch := m.epoch()

	// Inject an orphan in the current epoch.
	orphanKey := "mydb/changesets/" + epoch + "/cs-orphan.bin"
	store.Put(ctx, orphanKey, strings.NewReader("fake changeset"), NoCondition)

	// Compact (closes the epoch) then GC.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := db.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	// Orphan should be gone (swept with the epoch).
	if _, _, err := store.Get(ctx, orphanKey); err == nil {
		t.Error("orphan not cleaned up")
	}
}

func TestGC_PreservesStragglerEpoch(t *testing.T) {
	// If a changeset in an old epoch is still referenced by the current
	// manifest's log (straggler write during compaction), the epoch
	// must NOT be deleted.
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (1, 'x')`, nil)
	})
	m, _, _ := loadManifest(ctx, store, "mydb/manifest.json")
	oldEpoch := m.epoch()

	// Manually construct the "straggler" scenario: manifest has a new
	// snapshot but the log still references a changeset in the old epoch.
	// (In production this happens when a writer commits between
	// compactor's snapshot build and manifest CAS.)
	db.Compact(ctx)
	m2, etag2, _ := loadManifest(ctx, store, "mydb/manifest.json")

	// Inject a straggler: a log entry in the old epoch. We need a valid
	// changeset — build it on a throwaway DB synced to the compacted state
	// (NOT on db.st.conn, which would apply the insert locally and then
	// conflict when GC's refresh tries to apply it from the store).
	stragglerKey := "mydb/changesets/" + oldEpoch + "/cs-straggler.bin"
	scratchConn := openTestDB(t, filepath.Join(t.TempDir(), "scratch.sqlite"))
	defer scratchConn.Close()
	mustExec(t, scratchConn, `INSERT INTO users (id, name) VALUES (1, 'x')`) // match compacted state
	cs, _ := capture(scratchConn, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO users (id, name) VALUES (2, 'straggler')`, nil)
	})
	store.Put(ctx, stragglerKey, strings.NewReader(string(cs)), NoCondition)
	m3 := m2.appendLog(logEntry{Key: stragglerKey, Seq: m2.Seq + 1})
	putManifest(ctx, store, "mydb/manifest.json", m3, PutCondition{IfMatch: etag2})

	// GC must preserve the old epoch because the straggler is live.
	if err := db.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	if _, _, err := store.Get(ctx, stragglerKey); err != nil {
		t.Errorf("straggler changeset deleted: %v", err)
	}
}

func TestGC_EmptyStore(t *testing.T) {
	store := NewMemBlobStore()
	ctx := context.Background()
	db := openWithSchema(t, store, "mydb/")
	defer db.Close()

	// GC with nothing to collect should succeed silently.
	if err := db.GC(ctx); err != nil {
		t.Errorf("GC on clean store: %v", err)
	}
}

// --- groupByEpoch ----------------------------------------------------------

func TestGroupByEpoch(t *testing.T) {
	keys := []string{
		"mydb/changesets/snap-1/cs-a.bin",
		"mydb/changesets/snap-1/cs-b.bin",
		"mydb/changesets/snap-2/cs-c.bin",
		"mydb/changesets/malformed",
	}
	got := groupByEpoch(keys, "mydb/changesets/")

	if len(got["snap-1"]) != 2 {
		t.Errorf("snap-1: got %d keys, want 2", len(got["snap-1"]))
	}
	if len(got["snap-2"]) != 1 {
		t.Errorf("snap-2: got %d keys, want 1", len(got["snap-2"]))
	}
	if _, ok := got[""]; ok {
		t.Error("malformed key created empty epoch")
	}
}
