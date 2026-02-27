# Implementation Plan: S3-Backed SQLite Library (Go)

Companion to [DESIGN.md](./DESIGN.md). This document covers **how to build it**: dependencies, package layout, build order, testing strategy, and milestones.

---

## Dependencies

| Purpose | Choice | Rationale |
|---|---|---|
| SQLite binding | `zombiezen.com/go/sqlite` | First-class session/changeset API with Go-native conflict handler callbacks. CGo required. Not `database/sql` but that's fine — one connection, one file. |
| S3 client | `github.com/aws/aws-sdk-go-v2/service/s3` | Conditional write support (`IfMatch`, `IfNoneMatch`), context-aware, standard. |
| Testing | stdlib `testing`, optionally `testify/require` for ergonomics | No heavy framework. |
| Integration test backend | MinIO (Docker) or `s3mock` | Real conditional-write semantics; localstack also works. |

**Build requirement:** CGo means Lambda artifacts must be built on Linux (or in a Linux container). Document this in the README — `GOOS=linux go build` alone won't cut it; need `CGO_ENABLED=1` and a Linux toolchain or Docker-based build.

---

## Package layout

```
s3db/
  s3db.go          – DB struct, Open(), View(), Update(), public option types
  options.go       – functional options (WithMigrations, WithAutoCompact, etc.)
  errors.go        – sentinel errors (ErrConflict, ErrSchemaMismatch, ErrSchemaTooNew, ErrNotInitialized)

  manifest.go      – Manifest struct, JSON marshal/unmarshal, validation
  blob.go          – BlobStore interface + PutCondition type
  blob_s3.go       – BlobStore impl backed by aws-sdk-go-v2
  blob_mem.go      – in-memory fake with real ETag-CAS semantics (for tests)

  sync.go          – syncLocal(): bring local .sqlite file to manifest state
  session.go       – capture(): open session, run fn, return changeset bytes
  apply.go         – applyChangeset(): wrapper over sqlite3changeset_apply + conflict handler
  commit.go        – the Update() CAS loop (upload blob → CAS manifest → rebase/retry)

  compact.go       – Compact(): build snapshot, CAS manifest with empty log
  gc.go            – GC(): sweep old epochs + old snapshots
  migrate.go       – migration runner (forced-compaction per migration)

  internal/
    etag.go        – ETag normalization (S3 quotes them; strip quotes consistently)

  s3db_test.go     – high-level tests against blob_mem
  commit_test.go   – CAS loop edge cases, rebase scenarios
  concurrent_test.go – N goroutines × M updates, assert final state
  chaos_test.go    – failure injection wrapper around blob_mem
  integration_test.go – build-tagged, runs against MinIO
```

Single top-level package. No subpackages except `internal/` for small helpers — keeps the API surface tight and avoids import cycles.

---

## Build order

Each stage should be independently testable before moving on. Order is chosen so nothing depends on unbuilt pieces.

### Stage 1: Storage abstraction

**Deliverables:** `blob.go`, `blob_mem.go`, `errors.go`, `internal/etag.go`

```go
type BlobStore interface {
    Get(ctx context.Context, key string) (body io.ReadCloser, etag string, err error)
    Head(ctx context.Context, key string) (etag string, err error)
    Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (etag string, err error)
    List(ctx context.Context, prefix string) (keys []string, err error)
    Delete(ctx context.Context, key string) error
    DeletePrefix(ctx context.Context, prefix string) error
}

type PutCondition struct {
    IfMatch     string // empty = unconditional
    IfNoneMatch bool   // true = only if key doesn't exist
}
```

`Get` returns a stream so snapshots can be copied directly to disk without buffering. `Put` takes a reader; the body may be partially consumed on error, but this is fine for our protocols — manifests are rebuilt (not retried) on CAS failure, and changesets/snapshots use unique keys so never contend.

`blob_mem.go` implements this with `map[string]entry{body, etag}` + mutex. It drains readers into memory (unavoidable for a map-backed store). ETags are content hashes (same behavior as S3 for non-multipart). `Put` with `IfMatch` returns a distinguishable "precondition failed" error.

**Tests:** Put/Get roundtrip, CAS success/failure, IfNoneMatch race, List prefix filtering, DeletePrefix.

**Done when:** Full test coverage of CAS semantics on the fake. Everything downstream tests against this.

---

### Stage 2: Manifest

**Deliverables:** `manifest.go`

```go
type Manifest struct {
    Seq           int64      `json:"seq"`
    SchemaVersion int        `json:"schema_version"`
    Snapshot      BlobRef    `json:"snapshot"`
    Log           []LogEntry `json:"log"`
}

type BlobRef struct {
    Key string `json:"key"`
    Seq int64  `json:"seq"`
}

type LogEntry struct {
    Key string `json:"key"`
    Seq int64  `json:"seq"`
}

func (m *Manifest) Epoch() string       // derives epoch name from Snapshot.Key
func (m *Manifest) Validate() error     // seq monotonicity, no gaps in log, etc.
func loadManifest(ctx, store, key) (*Manifest, etag string, err error)
func putManifest(ctx, store, key, m, cond) (etag string, err error)
```

**Tests:** JSON roundtrip, validation rejects gaps/out-of-order seqs, epoch derivation.

**Done when:** Can marshal/unmarshal/validate. No S3 or SQLite involvement yet.

---

### Stage 3: Local DB reconstruction

**Deliverables:** `sync.go`, first real use of the sqlite binding

```go
// Brings the local SQLite file to the state described by manifest.
// Assumes localSeq is the seq the local file is currently at (-1 if fresh).
// Downloads snapshot if localSeq < manifest.Snapshot.Seq, else reuses.
// Applies missing log entries.
func syncLocal(ctx, store, manifest, conn *sqlite.Conn, localSeq int64) (newLocalSeq int64, err error)
```

This needs `apply.go`'s changeset-apply function, so build a minimal version of that here too:

```go
// Applies a changeset blob to conn. Uses SAVEPOINT for atomicity.
// conflictPolicy determines handler behavior; start with strictAbort only.
func applyChangeset(conn *sqlite.Conn, cs []byte, policy conflictPolicy) error
```

**Tests:** Seed a snapshot + 3 changesets in blob_mem, call syncLocal from various starting seqs, verify resulting DB state matches. Test the "local is ahead of snapshot, only apply tail of log" fast path.

**Done when:** Given a manifest and a blob store, can materialize a correct local DB.

---

### Stage 4: Changeset capture

**Deliverables:** `session.go`

```go
// Runs fn inside a session, returns the captured changeset.
// Uses SAVEPOINT so fn's effects are applied to conn on success, rolled back on fn error.
func capture(conn *sqlite.Conn, fn func(*sqlite.Conn) error) (changeset []byte, err error)
```

Implementation: create `sqlite.Session`, attach all tables (or accept a table list), SAVEPOINT, run fn, RELEASE on success / ROLLBACK on error, close session, return changeset bytes.

**Tests:** Run INSERT/UPDATE/DELETE in fn, verify changeset is non-empty and can be applied to a fresh copy of the DB to reproduce the same state. Verify fn error rolls back and returns empty changeset.

**Done when:** Can turn a function that mutates a connection into a changeset blob.

---

### Stage 5: The commit loop

**Deliverables:** `commit.go`, the heart of `Update()`

```go
func (db *DB) Update(ctx context.Context, fn func(*sqlite.Conn) error) error
```

Implements the full protocol from DESIGN.md:
1. Lock `db.mu`
2. Fetch manifest, sync local
3. Check `schema_version` match
4. Capture changeset via `fn`
5. Upload changeset to epoch prefix
6. CAS manifest
7. On 412: re-sync, try rebase (apply captured changeset with strict-abort handler)
   - Clean → retry CAS
   - Conflict → re-run `fn` from scratch
8. After `maxRetries` → `ErrConflict`

**Edge cases to handle:**
- Empty changeset (fn did only SELECTs, or no-op UPDATE) → skip upload, skip CAS, return nil
- fn returns error → rollback, return error unwrapped
- Network error on blob PUT vs manifest PUT (different retry semantics — blob PUT is idempotent with UUID key, manifest PUT is not)
- Context cancellation mid-loop

**Tests:**
- Happy path: single Update, verify manifest + blob in store
- Sequential Updates from same DB instance: verify seqs increment, log grows
- Concurrent Updates (two goroutines, same DB instance): both succeed, serialized by mutex
- Simulated 412 with disjoint changeset: rebase succeeds, no re-invocation of fn
- Simulated 412 with conflicting changeset: rebase fails, fn re-invoked, succeeds second time
- maxRetries exhaustion → ErrConflict
- Schema version mismatch → ErrSchemaMismatch before fn runs

**Done when:** Update() works against blob_mem under all scenarios above.

---

### Stage 6: Open, View, and the DB struct

**Deliverables:** `s3db.go`, `options.go`

```go
func Open(ctx context.Context, store BlobStore, prefix string, opts ...Option) (*DB, error)
func (db *DB) View(ctx context.Context, fn func(*sqlite.Conn) error) error
func (db *DB) Close() error
```

`Open`:
- Applies options
- Tries `GET manifest.json`; on 404, `PUT` an empty seq-0 manifest with `IfNoneMatch`; on 412 (race), re-GET
- Opens local SQLite connection (at `opts.localPath` or a temp file)
- Runs migrations if configured (stage 8 — stub for now)
- Syncs local DB

`View`:
- Locks (read lock if we add RWMutex later; mutex for now)
- Syncs local DB to current manifest
- Runs fn
- No write, no CAS

**Tests:** Open against empty store creates manifest. Open against existing store reads it. View reflects Updates. Close cleans up.

**Done when:** End-to-end usage pattern works: Open → Update → View → Close, all against blob_mem.

---

### Stage 7: Compaction and GC

**Deliverables:** `compact.go`, `gc.go`

```go
func (db *DB) Compact(ctx context.Context) error
func (db *DB) GC(ctx context.Context) error
```

`Compact`:
- Sync local, VACUUM, write local file to `snapshots/snap-<uuid>.sqlite`
- CAS manifest with `{snapshot: new, log: [], seq: unchanged}`
- On 412: re-sync (picks up new log entries), rebuild snapshot, retry. Cap retries.

`GC`:
- List epoch prefixes under `changesets/`
- For each prefix not matching current epoch: check if any contained key appears in `manifest.log`; if none, `DeletePrefix`
- List `snapshots/`, delete any key ≠ current `manifest.snapshot.key` AND older than grace period (use S3 object LastModified, or encode timestamp in key)

**Auto-compact wiring:** `WithAutoCompact(threshold int)` option. At the end of `Update()`, if `len(manifest.log) >= threshold`, spawn `go db.Compact(detachedCtx)` (errors logged, not returned). Needs a `sync.Once`-style guard so concurrent Updates don't all trigger compaction.

**Tests:**
- Compact with empty log → no-op
- Compact with N log entries → new snapshot, empty log, seq unchanged
- Compact with concurrent writer → eventually succeeds, includes the concurrent write
- GC after compaction → old epoch deleted, old snapshot deleted, current ones untouched
- GC is safe to run concurrently with writers (no live data deleted)

**Done when:** Log doesn't grow unboundedly; orphans get cleaned.

---

### Stage 8: Migrations

**Deliverables:** `migrate.go`

```go
type Migration struct {
    Version int
    Name    string
    Up      func(*sqlite.Conn) error
}

// internal, called by Open()
func (db *DB) migrate(ctx context.Context, migrations []Migration) error
```

Per DESIGN discussion: each migration is a forced compaction. Loop:
1. Load manifest, check `schema_version`
2. For each migration where `Version > manifest.SchemaVersion`:
   - Sync local, run `Up`, VACUUM
   - PUT snapshot
   - CAS manifest with `{schema_version: Version, snapshot: new, log: []}`
   - On 412: re-GET. If `schema_version >= Version`, skip (someone else ran it). Else retry.
3. If `manifest.SchemaVersion > max(migrations)` → `ErrSchemaTooNew`

Wire into `Open()`: `WithMigrations(migs)` stores them on the DB struct; `Open` calls `migrate` after manifest is available.

**Tests:**
- Fresh DB + 3 migrations → schema_version=3, all DDL applied
- DB at v1 + migrations v1,v2,v3 → only v2,v3 run
- Two concurrent Opens with same migrations → one runs them, other skips, both succeed
- Code with migrations v1,v2 opens DB at schema_version=3 → ErrSchemaTooNew
- Migration with data backfill (DML) → rows present in snapshot

**Done when:** Schema evolution works; old code can't write to new schema.

---

### Stage 9: Real S3 backend

**Deliverables:** `blob_s3.go`

Thin wrapper over `aws-sdk-go-v2/service/s3`:
- `Get` → `GetObject`, extract ETag from response
- `Head` → `HeadObject`
- `Put` with `IfMatch` → `PutObject` with `IfMatch` field; translate 412 PreconditionFailed to our sentinel error
- `Put` with `IfNoneMatch` → `PutObject` with `IfNoneMatch: aws.String("*")`
- `List` → `ListObjectsV2` with pagination
- `DeletePrefix` → list + `DeleteObjects` batch

**Gotchas:**
- S3 ETags come quoted (`"abc123"`). Normalize in `internal/etag.go`.
- Multipart upload ETags are not content hashes — but we're not doing multipart for these small blobs, so fine.
- `IfMatch` on PutObject is relatively new (GA 2024). SDK version must support it. Pin a minimum version in go.mod.

**Tests:** Build-tagged integration tests (`//go:build integration`) against MinIO in Docker. Re-run the core scenarios from stages 5–8 against real S3 semantics.

**Done when:** All integration tests pass against MinIO.

---

### Stage 10: Hardening and docs

- **Chaos tests** — wrap `blob_mem` with a fault injector: random errors, random latency, crashes between blob-PUT and manifest-PUT. Run the concurrent-update test under chaos, assert no corruption and correct final state.
- **README** — usage examples, Lambda build instructions (CGo cross-compile), cost model, when-not-to-use-this
- **Godoc** — every exported symbol
- **Benchmarks** — Update latency against blob_mem (baseline) and MinIO (realistic); cold vs warm read

---

## Milestone checkpoints

| Milestone | Stages | Demo |
|---|---|---|
| **M1: Storage + manifest** | 1–2 | Can round-trip a manifest through the fake store with CAS |
| **M2: Read path** | 3 | Can reconstruct a DB from snapshot + log |
| **M3: Write path** | 4–6 | `Open` → `Update` → `View` works end-to-end against blob_mem |
| **M4: Lifecycle** | 7–8 | Compaction, GC, migrations all work; auto-compact wired |
| **M5: Production** | 9–10 | Runs against real S3; chaos-tested; documented |

M3 is the first point where the library is *usable* (if you don't mind the log growing forever). M4 makes it operable. M5 makes it shippable.

---

## Open decisions

Defaults below; revisit if they cause friction.

| Decision | Default | Alternative |
|---|---|---|
| Mutex vs RWMutex on `DB` | Mutex (simple) | RWMutex if View contention matters. Premature for Lambda (typically 1 goroutine). |
| Auto-compact runs synchronously or in goroutine | Goroutine (don't block user's write) | Synchronous if users want backpressure |
| GC grace period for old snapshots | 1 hour | Configurable option; depends on max Lambda runtime |
| Session attaches all tables vs explicit list | All tables | Option to specify if users want to exclude some (e.g. local cache tables) |
| Conflict policy exposed as option | No (hard-coded strict-abort) | Expose if a use case needs last-writer-wins |
| `embed.FS` migration frontend | Skip for v1 | Easy to add later as a helper that builds `[]Migration` |
| Down migrations | Skip | S3 versioning on manifest is the rollback mechanism |

---

## Test matrix summary

| Layer | Test type | Against |
|---|---|---|
| BlobStore contract | Unit | blob_mem, then blob_s3 via integration |
| Manifest | Unit | In-memory |
| sync/capture/apply | Unit | blob_mem + local SQLite |
| commit loop | Unit + concurrency | blob_mem |
| Compact/GC/Migrate | Unit + concurrency | blob_mem |
| End-to-end | Integration | MinIO |
| Chaos | Fuzz/soak | Fault-injecting blob_mem wrapper |

Run unit + concurrency on every commit. Integration on PR. Chaos nightly or on-demand.
