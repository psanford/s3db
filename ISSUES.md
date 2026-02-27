# s3db — Known Issues

This document tracked issues found during a critical review of the codebase.
All issues listed have been addressed in commits `685453a`..`e210373`.

---

## Resolution Summary

| # | Issue | Severity | Resolution | Commit |
|---|---|---|---|---|
| 1 | Migration retry with dirty local state | **Critical** | `applyMigration` now forces full snapshot refresh on every attempt by clearing `st.snapshotKey` | `685453a` |
| 2 | `applyLog` partial failure loses `localSeq` | **Critical** | `st.localSeq = seq` now assigned before error check | `685453a` |
| 3 | Cross-device `os.Rename` failure | **Critical** | Temp file created in `filepath.Dir(destPath)` | `685453a` |
| 4 | GC snapshot TOCTOU race | **High** | Added `Stat` method + `WithGCGracePeriod` (default 5min); GC checks snapshot age before deleting | `6ad9515` |
| 5 | Auto-compact errors swallowed + lying docstring | **High** | Added `WithCompactErrorHandler`; docstring now says "synchronously" | `6ad9515` |
| 6 | `DeleteObjects` ignores per-key errors | **High** | `DeletePrefix` now inspects `out.Errors` | `6ad9515` |
| 7 | No SQLite context cancellation | **High** | `View`/`Update` now plumb `ctx.Done()` via `SetInterrupt` | `6ad9515` |
| 8 | `View` allows silent writes | **Medium** | `View` wraps fn in `BEGIN`/`ROLLBACK` | `6ad9515` |
| 9 | No CAS backoff; orphan accumulation | **Medium** | Jittered exponential backoff; best-effort orphan delete before recapture | `e210373` |
| 10 | `WithLocalPath` doesn't cache across Opens | **Medium** | Docstring clarified — warm-start caching requires keeping `*DB` alive | `6ad9515` |
| 11 | No fsync before rename | **Medium** | Added `tmp.Sync()` before close | `685453a` |
| 12 | Rebase ordering invariant untested | **Medium** | Added `verifyLocalMatchesReplay` — row-by-row diff of local vs fresh replay | `e210373` |
| 13 | `ErrNotInitialized` dead code | Low | Removed | `6ad9515` |
| 14 | `BlobStore.Head` unused by library | Low | Replaced with `Stat` (returns `BlobInfo{ETag, Size, LastModified}`) | `6ad9515` |
| 15 | Stale "Stage 6" comment | Low | Removed | `685453a` |
| 16 | Duplicated open flags | Low | `syncToManifest` now uses `openLocalConn` helper | `685453a` |
| 17 | `WithMaxRetries(0)` = zero attempts | Low | Values < 1 clamp to 1 | `6ad9515` |
| 18 | No manifest format version | Low | Added `FormatVersion` field; `validate` rejects newer; `putManifest` stamps | `e210373` |
| 19 | `PutCondition` both-set unchecked | Low | Both implementations validate; return `errBothConditions` | `6ad9515` |
| 20 | No guard against shared `WithLocalPath` | Low | Documented as caller constraint | `6ad9515` |
| 21 | `etag.Compute` docstring overclaims | Low | Clarified — only matches S3 for single-part unencrypted uploads | `e210373` |
| 22 | Leftover debug comments in test | Low | Removed | `685453a` |

## Test Coverage Additions

| Gap | Test Added | Commit |
|---|---|---|
| No test for migration vs. concurrent writer | `TestMigrate_ConcurrentWriter` — uses `migrateInterferingStore` to force a regular write between migrator's sync and CAS | `685453a` |
| No test verifying local conn == replay result after rebase | `verifyLocalMatchesReplay` hooked into `TestDoUpdate_CleanRebase` and `TestDoUpdate_ConflictRebase` | `e210373` |
| No test for View-writes-don't-persist | `TestView_WritesAreRolledBack` | `6ad9515` |
| No test for `WithMaxRetries(0)` | `TestWithMaxRetries_ClampsToOne` | `6ad9515` |
| Chaos test doesn't inject post-upload failures | `TestChaos_PhantomManifestCommits` — uses `phantomStore` which succeeds server-side but returns error to client | `e210373` |
| No test for applyLog partial failure | Covered indirectly: parallel-fetch refactor made fetch all-or-nothing; apply-phase failure tested via #2 fix | — |
| GC grace period | `TestGC_GracePeriod` | `6ad9515` |
| Compact error handler | `TestWithCompactErrorHandler` | `6ad9515` |
| Context cancellation | `TestView_InterruptOnContextCancel` | `6ad9515` |
| Manifest format version | `TestManifest_FormatVersion_Rejected`, `_ZeroOK`, `TestPutManifest_StampsFormatVersion` | `e210373` |
| PutCondition both-set | `TestPutCondition_BothSetRejected` | `e210373` |

**Test count: 143 → 154, all passing, race-clean.**

---

## Notes on Specific Fixes

### #1 Migration dirty state — the subtle case

The original `TestMigrate_Concurrent` only tested **migrator vs. migrator**. When a
concurrent migrator wins, it swaps `snapshot.key`, triggering a full refresh on
the loser. But when a **regular writer** wins (advancing seq without changing
the snapshot), `syncToManifest` takes the incremental path and keeps the loser's
dirty, already-migrated local state. Running `Up()` again then fails
("table already exists") or silently double-inserts.

`TestMigrate_ConcurrentWriter` uses `migrateInterferingStore` which hooks the
snapshot upload (between sync and CAS) and injects a regular write at that
exact moment, forcing the retry path with an unchanged snapshot key.

### #2 Severity re-assessment

The stated trigger ("network blip mid-fetch") no longer applies — the parallel
changeset-fetch refactor made fetch all-or-nothing. The bug only triggered on
an apply-phase failure (corrupt changeset). Still fixed as defense in depth.

### #9 Backoff tuning

Base delay starts at 10ms, doubles per retry, caps at 640ms. Jitter is
`base/2 + rand(0, base)` — classic "full jitter" variant. Respects `ctx.Done()`
during the sleep. Test suite runtime went from ~0.3s to ~3.5s due to
concurrent tests now spending time in backoff (expected and acceptable).


---

# Re-Review: Issues Found in the Fixes (2026-02-27)

The fixes for issues #1–#22 were audited. All 22 are genuinely addressed,
but **one new regression** was introduced by the context-cancellation fix
(#7), and **one gap** remains in the GC grace-period fix (#4).

## REGRESSION — High Severity

### 23. `withInterrupt` panics when full-refresh fails mid-Update

**Location**: `s3db.go:374-378` + `commit.go:50-63`

**Confirmed from zombiezen source** (`sqlite.go:286-288`):
```go
if c.closed {
    panic("sqlite.Conn is closed")
}
```

The new `withInterrupt` helper at `s3db.go:374-378`:
```go
func (db *DB) withInterrupt(ctx context.Context, fn func() error) error {
    db.st.conn.SetInterrupt(ctx.Done())
    defer func() { db.st.conn.SetInterrupt(nil) }()  // panics if conn was closed
    return fn()
}
```

The comment at `s3db.go:366-368` correctly notes that `st.conn` may be
**replaced** during `fn` (full refresh), and handles that by reading
`db.st.conn` at defer time. But it does NOT cover the case where the
replacement **failed** — `syncToManifest` may return an error with `st.conn`
still pointing at the CLOSED old connection:

```go
// commit.go:50-63
if needRefresh {
    if err := st.conn.Close(); err != nil { ... }   // conn now CLOSED
    if err := downloadSnapshot(...); err != nil {
        return err                                   // returns with conn STILL closed
    }
    conn, err := openLocalConn(localPath)
    if err != nil {
        return err                                   // same here
    }
    st.conn = conn                                   // only reached on success
}
```

**Failure path**:
1. `Update` → `withInterrupt` wraps `doUpdate`
2. `doUpdate` → CAS 412 → new manifest shows snapshot advanced (compaction
   happened on another DB instance) → `rollbackAndResync` → `syncToManifest`
3. `syncToManifest`: `needRefresh=true` → `st.conn.Close()` → `downloadSnapshot`
   fails (network blip, S3 5xx) → return error with `st.conn` still the closed conn
4. Error propagates to `withInterrupt`; deferred `SetInterrupt(nil)` on closed
   conn → **panic**

**Trigger**: any transient S3 error during snapshot download while auto-compact
or manual `Compact` is running concurrently on another DB instance. This is a
realistic scenario for Lambda fleets with auto-compact enabled.

**Why tests don't catch this**:
- `TestChaos_NoCorruption` / `TestChaos_PhantomManifestCommits` never compact
  during the Update loop — the `needRefresh` branch in `doUpdate` is never taken.
- `TestView_InterruptOnContextCancel` uses a pre-cancelled ctx that fails in
  `refreshManifest` (which is OUTSIDE `withInterrupt`), so it never reaches
  the vulnerable code.

**Fix options**:
- **a)** Make `syncToManifest` always leave `st.conn` valid: set `st.conn = nil`
  immediately after `Close()`, and have `withInterrupt`'s defer check for nil.
- **b)** Make `withInterrupt`'s defer defensive:
  ```go
  defer func() {
      defer func() { recover() }() // swallow closed-conn panic
      db.st.conn.SetInterrupt(nil)
  }()
  ```
- **c)** Cache the original conn at entry and only call `SetInterrupt(nil)` if
  `db.st.conn` is still the same pointer (though this defeats the "reset
  interrupt on the NEW conn" logic).

Option **(a)** is cleanest — nil-check is cheap and the DB is unusable after a
failed refresh anyway (caller will see the error and presumably Close or retry
with a fresh Open).

---

## Gap in Fix — Medium Severity

### 24. GC grace period doesn't protect changesets

**Location**: `gc.go:54-77`

The grace period fix only covers **snapshot** deletion (`gc.go:88-108`). The
**changeset epoch** deletion loop at `gc.go:54-77` has the same TOCTOU race
but no grace period:

1. Reader A: `loadManifest` → sees log `[cs-1, cs-2, cs-3]` in epoch E
2. Compactor B: `Compact()` → new manifest: epoch F, log empty
3. Compactor B: `GC()` → epoch E is not current, has no live refs →
   `DeletePrefix(epoch E)` **immediately** (no age check)
4. Reader A: `applyLog` fetches cs-1…cs-3 → `ErrNotFound` → `Open` fails

Window is smaller than for snapshots (changesets are KB, fetch in parallel),
but it's still a race. Same fix applies: `Stat` one object per epoch (e.g.
the oldest), skip if younger than `gcGracePeriod`.

---

## Minor Observations (correct-but-suboptimal — no action needed)

### 25. Migration force-refresh downloads snapshot even on first attempt

**Location**: `migrate.go:103`

`db.st.snapshotKey = ""` forces a full download on **every** `applyMigration`
call, including the first attempt when local state is already clean from `Open`.
For an N-migration chain on a fresh DB, that's N redundant snapshot downloads.
Correct (and simpler than tracking retry state), but wasteful. Fine for
correctness-first; could be optimized later by only clearing `snapshotKey`
inside the `errors.Is(err, ErrPreconditionFailed)` retry branch.

### 26. Orphan cleanup doesn't cover the final attempt

**Location**: `commit.go:272-273`

When `doUpdate` returns `ErrConflict`, `orphan` holds the last uploaded blob's
key but it's never deleted. One orphan per exhausted-retries call. GC will
eventually sweep it. Could add `if orphan != "" { cfg.store.Delete(ctx, orphan) }`
before line 272.

### 27. `withInterrupt` not applied to Compact/GC/migrations

**Location**: `compact.go:27`, `gc.go:28`, `migrate.go:26`

`Compact` runs `VACUUM` (potentially slow). Migrations run arbitrary user `Up()`.
Neither is interruptible via ctx. Inconsistent with View/Update but not a bug.
Note: extending `withInterrupt` to these callers would also expose them to
bug #23, so fix #23 first.

### 28. View's BEGIN/ROLLBACK breaks user code with explicit transactions

**Location**: `s3db.go:233`

If a user's `fn` previously did its own `BEGIN`/`COMMIT` inside `View`, it now
fails with `SQLITE_ERROR: cannot start a transaction within a transaction`.
Acceptable behavior change (docs always said read-only), but could surprise
upgraders. Not in any CHANGELOG.

### 29. Backoff variant naming

ISSUES.md note on #9 says "full jitter" but `base/2 + rand(0, base)` is the
AWS "equal jitter" variant (range `[base/2, 1.5·base)`). Full jitter is
`rand(0, base)`. Works either way; just a naming nitpick.

---

## Verification Scorecard

| Category | Count | Details |
|---|---|---|
| Original issues fixed correctly | 20 | All three Critical bugs correct |
| Original issues partially fixed | 2 | #4 (changesets not covered), #7 (Compact/GC/mig uncovered) |
| New regressions introduced | 1 | #23 — `withInterrupt` panic (**High**) |
| New gaps identified | 1 | #24 — changeset epoch TOCTOU |
| Minor observations | 5 | #25–#29 — no action needed |

**Only #23 needs immediate attention** — it turns a recoverable network error
into an unrecoverable panic under a realistic concurrency pattern (concurrent
compaction + transient download failure).

---

# Third Review: After b66911c + 43c0258 (2026-02-27)

Two commits since the last review:
- **b66911c** — fixes for #23–#26
- **43c0258** — new `Pull`/`Push` API + `cmd/s3db` CLI tool

All tests pass, including race detector. Build is clean, `go vet` clean.

## Verification of Re-Review Fixes

| # | Issue | Status | Notes |
|---|---|---|---|
| 23 | `withInterrupt` panic | **Fixed** | `st.conn = nil` after Close (`commit.go:58`); nil-check in defer (`s3db.go:380`). `TestUpdate_RefreshFailureDoesNotPanic` is a thorough regression test — sets up compaction on db2, swaps db1's store to fail snapshot downloads, wraps in `recover()` to catch panic as test failure. |
| 24 | GC grace on changesets | **Fixed** (minor semantic quirk) | Epoch deletion now `Stat`s first key, skips if younger than grace. See note #31 below. |
| 25 | Migration first-attempt download | **Fixed correctly** | `lastAttemptedVer` tracking. `forceRefresh` only true when same migration version is retried (writer interference); concurrent-migrator case handled naturally by snapshot-key change. Traced both paths — correct. |
| 26 | Orphan on ErrConflict | **Fixed** | Best-effort delete before returning `ErrConflict` (`commit.go:275-277`). |

## New Issues

### 30. DB is stuck (not panicked) after a failed refresh

**Severity**: Medium
**Location**: `commit.go:51`

The #23 fix prevents the panic but leaves the DB in an unrecoverable-without-reopen
state. After `syncToManifest` fails mid-refresh (`st.conn = nil`), the NEXT
`Update`/`View` call hits `syncToManifest` again, tries `st.conn.Close()` on
nil, and zombiezen returns the error `"sqlite: close: nil connection"`
(verified — zombiezen nil-checks in Close, no panic). Every subsequent call
returns the same error until `Close()` + re-`Open()`.

**Strictly better than before** (error vs. panic), but not self-healing. A
3-line fix would make it recover automatically:

```go
// commit.go:50-53
if needRefresh {
    if st.conn != nil {
        if err := st.conn.Close(); err != nil { ... }
        st.conn = nil
    }
    // ...download, reopen as before
}
```

With this, a failed refresh followed by a successful retry (e.g. transient
network error clears) would fully recover the DB. As-is, the caller must
know to Close+reopen on this specific error.

**Not urgent** — the behavior is documented-ish (`TestUpdate_RefreshFailureDoesNotPanic`
shows Close works after), but the recovery path isn't documented in godoc.

### 31. GC changeset grace check uses upload time, not garbage time

**Severity**: Low
**Location**: `gc.go:80`

The fix for #24 `Stat`s `epochKeys[0]` (lexicographically first key) and uses
its `LastModified` as a proxy for "how long has this epoch been garbage". But
the key's upload time is NOT when the epoch became garbage — it's when that
specific changeset was uploaded, which could be arbitrarily earlier (before
compaction).

**Scenario**:
1. Changeset `cs-aaa` uploaded at T-10min (was live in the log)
2. Compaction at T-30s (cs-aaa subsumed, epoch becomes garbage)
3. Reader loaded old manifest at T-20s, will fetch cs-aaa at T+10s
4. GC at T: `Stat(cs-aaa)` → age = 10min > grace (5min) → DELETE
5. Reader at T+10s: 404

The grace check intended to protect readers with a 30s-old manifest, but the
age check sees 10 minutes. The RIGHT thing to check is the **current snapshot's**
age (= time since compaction), not the old changeset's age.

**Suggested fix**: at the top of GC, `Stat(m.Snapshot.Key)`. If the snapshot
is younger than `grace`, skip the ENTIRE epoch-sweep (epochs only become
garbage via compaction; recent compaction = all garbage is recent). One
`Stat` call per GC, not one per epoch.

**Why it's only Low severity**: the current code is strictly safer than
no-grace (never deletes anything it shouldn't prematurely on the writer side;
may occasionally delete something a slow reader wants). The window is small
and readers fail fast with a clear error. This is a refinement, not a fix.

### 32. CLI `status`/`compact`/`gc` fail on migrated databases

**Severity**: Medium (usability, not correctness)
**Location**: `cmd/s3db/main.go:216,247,287`

All three commands call `s3db.Open(ctx, store, prefix)` with no
`WithMigrations` option. `Open` always runs `runMigrations`, which sees
`maxVer=0 < manifest.SchemaVersion` and returns `ErrSchemaTooNew`.

**Verified**:
```
Open (no migrations) on v1 DB:
  s3db: database schema is newer than this client supports:
  database at schema v1, client knows up to v0
```

Result: the CLI cannot status/compact/gc any database that has ever run
a migration — which is every real-world database. `Pull` and `Push` use
the raw manifest API directly and are unaffected.

**Fix options**:
- **a)** Add a `WithSkipSchemaCheck` option for admin operations (risky —
  Compact runs VACUUM on a schema the CLI doesn't understand, which is fine
  since VACUUM is schema-agnostic, but philosophically unclean)
- **b)** Have the CLI peek the manifest's `SchemaVersion` first and pass a
  stub `WithMigrations([]Migration{{Version: N, Up: noop}})` to satisfy the
  check (hacky, breaks on gaps)
- **c)** Refactor Compact/GC into top-level functions (like Pull/Push) that
  don't need a `*DB` and don't check schema (cleanest)

### 33. CLI `-grace 0` means "library default", not "zero"

**Severity**: Low
**Location**: `cmd/s3db/main.go:283`

```go
if *grace != 0 {
    opts = append(opts, s3db.WithGCGracePeriod(*grace))
}
```

There's no way to explicitly set grace=0 from the CLI. `-grace 0` is
indistinguishable from not passing the flag. Minor UX papercut — could
use a sentinel default like `-1` meaning "library default", or `flag.Visit`
to check if the flag was actually set.

### 34. `Push` uses `==` instead of `errors.Is`

**Severity**: Low
**Location**: `pullpush.go:117`

```go
if err == ErrPreconditionFailed {
```

Every other call site in the codebase uses `errors.Is(err, ErrPreconditionFailed)`.
Not a bug today (both `S3BlobStore.Put` and `MemBlobStore.Put` return the
sentinel directly), but brittle against future wrapping.

### 35. `s3db.go` missing trailing newline

**Severity**: Trivial
**Location**: `s3db.go:385` (end of file)

`git diff` shows `\ No newline at end of file`. Some linters will flag.

## New Feature Assessment: Pull/Push

`Pull` and `Push` are clean, well-tested, and correct. Push's safety
properties under concurrency are solid:

- **Concurrent Update during Push**: Push's manifest CAS changes the ETag;
  the concurrent Update gets 412, re-syncs, sees the new `snapshot.key`,
  does a full refresh → picks up the pushed state. Verified by tracing
  `syncToManifest` — the `snapshotKey != m.Snapshot.Key` check catches it.

- **Concurrent Update before Push**: Push's `expectedSeq` check fails with
  `ErrSeqMismatch`. `TestPush_SeqMismatch` covers this.

- **CAS window race**: `expectedSeq` passes but CAS fails (someone committed
  in the load→put window). Push translates 412 → `ErrSeqMismatch` for
  consistent error handling. `TestPush_ConcurrentCAS` covers this with
  `pushInterposingStore`.

- **Force push**: Documented as dangerous; `TestPush_Force` honestly
  demonstrates that concurrent writes are lost. Good.

- **SQLite validation**: `PRAGMA quick_check` before upload. Catches
  non-SQLite files and corruption. `TestPush_ValidatesSQLite` covers.

- **Schema preservation**: `withSnapshot` preserves `SchemaVersion`.
  `TestPush_PreservesSchemaVersion` covers.

The `.seq` sidecar file is a nice workflow touch.

## Summary

| # | Issue | Severity | Status |
|---|---|---|---|
| 23 | withInterrupt panic | — | **Fixed** |
| 24 | GC grace on changesets | — | **Fixed** |
| 25 | Migration first-attempt download | — | **Fixed** |
| 26 | Orphan on ErrConflict | — | **Fixed** |
| 30 | DB stuck after failed refresh (error, not panic) | Medium | Self-heal would be 3-line fix |
| 31 | GC changeset grace uses upload time | Low | Safer fix: check snapshot age |
| 32 | CLI status/compact/gc fail on migrated DBs | Medium | Real usability bug |
| 33 | CLI `-grace 0` = default | Low | UX papercut |
| 34 | Push uses `==` not `errors.Is` | Low | Style consistency |
| 35 | Missing trailing newline | Trivial | — |

**No new panics, no correctness bugs.** The two Medium items are #30 (quality
of life — DB should self-heal after transient download failure) and #32
(CLI unusable on any migrated DB — real blocker for the CLI's utility).