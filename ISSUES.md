# s3db — Known Issues

A critical review of the codebase. Issues are ordered by severity.

---

## Critical / Correctness Bugs

### 1. Migration retry after 412 leaves local DB in a dirty state

**Location**: `migrate.go:78-84`, `migrate.go:93-145`

When `applyMigration` loses the CAS race (returns `ErrPreconditionFailed`),
`runMigrations` loops back to retry. But by that point the local conn
**already has `Up()` applied** — DDL, DML backfill, and VACUUM all mutated
the local file, and none of it was wrapped in a SAVEPOINT.

On retry, `syncToManifest` (`migrate.go:98`) takes the **incremental path**
(snapshot key unchanged, `localSeq >= snapshot.seq`), so it only replays new
log entries — it does **not** discard the dirty migration state. Then `Up()`
runs **a second time** on an already-migrated DB:

- `CREATE TABLE` → fails with "table already exists"
- `ALTER TABLE ADD COLUMN` → fails with "duplicate column name"
- DML backfill with explicit PK → fails with PK constraint violation
- `CREATE TABLE IF NOT EXISTS` + INSERT → **silently double-inserts**

**Trigger**: a regular `Update` commits between the migrator's
`syncToManifest` and its manifest CAS. This is the **rolling-deploy
scenario** — old clients at schemaVer=N keep writing while a new client
tries to migrate to N+1.

`TestMigrate_Concurrent` only tests concurrent **migrators**. Those change
`snapshot.key` when they win, which triggers a full refresh on the loser
(`commit.go:49`). A regular writer does *not* change the snapshot key, so
the loser stays on the incremental path and keeps its dirty state.

**Fix options**:
- Force a full snapshot re-download at the start of each `applyMigration`
  attempt (ignore the incremental check).
- Or: wrap `Up()` + VACUUM in a SAVEPOINT and roll back before retrying.
  (Tricky — VACUUM cannot run inside a transaction.)
- Simplest: close the conn, re-download the snapshot, reopen, *then* run Up.

---

### 2. `applyLog` partial failure leaves `localSeq` out of sync with conn

**Location**: `commit.go:66-70`

```go
seq, err := applyLog(ctx, cfg.store, st.conn, m.Log, st.localSeq)
if err != nil {
    return err  // st.localSeq NOT updated, but conn may have advanced
}
st.localSeq = seq
```

If `applyLog` successfully applies changesets for seq N+1 and N+2, then
fails on N+3 (network blip mid-fetch, corrupt blob, etc.), `conn` is at
seq N+2 but `st.localSeq` stays at N.

The next `syncToManifest` will try to re-apply N+1 and N+2 — whose
before-images no longer match (the rows were already mutated). This fires
`errChangesetConflict` and the DB is **permanently stuck** until a full
refresh happens to be triggered by an unrelated snapshot change.

`applyLog` itself returns the correct partial seq on error
(`sync.go:170-177`), but the caller discards it. The same bug exists in
`s3db.go:108-115` (`Open`), though at least that path closes the conn and
fails the whole Open rather than corrupting an existing DB.

**Fix**: assign `st.localSeq = seq` *before* checking the error.

---

### 3. `os.Rename` cross-device failure in `downloadSnapshot`

**Location**: `sync.go:43`, `sync.go:63`

```go
tmp, err := os.CreateTemp("", "s3db-snap-*")   // system $TMPDIR
...
return os.Rename(tmpPath, destPath)            // may cross devices
```

The comment at `sync.go:33` says "temp sibling" but `os.CreateTemp("", ...)`
writes to the **system** temp dir, not a sibling of `destPath`. If
`destPath` is on a different filesystem (mounted volume, container overlay,
`WithLocalPath("/mnt/data/db.sqlite")`), `os.Rename` fails with `EXDEV`
(invalid cross-device link).

Works by luck on Lambda where the only writable location is `/tmp`.

**Fix**: `os.CreateTemp(filepath.Dir(destPath), "s3db-snap-*")`

---

## High-Severity Issues

### 4. GC can delete a snapshot an in-flight reader needs

**Location**: `gc.go:78-96`

TOCTOU race:
1. Reader A: `loadManifest` → sees old snapshot `snap-X`
2. Compactor B: `Compact()` → manifest now points at `snap-Y`
3. Compactor B: `GC()` → deletes `snap-X` (not the current snapshot)
4. Reader A: `downloadSnapshot(snap-X)` → 404 → `Open` fails

The comment at `gc.go:79-83` explicitly acknowledges this:

> "A more conservative policy would add a grace period to protect in-flight
> readers holding an old manifest... If this becomes a concern, add a
> time-based check on object LastModified."

`DESIGN.md:133` also documents the grace-period approach. The
implementation skipped it.

**Fix**: check object `LastModified` and skip snapshots younger than a
configurable grace period (default ~5 min).

---

### 5. Auto-compact errors are silently swallowed + docstring lies

**Location**: `s3db.go:260-262`, `options.go:35-37`

```go
if db.opts.autoCompactAfter > 0 && len(db.st.manifest.Log) >= db.opts.autoCompactAfter {
    _ = db.compactLocked(ctx)
}
```

If compaction consistently fails (IAM permissions on snapshot PUT, bucket
policy, S3 5xx during the VACUUM→upload→CAS sequence), the log grows
unbounded with **zero signal** to the operator. Cold-start latency degrades
silently until the DB is effectively unusable.

**Also**: `WithAutoCompact`'s docstring says:

> "Update will trigger a compaction in the background"

It does not. `compactLocked` runs **synchronously** inside `db.mu`, blocking
the caller's `Update` return until compaction finishes. For a 10 MB snapshot
this adds noticeable latency to every Nth write.

**Fix**: return a way to observe compaction errors (callback, error channel,
or at minimum stderr logging), and fix the docstring to say "synchronously".

---

### 6. `DeleteObjects` ignores per-object delete failures

**Location**: `blob_s3.go:155-164`

S3's `DeleteObjects` response includes an `Errors` field listing individual
keys that failed to delete (object lock, MFA-delete, permissions). The code
sets `Quiet: true` and never inspects the response. GC reports success while
leaving garbage behind.

**Fix**: inspect `out.Errors` and return a wrapped error if non-empty.

---

### 7. No context cancellation plumbed to SQLite

**Location**: `s3db.go:219-227`, `s3db.go:244-265`, `commit.go`

`View` and `Update` accept a `ctx` but the SQLite connection never gets
`conn.SetInterrupt(ctx.Done())`. If `ctx` is cancelled during a long query
inside `fn`, the query runs to completion regardless. This defeats Lambda
timeouts, HTTP request deadlines, and graceful shutdown.

**Fix**: call `st.conn.SetInterrupt(ctx.Done())` before invoking `fn`, and
reset it after.

---

## Medium-Severity Issues / Footguns

### 8. `View` has no write enforcement

**Location**: `s3db.go:211-227`

The docstring says "Don't write in View" but nothing prevents it. Writes in
`View` mutate the local SQLite file, are **not** captured in a changeset,
**not** uploaded, and **persist locally** until the next full-refresh —
which may never happen if compaction never runs.

This is silent, hard-to-debug state divergence: "my local query sees rows
that don't exist on a fresh Open."

**Fix options**:
- Wrap the `fn` call in `BEGIN; ... ROLLBACK;` (cheap, correct)
- Set `PRAGMA query_only=ON` before `fn`, reset after
- Create a session and return an error if it captured changes (catch-after-the-fact)

---

### 9. No backoff/jitter on CAS retry; orphan accumulation under contention

**Location**: `commit.go:128`

Under heavy contention, all writers retry immediately after a 412 — a
thundering-herd CAS storm where one writer wins and N-1 lose on every round.

Each failed rebase (`needCapture = true`) uploads a **new** changeset blob
with a new UUID. The previous one becomes an orphan. With `maxRetries=20`
and 10 concurrent writers on a hot row, worst case is ~180 orphan blobs per
successful write, all sitting in S3 until GC+compact runs.

This is documented behavior (`DESIGN.md:104`) but the interaction with
missing backoff makes it worse than the design anticipates.

**Fix**: exponential backoff with jitter on CAS retry. Optionally, on
`needCapture=true` retry, `Delete` the previous orphan blob before uploading
the new one (best-effort).

---

### 10. `WithLocalPath` doesn't cache the snapshot across `Open` calls

**Location**: `s3db.go:93`

`Open` **always** calls `downloadSnapshot`, overwriting whatever is at
`localPath`. If a user expects "set `WithLocalPath`, close, reopen later,
skip download" — that doesn't work.

This is fine for the documented Lambda pattern (`Open` once in `init()`,
never `Close`), but the option's semantics are unclear. The docstring says
"survives Lambda warm starts" which is misleading — it's the **open DB
handle** that survives warm starts, not the file.

**Fix**: either implement file-reuse (check if the file is a valid SQLite DB
at a known seq, skip download if so), or document that `WithLocalPath` only
controls where the file lives during one Open→Close cycle.

---

### 11. No fsync before rename

**Location**: `sync.go:60-63`

```go
if err := tmp.Close(); err != nil { return err }
return os.Rename(tmpPath, destPath)
```

No `tmp.Sync()` before `Close`. If the kernel crashes between rename and
page flush, the file is corrupt/truncated. S3 is the durable store so this
is "recoverable" — but the next `Open` would fail with a corrupted-database
error, and for `WithLocalPath` users the bad file persists until manually
deleted (Open won't delete it).

**Fix**: add `tmp.Sync()` before `tmp.Close()`. For extra safety, also
`fsync` the parent directory after `Rename` (standard POSIX durability
practice).

---

### 12. Rebase correctness relies on undocumented invariant

**Location**: `commit.go:194-226`

After a clean rebase, `conn` has `base + fn_changes + their_changes`.
The committed log order produces `base + their_changes + fn_changes`.

These are equivalent **only** when row sets are truly disjoint. The current
logic is correct — SQLite changeset before-images check the full row, so
same-row-different-column updates are correctly detected as conflicts — but:

- There is no test that explicitly validates `conn`-state-after-rebase ==
  fresh-replay-state. `chaos_test.go` re-`Open`s before verification, so it
  would not catch local-vs-replay divergence.
- If zombiezen/sqlite ever adopts indirect-mode sessions or partial-row
  before-images, this invariant silently breaks.

**Recommendation**: add an explicit test that clones the rebased conn state,
replays the log from scratch on a fresh conn, and diffs the two row-by-row.

---

## Minor Issues / Dead Code

### 13. `ErrNotInitialized` is declared but never returned

**Location**: `errors.go:27-29`

Docstring says "when Open finds no manifest and auto-initialization is
disabled" — but there is no option to disable auto-initialization.
`bootstrap` always runs. Dead code or unimplemented feature.

### 14. `BlobStore.Head` is only used by tests

**Location**: `blob.go:34-36`

Every call to `.Head(` is in a `*_test.go` file. The method is part of the
public `BlobStore` interface but the library itself never uses it. Interface
bloat — forces all implementors to write a method nobody calls.

### 15. Stale "Stage 6" comment

**Location**: `commit.go:15-16`

> "The DB struct (Stage 6) will embed this; for now it's passed explicitly"

Stage 6 clearly happened — `DB` embeds `commitState`. Stale development
note.

### 16. Duplicated open flags

**Location**: `commit.go:57` vs `s3db.go:341-343`

`syncToManifest` hardcodes `sqlite.OpenReadWrite` instead of calling the
`openLocalConn` helper. If flags ever need to change (add `OpenNoMutex`,
`OpenURI`, etc.), these drift.

### 17. `WithMaxRetries(0)` → zero attempts

**Location**: `commit.go:128`, `options.go:30-32`

`for attempt := 0; attempt < cfg.maxRetries` means `WithMaxRetries(0)`
gives immediate `ErrConflict` without attempting a single CAS. Probably not
what anyone wants; no validation.

### 18. Manifest JSON has no format-version field

**Location**: `manifest.go:21-37`

If `manifest.json` wire format needs to evolve (new fields, changed
semantics), there's no way to detect old vs. new format. `SchemaVersion`
tracks the **user's** SQL schema, not the manifest's encoding.

### 19. `PutCondition` doesn't reject both-fields-set

**Location**: `blob.go:57-67`

Comment says "At most one of IfMatch or IfNoneMatch should be set" — no
enforcement. S3 behavior with both headers is undefined and may vary by
backend (MinIO, R2, etc.).

### 20. Two `DB` instances on same `WithLocalPath` collide

**Location**: `s3db.go:58-155`

No lock file, no advisory flock. Two processes (or two `Open` calls in one
process) with the same `localPath` will race on `downloadSnapshot` and
produce undefined results. Not documented as a constraint.

### 21. `etag.Compute` docstring overclaims

**Location**: `internal/etag/etag.go:20-26`

> "for predicting what S3 will return"

Only true for single-part uploads of **unencrypted** objects. Buckets with
SSE-KMS or SSE-S3 return non-MD5 ETags. This is harmless (the function is
only used by `MemBlobStore`), but the comment is misleading.

### 22. Test file has leftover debugging comments

**Location**: `migrate_test.go:368-373`

```go
// Open succeeds (no migrations to check), but...
// Actually Open should fail because runMigrations checks schema_version
// against maxVer=0 and the DB is at 3 → ErrSchemaTooNew.
// Wait, let me check the logic... runMigrations is always called, and
// with no migrations maxVer=0, manifest.SchemaVersion=3 > 0 → error.
// So db2 should be nil.
```

Stream-of-consciousness notes that were never cleaned up. The test itself
is correct but the comments are confusing.

---

## Test Coverage Gaps

- **No test for migration vs. concurrent writer** — the exact scenario that
  triggers Critical bug #1.
- **No test verifying local conn == replay result** after a clean rebase —
  would catch any future divergence in issue #12.
- **No test for View-writes-don't-persist** — nothing checks that writes
  inside `View` are discarded (they aren't — issue #8).
- **No test for `applyLog` partial failure** — issue #2.
- **No test for `WithMaxRetries(0)`** — issue #17.
- **Chaos test doesn't inject post-upload failures** — `faultyStore` fails
  *before* calling `inner`, never simulating "PUT succeeded but response
  was lost" (the most dangerous failure mode for the manifest CAS).

---

## Summary Table

| # | Issue | Severity | File:Line |
|---|---|---|---|
| 1 | Migration retry with dirty local state | **Critical** | `migrate.go:78-84` |
| 2 | `applyLog` partial failure loses `localSeq` | **Critical** | `commit.go:66-70` |
| 3 | Cross-device `os.Rename` failure | **Critical** | `sync.go:43,63` |
| 4 | GC snapshot TOCTOU race | **High** | `gc.go:78-96` |
| 5 | Auto-compact errors swallowed + lying docstring | **High** | `s3db.go:260`, `options.go:35` |
| 6 | `DeleteObjects` ignores per-key errors | **High** | `blob_s3.go:155-164` |
| 7 | No SQLite context cancellation | **High** | `s3db.go`, `commit.go` |
| 8 | `View` allows silent writes | **Medium** | `s3db.go:219-227` |
| 9 | No CAS backoff; orphan accumulation | **Medium** | `commit.go:128` |
| 10 | `WithLocalPath` doesn't cache across Opens | **Medium** | `s3db.go:93` |
| 11 | No fsync before rename | **Medium** | `sync.go:60-63` |
| 12 | Rebase ordering invariant untested | **Medium** | `commit.go:194-226` |
| 13 | `ErrNotInitialized` dead code | Low | `errors.go:29` |
| 14 | `BlobStore.Head` unused by library | Low | `blob.go:36` |
| 15 | Stale "Stage 6" comment | Low | `commit.go:15-16` |
| 16 | Duplicated open flags | Low | `commit.go:57` |
| 17 | `WithMaxRetries(0)` = zero attempts | Low | `commit.go:128` |
| 18 | No manifest format version | Low | `manifest.go:21` |
| 19 | `PutCondition` both-set unchecked | Low | `blob.go:57-67` |
| 20 | No guard against shared `WithLocalPath` | Low | `s3db.go:58` |
| 21 | `etag.Compute` docstring overclaims | Low | `internal/etag/etag.go:20` |
| 22 | Leftover debug comments in test | Low | `migrate_test.go:368-373` |
