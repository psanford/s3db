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
