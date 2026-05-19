# S3-Backed SQLite Database for Lambda

## session/changeset extension

A **changeset** is a compact binary blob recording the before/after state of every row touched by a set of INSERTs/UPDATEs/DELETEs. It can be:

- **Applied** to another database (replays the changes)
- **Inverted** (undo)
- **Concatenated** (merge multiple changesets)

Applying a changeset to a database whose rows have diverged invokes a **conflict handler** with the specific mismatch (before-image doesn't match, row already exists, constraint violation, etc.). Requires tables to have explicit PRIMARY KEYs.

---

## Design

### Storage layout

```
s3://bucket/mydb/
  manifest.json                            ← single source of truth; ETag is the DB version
  snapshots/snap-<id>.sqlite               ← immutable full-database files
  changesets/<epoch>/cs-<uuid>.bin         ← immutable changeset blobs, grouped by origin snapshot
```

**Manifest** (the only contended file — everything else is write-once to a unique key):

```json
{
  "seq": 149,
  "snapshot": {
    "key": "snapshots/snap-147.sqlite",
    "seq": 147
  },
  "log": [
    { "key": "changesets/snap-147/cs-ghi.bin", "seq": 148 },
    { "key": "changesets/snap-147/cs-jkl.bin", "seq": 149 }
  ]
}
```

- `seq` is a monotonically increasing logical version, assigned at manifest-commit time (never has gaps)
- `snapshot.seq` is the highest sequence number included in the snapshot
- `log` lists changesets to apply on top of the snapshot, in order, to reach `seq`
- The current **epoch** is derived from `snapshot.key` — changesets are written under `changesets/<snapshot-basename>/`

### Reconstructing current state

```
current_state = load(snapshot.key)
for entry in log:
    apply_changeset(current_state, load(entry.key))
```

Readers can cache the reconstructed database in Lambda's `/tmp` between invocations. On warm start, fetch the manifest and apply only changesets not already applied locally.

---

## Protocols

### Read

1. `GET manifest.json`
2. If cached local DB is at `seq` ≥ `manifest.seq` → done
3. If cached local DB is at `seq` ≥ `manifest.snapshot.seq` → fetch and apply only missing log entries
4. Otherwise → fetch snapshot, apply full log
5. Query locally

### Write

1. `GET manifest.json` → body + ETag `E`
2. Ensure local DB is current (per read protocol)
3. Open session, run transaction, capture changeset `C`
4. `epoch = basename(manifest.snapshot.key)`
5. `PUT changesets/{epoch}/cs-{uuid}.bin` (no precondition — unique key, never contends)
6. Build new manifest: append `{key, seq: manifest.seq + 1}` to `log`, bump top-level `seq`
7. `PUT manifest.json` with `If-Match: E`
   - **200** → committed
   - **412** → concurrent writer won. Retry:
     - `GET manifest.json` (new ETag `E'`)
     - Fetch and apply any changesets in the new log that we're missing
     - Attempt `sqlite3changeset_apply(C)` on the refreshed DB with a **strict conflict handler** (abort on any DATA conflict)
       - **Clean apply** → goto step 6 with new manifest (blob already uploaded, no re-upload)
       - **Conflict** → discard `C`, re-run transaction from step 3 (the original blob becomes an orphan, cleaned by GC)
   - After N failed retries → fail up to application with a contention error

### Compaction

Triggered when `len(log)` exceeds a threshold, or on a schedule.

1. `GET manifest.json` → ETag `E`, `seq = N`
2. Reconstruct full DB (snapshot + log)
3. `VACUUM` (optional but recommended — defragment, shrink)
4. `PUT snapshots/snap-{N}.sqlite` with `If-None-Match: *` (uncontended — unique key)
5. Build new manifest: `{ seq: N, snapshot: {key: snap-{N}, seq: N}, log: [] }`
6. `PUT manifest.json` with `If-Match: E`
   - **200** → done; a new epoch has opened
   - **412** → a writer added seq `N+1` during compaction. Either:
     - **Retry**: re-read manifest, apply new changesets, rebuild snapshot including them, retry CAS. Converges under light load.
     - **Abort**: try again later. No harm done — the snapshot blob at step 4 is an orphan, GC'd eventually.

Compaction never blocks writers and never loses work.

### Garbage collection

An epoch `changesets/<epoch>/` is garbage when every changeset in it has `seq` ≤ `manifest.snapshot.seq` (i.e., it has been fully rolled into a snapshot).

Algorithm:
1. `GET manifest.json`
2. `LIST` prefixes under `changesets/`
3. For each epoch prefix ≠ current epoch:
   - If it contains no keys referenced by `manifest.log` → delete entire prefix
4. `LIST snapshots/`, delete any snapshot whose key ≠ `manifest.snapshot.key` and which is older than a grace period (to allow in-flight readers to finish)

In practice an epoch becomes deletable one or two compaction cycles after it closes (two if a straggler write landed in it during compaction). GC can run as a post-compaction step or as an independent scheduled sweep.

**Orphan handling is automatic.** Crashed writers leave unreferenced blobs in whichever epoch was current; stale writers (who lost the CAS after an epoch swap) leave them in the old epoch. Both are swept by the epoch prefix delete — no separate "find unreferenced blobs" scan is needed.

---

## Conflict semantics

The changeset rebase in the write retry path is **only safe with abort-on-conflict**. A changeset records *physical* before/after row states, not *logical intent* — "change balance from 100 to 110" is not the same as "increment balance by 10."

| Conflict handler policy | Behavior | Safe? |
|---|---|---|
| `ABORT` on any DATA conflict | Falls back to full transaction re-execution | **Yes** — equivalent to serializable isolation |
| `REPLACE` (last writer wins) | Overwrites the other writer's change with our stale computed value | Only if the column is register-like (latest value is all that matters) |
| `OMIT` | Silently drops our change to the conflicting row | Rarely desirable |

**Default policy: strict abort.** The changeset machinery then buys you:
- Free merge when concurrent writes touch disjoint rows (the common case)
- Cheap conflict *detection* when they don't, followed by a correct re-execution

This is serializable isolation with optimistic concurrency control.
