# s3db

A concurrency-safe SQLite database backed by S3, designed for AWS Lambda and other serverless environments where you want a real relational database without an always-on server.

See [DESIGN.md](./DESIGN.md) for the architecture.

## What it is

- **A SQLite database** — full SQL, transactions, indexes, the works
- **Stored in S3** — no RDS, no idle compute cost, pennies at small scale
- **Safe under concurrency** — multiple Lambda invocations can read and write simultaneously with serializable isolation, no locks, no corruption
- **Pure Go** — no CGo, `GOOS=linux go build` produces a Lambda-ready binary

## What it isn't

- **Not for large data** — sweet spot is KB to low-MB databases. Past ~10MB, snapshot download latency becomes painful.
- **Not for hot-row contention** — if every write hits the same row, throughput degrades to a few writes per second (one per S3 round-trip). Use DynamoDB atomic operations for counters.
- **Not a drop-in for `database/sql`** — uses `zombiezen.com/go/sqlite` directly. If you need `database/sql` compatibility, wrap it yourself.

## Quick start

```go
package main

import (
    "context"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/psanford/s3db"
    "zombiezen.com/go/sqlite"
    "zombiezen.com/go/sqlite/sqlitex"
)

var migrations = []s3db.Migration{
    {Version: 1, Name: "init", Up: func(c *sqlite.Conn) error {
        return sqlitex.ExecuteScript(c, `
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                qty INTEGER NOT NULL DEFAULT 0
            );
        `, nil)
    }},
}

func main() {
    ctx := context.Background()

    cfg, _ := config.LoadDefaultConfig(ctx)
    client := s3.NewFromConfig(cfg)
    store := s3db.NewS3BlobStore(client, "my-bucket")

    db, err := s3db.Open(ctx, store, "mydb/",
        s3db.WithMigrations(migrations),
        s3db.WithLocalPath("/tmp/mydb.sqlite"), // survives Lambda warm starts
        s3db.WithAutoCompact(50),               // compact when log reaches 50
    )
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Write — may retry fn if concurrent writers conflict
    err = db.Update(ctx, func(c *sqlite.Conn) error {
        return sqlitex.Execute(c,
            `INSERT INTO items (name, qty) VALUES ('widget', 10)`, nil)
    })

    // Read
    var total int64
    db.View(ctx, func(c *sqlite.Conn) error {
        return sqlitex.Execute(c, `SELECT SUM(qty) FROM items`,
            &sqlitex.ExecOptions{
                ResultFunc: func(s *sqlite.Stmt) error {
                    total = s.ColumnInt64(0)
                    return nil
                },
            })
    })
}
```

## Lambda deployment

No special build setup — the whole stack is pure Go:

```bash
GOOS=linux GOARCH=amd64 go build -o bootstrap ./cmd/mylambda
zip lambda.zip bootstrap
```

For warm-start caching, store the `*s3db.DB` in a package-level variable and `Open` it once:

```go
var db *s3db.DB

func init() {
    // ... Open ...
}

func HandleRequest(ctx context.Context, evt Event) error {
    return db.Update(ctx, func(c *sqlite.Conn) error {
        // ...
    })
}
```

On warm invocations, `Update`/`View` will sync only the changesets written since the last call — typically one `GET` + a few small changeset fetches.

## How it works (short version)

- One `manifest.json` in S3 is the source of truth — it points at a snapshot and an ordered log of changesets
- Writes: run transaction locally → capture changeset → upload changeset to a unique key → CAS the manifest with `If-Match`
- On CAS conflict: fetch new changesets, try to rebase (apply on top); if clean, retry CAS; if rows conflict, re-run the transaction
- Compaction: roll changesets into a new snapshot, CAS manifest with empty log
- GC: delete unreachable changeset epochs and old snapshots

Serializable isolation. No locks. No corruption under any failure mode.

## Local filesystem backend

`FileSystemBlobStore` implements the same `BlobStore` interface against a local directory. Use it for local development, tests, CLI workflows, or single-machine apps that want the s3db model without S3:

```go
store, err := s3db.NewFileSystemBlobStore("/var/lib/myapp/db")
if err != nil { ... }
defer store.Close()
db, err := s3db.Open(ctx, store, "mydb/")
```

It is safe for concurrent access by **multiple goroutines and multiple processes on the same machine** — conditional writes are serialized with an advisory file lock (`flock(2)` on Unix, `LockFileEx` on Windows) on `<root>/.s3db.lock`, and writes are staged to a temp file and atomically renamed into place. Reads never lock.

Every operation is scoped with [`os.Root`](https://pkg.go.dev/os#Root), so no key — and no symlink someone plants inside the directory — can read or write outside the store root.

### ⚠️ Filesystem support

Advisory file locks only work when **a single OS kernel mediates all access** to the directory. That means:

| | Filesystem | Notes |
|---|---|---|
| ✅ | ext4, xfs, btrfs, zfs, APFS, NTFS, tmpfs | Local disk, any number of processes on one machine |
| ❌ | NFS (v3, v4) | `flock` is silently ignored or local-only depending on mount options. Two clients can both believe they hold the lock and **corrupt the manifest**. |
| ❌ | CIFS / SMB | Advisory locks not reliably honored across clients |
| ❌ | FUSE filesystems (sshfs, s3fs-fuse, …) | Lock support varies; most don't implement it |
| ❌ | Distributed FS (GlusterFS, CephFS, Lustre, GFS2) | `flock` is often a no-op or local-node-only |

**Do not point `FileSystemBlobStore` at a network filesystem.** It will appear to work and then silently lose writes under concurrent access. If you need multi-machine access, use `S3BlobStore` — that is what this library is for.

### Other limitations vs. S3

- **Durability.** `Put` fsyncs the data file before the atomic rename, so a crash never leaves a torn or partial object. The directory entry is *not* fsynced, so a power loss immediately after `Put` may roll back the rename — readers would see the previous version, not garbage. This is weaker than S3's durability guarantee.
- **ETags.** Computed as the MD5 of the content, recomputed on every `Get`/`Stat` by reading the file. Cheap for s3db's KB–MB objects; will show up if you `Stat` large blobs in a tight loop.
- **Crash leftovers.** A process killed mid-`Put` may leave a `.s3db-tmp-*` file behind. These are excluded from `List` and harmless, but accumulate.
- **Key namespace.** Keys map directly to filesystem paths, so a key and a path-prefix of it (e.g. `a` and `a/b`) cannot coexist — the filesystem cannot have a file and a directory at the same path. S3 has a flat namespace and allows both. s3db never generates such keys.
- **Reserved names.** The key `.s3db.lock` and any key whose basename starts with `.s3db-tmp-` are reserved.
- **Permissions.** Directories are created `0700` and files `0600` (owner-only). `chmod` the root if you need broader access.

## Important contract: `Update`'s closure may run multiple times

If a concurrent writer causes a rebase conflict, `Update` re-runs your function on the refreshed state. **Don't do anything with external side effects inside the closure** — no emails, no API calls, no logging that can't be repeated. Same contract as a Postgres serializable transaction retry loop.

## Cost

At rest: S3 storage only (~$0.023/GB-month). No idle compute.

Per write: ~2 GETs (manifest + sync) + 2 PUTs (changeset + manifest) ≈ $0.00001. A million writes costs roughly $10.

Per read: 1 GET (manifest) + 0–N GETs (new changesets since last sync). Warm Lambda with no intervening writes: just the manifest GET.

## Testing

```bash
go test ./...                    # unit tests (in-memory + filesystem stores)
go test ./... -race              # with race detector
go test ./... -run Chaos         # fault injection soak tests

# Integration tests against real S3 or MinIO:
S3DB_TEST_BUCKET=my-bucket go test -tags integration ./...
```

## Requirements

- **S3 conditional writes** — `If-Match` and `If-None-Match` on `PutObject`. Generally available since late 2024. MinIO, Cloudflare R2, and most S3-compatible stores support it.
- **Tables must have explicit PRIMARY KEYs** — the SQLite session extension requires it. Rowid-only tables won't replicate.
