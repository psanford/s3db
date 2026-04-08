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

## Important contract: `Update`'s closure may run multiple times

If a concurrent writer causes a rebase conflict, `Update` re-runs your function on the refreshed state. **Don't do anything with external side effects inside the closure** — no emails, no API calls, no logging that can't be repeated. Same contract as a Postgres serializable transaction retry loop.

## Cost

At rest: S3 storage only (~$0.023/GB-month). No idle compute.

Per write: ~2 GETs (manifest + sync) + 2 PUTs (changeset + manifest) ≈ $0.00001. A million writes costs roughly $10.

Per read: 1 GET (manifest) + 0–N GETs (new changesets since last sync). Warm Lambda with no intervening writes: just the manifest GET.

## Testing

```bash
go test ./...                    # unit tests (against in-memory fake store)
go test ./... -race              # with race detector
go test ./... -run Chaos         # fault injection soak tests

# Integration tests against real S3 or MinIO:
S3DB_TEST_BUCKET=my-bucket go test -tags integration ./...
```

## Requirements

- **S3 conditional writes** — `If-Match` and `If-None-Match` on `PutObject`. Generally available since late 2024. MinIO, Cloudflare R2, and most S3-compatible stores support it.
- **Tables must have explicit PRIMARY KEYs** — the SQLite session extension requires it. Rowid-only tables won't replicate.
