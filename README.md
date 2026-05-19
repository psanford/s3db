# s3db

A concurrency-safe SQLite database backed by S3, designed for AWS Lambda and other serverless environments where you want a real relational database without an always-on server.

## Overview

The goal of s3db is to have a very simple, cheap read/write database backed by just S3. The goal is not to be infinitely scalable, I would not use this for storing more than 100MB of data; I also would not use it for databases with lots of concurrent writers.

I use this for a lot of little lambda functions that are mostly idle but occasionally need to read or write a little bit of data to a database.


## How it works

We depend on a few things beyond plain sqlite to make this work:
- The [sqlite session extension](https://www.sqlite.org/sessionintro.html)
- S3 Conditional writes for atomic updates
- A simple metadata file and s3 directory structure

You can think of the sqlite [session extension](https://www.sqlite.org/sessionintro.html) as a diff+patch mechanism for sqlite databases. It allows you to create small changesets of changes from a base database, and ship around just those patch sets to apply. One of the example use cases for it is an application that allows for offline changes to the database that are then synced to a central db when the system comes back online. In our case, we use it to track changes to the sqlite db in s3 without having to push a full copy of that database back to s3 on every change. We store the base db and then a series of changesets on top of it in a directory. After a certain number of changes have collected we can do a compaction to generate a new base sqlite db in s3.

We then store a small json manifest file. This file points to the current base snapshot of the db along with the list of changesets to be applied on top. All locking occurs against this file, so two concurrent writers will not be able to stomp on each other. This also allows clients to quickly check for changes without needing to download large files or iterate over s3 prefixes.

Clients can cache the sqlite db locally, either in memory or to disk.


## Some more implementation details

Because we are using the sqlite session extension, we need to use a sqlite driver that supports it. That also means we can't just use a plain database/sql API, because we need to handle writes failing from a write conflict. The API is pretty straight forward, it is just a little different:

```
// open a database
s3db.Open()

// Attempt an update (might error on concurrent writer conflict)
// Will retry based on WithMaxRetries() before returning an ErrConflict
db.Update()

// Perform a read-only operation on the database
db.View()
```

One other important note: **Tables must have explicit PRIMARY KEYs** — the SQLite session extension requires it. Rowid-only tables won't replicate.

### Migrations / Schema changes

Unfortunately, the session extension does not have any support for tracking the schema version across changesets. This means if you are using the session extension you need some other system for handling schema changes.

We provide a basic mechanism for this, when opening a database you can provide a slice of `s3db.Migration` objects, for how the database should be setup and any additional migrations that need to be run. The schema version is tracked in the manifest.json file. Writers need to track the same schema changes or you will have a bad time. Clients will error if they attempt to open a db with a newer schema. Set `WithSchemaUnchecked` if you do not want to have s3db manage the schema.


## Example usage

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

## Important contract: `Update`'s closure may run multiple times

If a concurrent writer causes a rebase conflict, `Update` re-runs your function on the refreshed state. **Don't do anything with external side effects inside the closure** — no emails, no API calls, no logging that can't be repeated. Same contract as a Postgres serializable transaction retry loop.

## License

MIT
