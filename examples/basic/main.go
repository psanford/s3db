// Package main demonstrates the core s3db operations: migrations, writes,
// reads, compaction, and GC. This is the happy-path tour of the API.
//
// Run it against real S3:
//
//	AWS_REGION=us-east-1 go run ./examples/basic -bucket my-bucket -prefix demo/
//
// Or against MinIO:
//
//	docker run -d -p 9000:9000 minio/minio server /data
//	AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin \
//	  AWS_SECRET_ACCESS_KEY=minioadmin \
//	  go run ./examples/basic -bucket test -prefix demo/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/psanford/s3db"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var migrations = []s3db.Migration{
	{
		Version: 1,
		Name:    "create_tasks",
		Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `
				CREATE TABLE tasks (
					id INTEGER PRIMARY KEY,
					title TEXT NOT NULL,
					done INTEGER NOT NULL DEFAULT 0,
					created_at INTEGER NOT NULL
				);
			`, nil)
		},
	},
	{
		Version: 2,
		Name:    "add_priority",
		Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `
				ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
				CREATE INDEX tasks_priority ON tasks(priority);
			`, nil)
		},
	},
}

func main() {
	bucket := flag.String("bucket", "", "S3 bucket name (required)")
	prefix := flag.String("prefix", "s3db-demo/", "key prefix (must end with /)")
	flag.Parse()

	if *bucket == "" {
		log.Fatal("missing -bucket")
	}

	ctx := context.Background()
	store := s3db.NewS3BlobStore(newS3Client(), *bucket)

	// Open. Bootstraps the manifest + initial snapshot if the prefix is
	// empty, then applies any pending migrations. Safe under concurrent
	// callers — exactly one bootstrap/migration wins the CAS, others
	// sync to its result.
	db, err := s3db.Open(ctx, store, *prefix,
		s3db.WithMigrations(migrations),
		s3db.WithAutoCompact(10), // compact when log reaches 10 changesets
	)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	fmt.Printf("Opened database at seq=%d\n", db.Seq())

	// Write. The closure may run more than once if a concurrent writer
	// forces a rebase conflict — don't put external side effects here.
	err = db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `
			INSERT INTO tasks (title, priority, created_at)
			VALUES (?, ?, unixepoch())
		`, &sqlitex.ExecOptions{
			Args: []any{"Write a demo example", 1},
		})
	})
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("Inserted task, now at seq=%d\n", db.Seq())

	// Read.
	err = db.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `
			SELECT id, title, priority, done FROM tasks ORDER BY priority DESC, id
		`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				fmt.Printf("  task #%d: %q (prio=%d, done=%v)\n",
					stmt.ColumnInt64(0),
					stmt.ColumnText(1),
					stmt.ColumnInt64(2),
					stmt.ColumnInt64(3) != 0,
				)
				return nil
			},
		})
	})
	if err != nil {
		log.Fatalf("view: %v", err)
	}

	// Maintenance operations. Usually run from a separate scheduled
	// Lambda (cron), not inline with user requests.
	if err := db.Compact(ctx); err != nil {
		log.Printf("compact: %v (non-fatal)", err)
	}
	if err := db.GC(ctx); err != nil {
		log.Printf("gc: %v (non-fatal)", err)
	}
}

// newS3Client constructs an S3 client from environment variables. Uses
// options-style configuration to avoid requiring the aws-sdk-go-v2/config
// package (which pulls in a large dependency tree). Real applications will
// typically use config.LoadDefaultConfig instead.
func newS3Client() *awss3.Client {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	return awss3.New(awss3.Options{}, func(o *awss3.Options) {
		o.Region = region
		if ep := os.Getenv("AWS_ENDPOINT_URL"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true // needed for MinIO and most self-hosted S3
		}
		if ak := os.Getenv("AWS_ACCESS_KEY_ID"); ak != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				ak,
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				os.Getenv("AWS_SESSION_TOKEN"),
			)
		}
	})
}
