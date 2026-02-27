// Package main demonstrates using s3db with the in-memory blob store —
// useful for local development, unit tests, and exploring the library
// without AWS credentials.
//
// The in-memory store has the same concurrency semantics as real S3
// (atomic PUT, strong consistency, ETag CAS), so correctness properties
// verified against it hold against S3 too.
//
// This example spawns several concurrent "writers" that all increment a
// counter, then verifies the final value is exactly right — demonstrating
// that the CAS loop prevents lost updates under contention.
//
// Run it:
//
//	go run ./examples/inmemory
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/psanford/s3db"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var migrations = []s3db.Migration{
	{Version: 1, Name: "init", Up: func(c *sqlite.Conn) error {
		return sqlitex.ExecuteScript(c, `
			CREATE TABLE counters (
				name TEXT PRIMARY KEY,
				value INTEGER NOT NULL DEFAULT 0
			);
			INSERT INTO counters (name, value) VALUES ('hits', 0);
		`, nil)
	}},
}

func main() {
	ctx := context.Background()

	// MemBlobStore is exported specifically for tests and local dev.
	// It's a simple map[string][]byte with real ETag-based CAS.
	store := s3db.NewMemBlobStore()

	const workers = 8
	const incrementsPerWorker = 10
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each worker opens its OWN *s3db.DB — simulating separate
			// Lambda invocations. They share the store (same "S3 bucket")
			// but have independent local SQLite files.
			db, err := s3db.Open(ctx, store, "demo/",
				s3db.WithMigrations(migrations),
				s3db.WithMaxRetries(50), // high — we expect contention
			)
			if err != nil {
				log.Printf("worker %d open: %v", id, err)
				return
			}
			defer db.Close()

			for j := 0; j < incrementsPerWorker; j++ {
				err := db.Update(ctx, func(c *sqlite.Conn) error {
					return sqlitex.Execute(c,
						`UPDATE counters SET value = value + 1 WHERE name = 'hits'`,
						nil)
				})
				if err != nil {
					log.Printf("worker %d update %d: %v", id, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Verify the final count using a fresh reader.
	verifier, err := s3db.Open(ctx, store, "demo/", s3db.WithMigrations(migrations))
	if err != nil {
		log.Fatalf("open verifier: %v", err)
	}
	defer verifier.Close()

	var final int64
	verifier.View(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `SELECT value FROM counters WHERE name = 'hits'`,
			&sqlitex.ExecOptions{
				ResultFunc: func(s *sqlite.Stmt) error {
					final = s.ColumnInt64(0)
					return nil
				},
			})
	})

	expected := int64(workers * incrementsPerWorker)
	fmt.Printf("Final counter: %d (expected %d)\n", final, expected)
	fmt.Printf("Database at seq: %d\n", verifier.Seq())

	if final != expected {
		log.Fatalf("LOST UPDATES: got %d, want %d", final, expected)
	}
	fmt.Println("✓ No lost updates under concurrent contention.")
}
