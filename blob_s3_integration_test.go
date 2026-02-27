//go:build integration

// Integration tests for S3BlobStore. Run with:
//
//	S3DB_TEST_BUCKET=my-test-bucket go test -tags integration ./...
//
// Or against MinIO:
//
//	docker run -d -p 9000:9000 minio/minio server /data
//	AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin \
//	  AWS_SECRET_ACCESS_KEY=minioadmin S3DB_TEST_BUCKET=test \
//	  go test -tags integration ./...
//
// The bucket must exist and be writable. Tests use a unique prefix per run
// and clean up after themselves, but they are not safe to run concurrently
// against the same bucket prefix.

package s3db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// integrationS3Client builds an S3 client from environment variables.
// Returns nil if S3DB_TEST_BUCKET is not set (tests will skip).
func integrationS3Client(t *testing.T) (*s3.Client, string) {
	t.Helper()
	bucket := os.Getenv("S3DB_TEST_BUCKET")
	if bucket == "" {
		t.Skip("S3DB_TEST_BUCKET not set; skipping integration test")
	}

	// Build config manually rather than using config.LoadDefaultConfig
	// so this file doesn't require the config package (which hit an
	// artifactory mirror lag during development).
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = region
			if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
				o.BaseEndpoint = aws.String(endpoint)
				o.UsePathStyle = true // MinIO and most self-hosted need this
			}
			if ak := os.Getenv("AWS_ACCESS_KEY_ID"); ak != "" {
				o.Credentials = credentials.NewStaticCredentialsProvider(
					ak,
					os.Getenv("AWS_SECRET_ACCESS_KEY"),
					os.Getenv("AWS_SESSION_TOKEN"),
				)
			}
		},
	}

	return s3.New(s3.Options{}, opts...), bucket
}

// integrationStore returns an S3BlobStore and a unique test prefix. Cleanup
// is registered to delete everything under the prefix.
func integrationStore(t *testing.T) (*S3BlobStore, string) {
	t.Helper()
	client, bucket := integrationS3Client(t)
	store := NewS3BlobStore(client, bucket)
	prefix := fmt.Sprintf("s3db-test/%d-%s/", time.Now().UnixNano(), t.Name())
	t.Cleanup(func() {
		_ = store.DeletePrefix(context.Background(), prefix)
	})
	return store, prefix
}

// --- BlobStore contract tests ----------------------------------------------
// These re-run the core MemBlobStore test scenarios against real S3 to
// verify our error translation and ETag handling are correct.

func TestS3BlobStore_PutGetRoundtrip(t *testing.T) {
	store, prefix := integrationStore(t)

	etag := putString(t, store, prefix+"key", "hello s3", NoCondition)
	if etag == "" {
		t.Fatal("empty etag from Put")
	}

	body, gotEtag := getString(t, store, prefix+"key")
	if body != "hello s3" {
		t.Errorf("body = %q, want %q", body, "hello s3")
	}
	if gotEtag != etag {
		t.Errorf("Get etag = %q, Put etag = %q", gotEtag, etag)
	}
}

func TestS3BlobStore_NotFound(t *testing.T) {
	store, prefix := integrationStore(t)
	ctx := context.Background()

	_, _, err := store.Get(ctx, prefix+"nonexistent")
	if err != ErrNotFound {
		t.Errorf("Get: expected ErrNotFound, got %v", err)
	}

	_, err = store.Head(ctx, prefix+"nonexistent")
	if err != ErrNotFound {
		t.Errorf("Head: expected ErrNotFound, got %v", err)
	}
}

func TestS3BlobStore_IfMatchCAS(t *testing.T) {
	store, prefix := integrationStore(t)
	ctx := context.Background()
	key := prefix + "cas-key"

	etag1 := putString(t, store, key, "v1", NoCondition)

	// Correct etag succeeds.
	etag2 := putString(t, store, key, "v2", PutCondition{IfMatch: etag1})
	if etag2 == etag1 {
		t.Error("etag should change on successful CAS")
	}

	// Stale etag fails.
	_, err := store.Put(ctx, key, strings.NewReader("v3"), PutCondition{IfMatch: etag1})
	if err != ErrPreconditionFailed {
		t.Errorf("stale CAS: expected ErrPreconditionFailed, got %v", err)
	}

	body, _ := getString(t, store, key)
	if body != "v2" {
		t.Errorf("body = %q, want v2 (stale CAS should not write)", body)
	}
}

func TestS3BlobStore_IfNoneMatch(t *testing.T) {
	store, prefix := integrationStore(t)
	ctx := context.Background()
	key := prefix + "once-key"

	// First write succeeds.
	_, err := store.Put(ctx, key, strings.NewReader("first"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("first IfNoneMatch: %v", err)
	}

	// Second fails.
	_, err = store.Put(ctx, key, strings.NewReader("second"), PutCondition{IfNoneMatch: true})
	if err != ErrPreconditionFailed {
		t.Errorf("second IfNoneMatch: expected ErrPreconditionFailed, got %v", err)
	}
}

func TestS3BlobStore_ListDeletePrefix(t *testing.T) {
	store, prefix := integrationStore(t)
	ctx := context.Background()

	putString(t, store, prefix+"a/1", "x", NoCondition)
	putString(t, store, prefix+"a/2", "x", NoCondition)
	putString(t, store, prefix+"b/1", "x", NoCondition)

	keys, err := store.List(ctx, prefix+"a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("List returned %d keys, want 2: %v", len(keys), keys)
	}

	if err := store.DeletePrefix(ctx, prefix+"a/"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}

	keys, _ = store.List(ctx, prefix)
	if len(keys) != 1 || keys[0] != prefix+"b/1" {
		t.Errorf("after DeletePrefix: keys = %v, want only b/1", keys)
	}
}

// --- End-to-end DB test against S3 -----------------------------------------

func TestS3BlobStore_FullDB(t *testing.T) {
	store, prefix := integrationStore(t)
	ctx := context.Background()

	migs := []Migration{
		{Version: 1, Name: "init", Up: func(c *sqlite.Conn) error {
			return sqlitex.ExecuteScript(c, `
				CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
			`, nil)
		}},
	}

	db, err := Open(ctx, store, prefix, WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write some data.
	for i := 1; i <= 3; i++ {
		err := db.Update(ctx, func(c *sqlite.Conn) error {
			return sqlitex.Execute(c,
				fmt.Sprintf(`INSERT INTO items (id, name, qty) VALUES (%d, 'item-%d', %d)`, i, i, i*10),
				nil)
		})
		if err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
	}

	// Compact.
	if err := db.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// One more write after compaction.
	db.Update(ctx, func(c *sqlite.Conn) error {
		return sqlitex.Execute(c, `INSERT INTO items (id, name, qty) VALUES (4, 'post-compact', 40)`, nil)
	})

	// GC.
	if err := db.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	// Open a second DB and verify state matches.
	db2, err := Open(ctx, store, prefix, WithMigrations(migs))
	if err != nil {
		t.Fatalf("Open db2: %v", err)
	}
	defer db2.Close()

	var count, totalQty int64
	db2.View(ctx, func(c *sqlite.Conn) error {
		sqlitex.Execute(c, `SELECT COUNT(*), SUM(qty) FROM items`, &sqlitex.ExecOptions{
			ResultFunc: func(s *sqlite.Stmt) error {
				count = s.ColumnInt64(0)
				totalQty = s.ColumnInt64(1)
				return nil
			},
		})
		return nil
	})

	if count != 4 {
		t.Errorf("count = %d, want 4", count)
	}
	if totalQty != 100 {
		t.Errorf("totalQty = %d, want 100", totalQty)
	}
}
