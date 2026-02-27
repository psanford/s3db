// Package main shows the recommended pattern for using s3db from AWS Lambda.
//
// Key points:
//
//   - The *s3db.DB is a package-level variable, opened once in init() and
//     reused across warm invocations. The local SQLite file under /tmp
//     survives between warm starts, so subsequent invocations only fetch
//     the manifest + any new changesets — not the full snapshot.
//
//   - Business logic that shouldn't retry (sending emails, calling external
//     APIs) runs OUTSIDE the Update closure. The closure itself only does
//     database work, since it may be re-invoked on rebase conflict.
//
//   - Compaction and GC are handled by a separate scheduled Lambda (not
//     shown here) triggered on a cron — they're too slow to run inline
//     with user requests. Alternatively, use WithAutoCompact for light
//     workloads.
//
// Build for Lambda (pure Go, no CGo toolchain needed):
//
//	GOOS=linux GOARCH=amd64 go build -o bootstrap ./examples/lambda
//	zip lambda.zip bootstrap
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/psanford/s3db"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var db *s3db.DB

var migrations = []s3db.Migration{
	{Version: 1, Name: "init", Up: func(c *sqlite.Conn) error {
		return sqlitex.ExecuteScript(c, `
			CREATE TABLE events (
				id INTEGER PRIMARY KEY,
				source TEXT NOT NULL,
				payload TEXT NOT NULL,
				received_at INTEGER NOT NULL
			);
		`, nil)
	}},
}

func init() {
	// Lambda initializes cold containers once and reuses them for
	// subsequent "warm" invocations. Opening here means warm starts
	// skip the bootstrap/migration/full-snapshot path entirely.
	ctx := context.Background()
	store := s3db.NewS3BlobStore(newS3Client(), os.Getenv("S3DB_BUCKET"))

	var err error
	db, err = s3db.Open(ctx, store, os.Getenv("S3DB_PREFIX"),
		s3db.WithMigrations(migrations),

		// The local SQLite file lives under /tmp, which persists across
		// warm invocations (but not cold starts). On a warm invocation,
		// View/Update will fetch the manifest, see that only a few
		// changesets are new, and apply just those — no snapshot download.
		s3db.WithLocalPath("/tmp/s3db.sqlite"),

		// Default is 10; bump if you expect heavy contention.
		s3db.WithMaxRetries(20),
	)
	if err != nil {
		// init() can't return an error. Real handlers should defer
		// Open to the first invocation or panic here to fail fast.
		panic(fmt.Sprintf("s3db open: %v", err))
	}
}

// Event is whatever your Lambda is triggered by — SNS, SQS, API Gateway,
// etc. Adapt to your trigger type.
type Event struct {
	Source  string `json:"source"`
	Payload string `json:"payload"`
}

type Response struct {
	EventID  int64 `json:"event_id"`
	Duration int64 `json:"duration_ms"`
}

// HandleRequest is the Lambda entry point.
func HandleRequest(ctx context.Context, evt Event) (*Response, error) {
	start := time.Now()

	// Database write. This closure might run more than once under
	// contention, so it contains ONLY database operations. Anything
	// with external side effects happens before or after.
	var eventID int64
	err := db.Update(ctx, func(c *sqlite.Conn) error {
		if err := sqlitex.Execute(c, `
			INSERT INTO events (source, payload, received_at)
			VALUES (?, ?, ?)
		`, &sqlitex.ExecOptions{
			Args: []any{evt.Source, evt.Payload, start.Unix()},
		}); err != nil {
			return err
		}
		eventID = c.LastInsertRowID()
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("record event: %w", err)
	}

	// External side effects happen HERE, outside Update, with the
	// committed eventID in hand. If this fails, the DB write already
	// happened — design your system accordingly (idempotency keys,
	// outbox pattern, etc.).
	notifyDownstream(ctx, eventID)

	return &Response{
		EventID:  eventID,
		Duration: time.Since(start).Milliseconds(),
	}, nil
}

func notifyDownstream(ctx context.Context, eventID int64) {
	// Placeholder: publish to SNS, call another service, etc.
	_ = ctx
	_ = eventID
}

// newS3Client — same as basic example. Real Lambda handlers would use
// config.LoadDefaultConfig(ctx) which picks up IAM role credentials
// automatically.
func newS3Client() *awss3.Client {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	return awss3.New(awss3.Options{}, func(o *awss3.Options) {
		o.Region = region
		if ep := os.Getenv("AWS_ENDPOINT_URL"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
			o.UsePathStyle = true
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

// main would typically be: lambda.Start(HandleRequest)
// Stubbed here so the example compiles without the Lambda runtime dependency.
func main() {
	resp, err := HandleRequest(context.Background(), Event{
		Source:  "test",
		Payload: `{"demo": true}`,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("event_id=%d duration=%dms\n", resp.EventID, resp.Duration)
}
