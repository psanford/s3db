package s3db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/psanford/s3db/internal/etag"
)

// S3BlobStore implements BlobStore against AWS S3 (or S3-compatible services
// like MinIO). It requires S3 conditional writes (If-Match, If-None-Match on
// PutObject), which became generally available in late 2024.
type S3BlobStore struct {
	client *s3.Client
	bucket string
}

// NewS3BlobStore wraps an s3.Client and bucket name. The client should be
// constructed with whatever config/credentials loader is appropriate for
// your environment — this package doesn't impose one.
//
// Example:
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	client := s3.NewFromConfig(cfg)
//	store := s3db.NewS3BlobStore(client, "my-bucket")
//	db, _ := s3db.Open(ctx, store, "mydb/")
func NewS3BlobStore(client *s3.Client, bucket string) *S3BlobStore {
	return &S3BlobStore{client: client, bucket: bucket}
}

func (s *S3BlobStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	return out.Body, etag.Normalize(aws.ToString(out.ETag)), nil
}

func (s *S3BlobStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return out.Body, nil
}

func (s *S3BlobStore) Stat(ctx context.Context, key string) (BlobInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return BlobInfo{}, ErrNotFound
		}
		return BlobInfo{}, err
	}
	var mod time.Time
	if out.LastModified != nil {
		mod = *out.LastModified
	}
	return BlobInfo{
		ETag:         etag.Normalize(aws.ToString(out.ETag)),
		Size:         aws.ToInt64(out.ContentLength),
		LastModified: mod,
	}, nil
}

func (s *S3BlobStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if cond.IfMatch != "" && cond.IfNoneMatch {
		return "", errBothConditions
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if cond.IfMatch != "" {
		// S3 expects the quoted form. Normalize then re-quote for
		// consistency regardless of what the caller passed.
		in.IfMatch = aws.String(`"` + etag.Normalize(cond.IfMatch) + `"`)
	}
	if cond.IfNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}

	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrPreconditionFailed
		}
		return "", err
	}
	return etag.Normalize(aws.ToString(out.ETag)), nil
}

func (s *S3BlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

func (s *S3BlobStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	// S3 returns success for deleting a nonexistent key, which matches
	// our interface contract. No special handling needed.
	return err
}

func (s *S3BlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	// DeleteObjects batches up to 1000 keys per request.
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		objs := make([]types.ObjectIdentifier, end-i)
		for j, k := range keys[i:end] {
			objs[j] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: objs,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return err
		}
		// With Quiet: true, only per-object failures appear in Errors.
		// Object lock, MFA-delete, or permission issues can cause
		// individual keys to fail while the overall request succeeds.
		if len(out.Errors) > 0 {
			e := out.Errors[0]
			return fmt.Errorf("delete %s: %s: %s (%d keys failed)",
				aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message), len(out.Errors))
		}
	}
	return nil
}

// isNotFound reports whether err represents a 404 / NoSuchKey / NotFound.
// S3 is inconsistent about which error type it returns across operations
// (GetObject vs HeadObject) and SDK versions, so we check several ways.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// Fallback: check HTTP status.
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return true
	}
	return false
}

// isPreconditionFailed reports whether err is a 412 / PreconditionFailed,
// meaning a conditional write (If-Match or If-None-Match) was not satisfied.
func isPreconditionFailed(err error) bool {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 412 {
		return true
	}
	// Some S3-compatible backends return 409 for If-None-Match conflicts.
	if errors.As(err, &re) && re.HTTPStatusCode() == 409 {
		return true
	}
	return false
}

// Compile-time check.
var _ BlobStore = (*S3BlobStore)(nil)
