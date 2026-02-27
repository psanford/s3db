// Package etag provides ETag computation and normalization.
//
// S3 returns ETags wrapped in double quotes (per RFC 7232). We normalize
// them to unquoted strings internally so comparisons work regardless of
// which layer produced the value.
package etag

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
)

// Normalize strips surrounding double quotes from an ETag.
// Idempotent: safe to call on already-unquoted values.
func Normalize(s string) string {
	return strings.Trim(s, `"`)
}

// Compute returns the ETag for a body the way S3 does for single-part
// uploads: hex-encoded MD5.
//
// Note: S3 multipart uploads use a different scheme (MD5-of-MD5s with a part
// count suffix). This library never does multipart uploads, so simple MD5 is
// correct and sufficient for both the in-memory fake and for predicting what
// S3 will return.
func Compute(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}
