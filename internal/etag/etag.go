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

// Compute returns a hex-encoded MD5 of body. This is the ETag that S3
// returns for single-part, unencrypted uploads — but NOT for multipart
// uploads (MD5-of-MD5s with part-count suffix) or server-side encrypted
// objects (opaque). In this library it is only used by MemBlobStore to
// generate deterministic ETags for testing; production code never
// predicts S3's ETag, it just echoes what S3 returns.
func Compute(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}
