package s3db

import "testing"

func TestMemBlobStore(t *testing.T) {
	runBlobStoreConformanceTests(t, func(t *testing.T) BlobStore {
		return NewMemBlobStore()
	})
}
