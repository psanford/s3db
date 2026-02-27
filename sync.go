package s3db

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/sync/errgroup"
	"zombiezen.com/go/sqlite"
)

// parallelDownloadThreshold is the snapshot size above which downloadSnapshot
// uses parallel range requests. Below this, a single GET wins: the
// coordination overhead and the fact that RTT dominates for small files
// means parallelism adds latency rather than removing it.
const parallelDownloadThreshold = 4 << 20 // 4 MiB

// parallelDownloadParts is how many concurrent range requests to use when
// parallel download is triggered. Each part is at least threshold/parts
// bytes. More parts means more concurrency but also more per-request
// overhead; 4 is a reasonable default for S3.
const parallelDownloadParts = 4

// fetchConcurrency bounds how many changesets are fetched concurrently in
// applyLog. Changesets are small, so this is mainly about limiting
// in-flight requests. It does NOT bound memory — all fetched changesets
// are held until applied, since apply must be sequential.
const fetchConcurrency = 8

// downloadSnapshot streams a snapshot from the store to a local file at
// destPath. Any existing file at destPath is replaced atomically by writing
// to a temp sibling and renaming — a crash mid-download leaves the old file
// intact (or no file, if there was none).
//
// If size is known (> 0) and above parallelDownloadThreshold, the download
// uses parallel range requests. Otherwise it falls back to a single GET.
// The size hint comes from the manifest (BlobRef.Size); 0 means unknown.
//
// The caller is responsible for closing any SQLite connection on destPath
// before calling this, and reopening after.
func downloadSnapshot(ctx context.Context, store BlobStore, key string, size int64, destPath string) error {
	tmp, err := os.CreateTemp("", "s3db-snap-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if size >= parallelDownloadThreshold {
		err = downloadParallel(ctx, store, key, size, tmp, parallelDownloadParts)
	} else {
		err = downloadSingle(ctx, store, key, tmp)
	}
	if err != nil {
		tmp.Close()
		return fmt.Errorf("download snapshot %s: %w", key, err)
	}

	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, destPath)
}

// downloadSingle does one GET and streams the body to dst.
func downloadSingle(ctx context.Context, store BlobStore, key string, dst io.Writer) error {
	rc, _, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(dst, rc)
	return err
}

// downloadParallel splits the object into `parts` ranges, fetches them
// concurrently, and writes each directly to its offset in dst via WriteAt.
// No chunk is fully buffered — each range streams from the store to the
// file.
func downloadParallel(ctx context.Context, store BlobStore, key string, size int64, dst io.WriterAt, parts int) error {
	chunkSize := size / int64(parts)
	if chunkSize == 0 {
		chunkSize = size
		parts = 1
	}

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < parts; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1
		if i == parts-1 {
			end = size - 1 // last part picks up the remainder
		}
		g.Go(func() error {
			rc, err := store.GetRange(gctx, key, start, end)
			if err != nil {
				return err
			}
			defer rc.Close()
			_, err = io.Copy(&offsetWriter{dst, start}, rc)
			return err
		})
	}
	return g.Wait()
}

// offsetWriter adapts io.WriterAt to io.Writer, tracking the current offset.
// Each parallel download goroutine has its own offsetWriter, so the shared
// state is only the underlying file's WriteAt — which is safe for concurrent
// use on non-overlapping ranges (per os.File docs).
type offsetWriter struct {
	w      io.WriterAt
	offset int64
}

func (o *offsetWriter) Write(p []byte) (int, error) {
	n, err := o.w.WriteAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

// applyLog fetches and applies every log entry with Seq > fromSeq to conn,
// in order.
//
// Fetches are concurrent (bounded by fetchConcurrency) and buffered in
// memory — changesets are small (typically KB) so this is cheap, and it
// collapses N round-trips into roughly one. Apply is sequential (SQLite
// conn is single-threaded and changesets must apply in seq order).
//
// Returns the seq of the last applied entry, or fromSeq if no entries were
// applied (log was empty or all entries were at or before fromSeq).
func applyLog(ctx context.Context, store BlobStore, conn *sqlite.Conn, log []LogEntry, fromSeq int64) (int64, error) {
	// Collect entries we need.
	var need []LogEntry
	for _, e := range log {
		if e.Seq > fromSeq {
			need = append(need, e)
		}
	}
	if len(need) == 0 {
		return fromSeq, nil
	}

	// Fetch all concurrently.
	bodies := make([][]byte, len(need))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fetchConcurrency)
	for i, e := range need {
		i, e := i, e
		g.Go(func() error {
			rc, _, err := store.Get(gctx, e.Key)
			if err != nil {
				return fmt.Errorf("fetch changeset seq=%d key=%s: %w", e.Seq, e.Key, err)
			}
			defer rc.Close()
			body, err := io.ReadAll(rc)
			if err != nil {
				return fmt.Errorf("read changeset seq=%d: %w", e.Seq, err)
			}
			bodies[i] = body
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fromSeq, err
	}

	// Apply sequentially in seq order.
	seq := fromSeq
	for i, cs := range bodies {
		if err := applyChangeset(conn, cs, conflictAbort); err != nil {
			return seq, fmt.Errorf("apply changeset seq=%d: %w", need[i].Seq, err)
		}
		seq = need[i].Seq
	}
	return seq, nil
}
