package s3db

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"

	"github.com/psanford/s3db/internal/etag"
)

// FileSystemBlobStore implements BlobStore against a local filesystem
// directory. It is intended for local development, testing, and CLI
// workflows where running MinIO or hitting real S3 is overkill, and for
// embedding s3db in single-machine applications that want a real on-disk
// database with the same concurrency model.
//
// All filesystem operations are scoped with os.Root, so a key cannot
// resolve outside the store directory via ".." components or symlinks —
// the boundary is enforced atomically by the OS, not by string checks.
//
// # Concurrency
//
// FileSystemBlobStore is safe for concurrent use by multiple goroutines
// AND multiple processes on the same machine. Cross-process safety is
// provided by an advisory file lock (flock(2) on Unix, LockFileEx on
// Windows) on a reserved file at <root>/.s3db.lock. The lock is held
// only for the duration of a Put or Delete; reads never take the lock
// (atomic rename guarantees readers see either the old or the new
// content, never a partial write).
//
// # Limitations — read this before using in production
//
// Advisory file locks are only reliable when a single OS kernel mediates
// all access to the directory. In practice that means:
//
//   - SUPPORTED: local filesystems (ext4, xfs, btrfs, zfs, APFS, NTFS,
//     tmpfs) accessed by one or more processes on one machine.
//
//   - NOT SUPPORTED: any filesystem shared across machines.
//   - NFS (v3 and v4): flock is silently ignored, mapped to local-only,
//     or converted to lease-based locking depending on mount options,
//     server, and kernel version. Two clients can both believe they hold
//     the lock and corrupt the manifest.
//   - CIFS/SMB: advisory locks are not reliably honored across clients.
//   - FUSE filesystems (sshfs, s3fs-fuse, etc.): lock support depends
//     entirely on the implementation; most do not support it.
//   - Distributed/clustered filesystems (GlusterFS, CephFS, Lustre,
//     GFS2): flock is often a no-op or local-node-only.
//
// If you need multi-machine access, use S3BlobStore — that is the whole
// point of this library.
//
// Other things to know:
//
//   - Durability: Put fsyncs the data file before the atomic rename, so
//     a successful Put will not produce a torn or zero-length file after
//     a crash. The parent directory entry is NOT fsynced, so a power
//     loss immediately after Put may roll back the rename — readers
//     would see the previous version of the key, not garbage. This is a
//     weaker durability guarantee than S3.
//   - ETags: computed as the hex MD5 of the content, matching what S3
//     returns for single-part unencrypted uploads. ETags are recomputed
//     on every Get and Stat by reading the file. This is cheap for the
//     small objects s3db produces; if you Stat large blobs in a tight
//     loop it will show up.
//   - Crash cleanup: a process that crashes mid-Put may leave a
//     temporary file (basename prefix ".s3db-tmp-") in the target's
//     directory. These are excluded from List and DeletePrefix and are
//     harmless, but accumulate. Remove them by hand if they bother you.
//   - Key namespace: keys map directly to filesystem paths, so a key
//     and a path-prefix of it cannot coexist (e.g. "a" and "a/b" — the
//     filesystem cannot have a file and a directory at the same path).
//     Put returns an error for the conflicting key; Get, GetRange,
//     Stat, and Delete treat a path that is a directory as a
//     nonexistent key. S3 has a flat key namespace and allows both.
//     s3db never generates such keys, but callers using this type as a
//     general-purpose store should not.
//   - Reserved names: the key ".s3db.lock" and any key whose basename
//     starts with ".s3db-tmp-" are reserved and must not be used.
//   - Permissions: directories are created 0o700 and files 0o600
//     (owner-only), since the store typically holds a database.
//     chmod the root if you need broader access; pre-existing
//     directories keep their permissions.
//
// # Example
//
//	store, err := s3db.NewFileSystemBlobStore("/var/lib/myapp/db")
//	if err != nil { ... }
//	defer store.Close()
//	db, err := s3db.Open(ctx, store, "mydb/")
type FileSystemBlobStore struct {
	root     *os.Root // scoped FD on the store directory; see os.Root docs
	rootPath string   // absolute store directory, for the lock file path
	lockPath string   // rootPath + "/.s3db.lock"
}

const (
	fsLockName  = ".s3db.lock"
	fsTmpPrefix = ".s3db-tmp-"
)

// NewFileSystemBlobStore opens (or creates) a blob store rooted at dir.
// The directory and the lock file are created if they do not exist.
// Call Close when done to release the directory file descriptor.
func NewFileSystemBlobStore(dir string) (*FileSystemBlobStore, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("s3db: filesystem store: %w", err)
	}
	// 0o700: the store holds a database; default to owner-only so we
	// don't leak data via group/world-readable files. Pre-existing
	// directories keep their permissions; chmod the root if you want
	// broader access.
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("s3db: filesystem store: %w", err)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("s3db: filesystem store: %w", err)
	}
	return &FileSystemBlobStore{
		root:     root,
		rootPath: abs,
		lockPath: filepath.Join(abs, fsLockName),
	}, nil
}

// Root returns the absolute root directory backing this store.
func (s *FileSystemBlobStore) Root() string { return s.rootPath }

// Close releases the directory file descriptor held by the store. The
// store must not be used after Close. Close is optional — the descriptor
// is reclaimed by the OS on process exit — but recommended for
// long-running processes that open and discard many stores.
func (s *FileSystemBlobStore) Close() error { return s.root.Close() }

// validateKey rejects keys that cannot map cleanly and unambiguously to
// a regular file inside the store root.
//
// Keys must satisfy fs.ValidPath: unrooted, slash-separated, no empty,
// "." or ".." elements. This rejects "..", "a/../b", "a//b", "./a",
// "/abs", and "a/". Such keys would either escape the root, alias
// another key (e.g. "a/../b" and "b" resolve to the same file but are
// distinct S3 keys, breaking round-trips through List), or leave stray
// directories behind. Refusing the ambiguity is simpler and safer than
// silently normalizing. The lone "." (which fs.ValidPath special-cases
// as valid) is also rejected — it names the root directory, not a blob.
//
// Reserved internal names (the lock file and temp-file prefix) are
// rejected in any path component.
//
// This is a fast lexical pre-check that produces clear error messages
// and enforces key↔file round-trip identity. os.Root independently
// enforces the root boundary on every filesystem operation (including
// against symlink escapes), so the traversal part is belt-and-suspenders,
// not the security boundary.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("s3db: filesystem store: empty key")
	}
	if !fs.ValidPath(key) || key == "." {
		return fmt.Errorf("s3db: filesystem store: key %q is not a valid relative path", key)
	}
	if strings.ContainsRune(key, '\\') {
		// Backslash is the path separator on Windows, so "a\b" would
		// resolve to a/b there but be a single element on Unix —
		// breaking key round-trip identity and portability. S3 permits
		// backslashes, but s3db never generates them, so refusing the
		// ambiguity is cheaper than carrying an escape scheme.
		return fmt.Errorf("s3db: filesystem store: key %q contains a backslash", key)
	}
	if isReservedKey(key) {
		return fmt.Errorf("s3db: filesystem store: key %q uses a reserved name", key)
	}
	return nil
}

// isReservedKey reports whether any path component of the key matches a
// reserved internal name (the lock file or temp-file prefix). The check
// is case-insensitive because the README's supported-filesystem list
// includes case-insensitive filesystems (APFS, NTFS), where ".S3DB.LOCK"
// would resolve to the same inode as ".s3db.lock". Every component is
// checked, not just the basename, because Put creates intermediate
// directories — a key like ".s3db.lock/x" would otherwise create a
// directory at the lock file's path.
func isReservedKey(key string) bool {
	for _, comp := range strings.Split(key, "/") {
		lc := strings.ToLower(comp)
		if lc == fsLockName || strings.HasPrefix(lc, fsTmpPrefix) {
			return true
		}
	}
	return false
}

// nativeName converts a forward-slash blob key to the OS-native path
// separator for use with os.Root methods.
func nativeName(key string) string { return filepath.FromSlash(key) }

// withLock acquires the cross-process exclusive lock, runs fn, and
// releases the lock. Each call uses a fresh file descriptor, so
// concurrent goroutines in the same process serialize via the OS lock —
// no additional in-process mutex is needed.
func (s *FileSystemBlobStore) withLock(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fl := flock.New(s.lockPath)
	// Poll so we honor context cancellation; the lock is only held for a
	// stat+rename so contention is brief and 5ms is responsive enough.
	locked, err := fl.TryLockContext(ctx, 5*time.Millisecond)
	if err != nil {
		return err
	}
	if !locked {
		// TryLockContext only returns (false, nil) on a 0-retry config;
		// treat it as a context error to be safe.
		return ctx.Err()
	}
	defer fl.Unlock()
	return fn()
}

func (s *FileSystemBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := validateKey(key); err != nil {
		return nil, "", err
	}
	f, _, err := s.openBlob(key)
	if err != nil {
		return nil, "", err
	}
	// Compute the ETag from this file descriptor, then rewind. The FD
	// continues to refer to the original inode even if a concurrent Put
	// renames a new file over the path, so the returned ETag always
	// matches the body the caller will read.
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, "", err
	}
	return f, hex.EncodeToString(h.Sum(nil)), nil
}

// openBlob opens the file backing key for reading and returns it along
// with its FileInfo. Returns ErrNotFound if the key does not exist, or
// if the path is a directory (an intermediate directory created by Put
// for some deeper key — there is no blob at that key).
func (s *FileSystemBlobStore) openBlob(key string) (*os.File, fs.FileInfo, error) {
	f, err := s.root.Open(nativeName(key))
	if err != nil {
		return nil, nil, mapFSErr(err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if fi.IsDir() {
		f.Close()
		return nil, nil, ErrNotFound
	}
	return f, fi, nil
}

func (s *FileSystemBlobStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	f, fi, err := s.openBlob(key)
	if err != nil {
		return nil, err
	}
	// Clamp to the file size, matching S3's behavior for ranges past EOF.
	size := fi.Size()
	if start >= size {
		f.Close()
		return io.NopCloser(strings.NewReader("")), nil
	}
	if end >= size {
		end = size - 1
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &limitedReadCloser{r: io.LimitReader(f, end-start+1), c: f}, nil
}

// limitedReadCloser wraps a LimitReader but closes the underlying file.
type limitedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedReadCloser) Close() error               { return l.c.Close() }

func (s *FileSystemBlobStore) Stat(ctx context.Context, key string) (BlobInfo, error) {
	if err := ctx.Err(); err != nil {
		return BlobInfo{}, err
	}
	if err := validateKey(key); err != nil {
		return BlobInfo{}, err
	}
	// Open once and derive both metadata and ETag from the same file
	// descriptor. A separate os.Stat + reopen would race a concurrent
	// Put and could return a chimera (Size/ModTime from one version,
	// ETag from another).
	f, fi, err := s.openBlob(key)
	if err != nil {
		return BlobInfo{}, err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return BlobInfo{}, err
	}
	return BlobInfo{
		ETag:         hex.EncodeToString(h.Sum(nil)),
		Size:         fi.Size(),
		LastModified: fi.ModTime(),
	}, nil
}

// computeETag opens name (root-relative) and returns its hex MD5.
func (s *FileSystemBlobStore) computeETag(name string) (string, error) {
	f, err := s.root.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *FileSystemBlobStore) Put(ctx context.Context, key string, body io.Reader, cond PutCondition) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if cond.IfMatch != "" && cond.IfNoneMatch {
		return "", errBothConditions
	}
	if err := validateKey(key); err != nil {
		return "", err
	}
	name := nativeName(key)
	dir := filepath.Dir(name)
	if dir != "." {
		if err := s.root.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
	}

	// Stage the body into a temp file in the target directory (same
	// filesystem, so the rename below is atomic) and compute its MD5 in
	// the same pass. We do this BEFORE taking the cross-process lock,
	// matching S3 (the upload happens before the precondition is
	// evaluated server-side) and avoiding holding the lock during a
	// potentially slow read from the caller's reader.
	tmp, tmpName, err := s.createTemp(dir)
	if err != nil {
		return "", err
	}
	cleanup := func() { tmp.Close(); s.root.Remove(tmpName) }

	h := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), body); err != nil {
		cleanup()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		s.root.Remove(tmpName)
		return "", err
	}
	newETag := hex.EncodeToString(h.Sum(nil))

	err = s.withLock(ctx, func() error {
		// Look at what's currently at the target path. A directory means
		// there is no blob at this key (it's an intermediate directory
		// created by a Put for some deeper key) — and the write itself
		// cannot proceed because a regular file cannot replace it.
		var targetExists, targetIsDir bool
		if fi, lerr := s.root.Lstat(name); lerr == nil {
			targetIsDir = fi.IsDir()
			targetExists = !fi.IsDir()
		} else if !errors.Is(lerr, fs.ErrNotExist) {
			return lerr
		}

		if cond.IfNoneMatch && targetExists {
			return ErrPreconditionFailed
		}
		if cond.IfMatch != "" {
			if !targetExists {
				// No blob at this key (nonexistent or a directory).
				// S3 returns 412 for If-Match against a nonexistent
				// object.
				return ErrPreconditionFailed
			}
			cur, cerr := s.computeETag(name)
			if errors.Is(cerr, fs.ErrNotExist) {
				return ErrPreconditionFailed
			}
			if cerr != nil {
				return cerr
			}
			if cur != etag.Normalize(cond.IfMatch) {
				return ErrPreconditionFailed
			}
		}
		if targetIsDir {
			// Preconditions (if any) passed, but the write itself is
			// impossible: the filesystem cannot store a blob at a path
			// that is already a directory holding deeper keys.
			return fmt.Errorf("s3db: filesystem store: key %q conflicts with an existing directory", key)
		}
		return s.root.Rename(tmpName, name)
	})
	if err != nil {
		s.root.Remove(tmpName)
		return "", err
	}
	return newETag, nil
}

// createTemp creates a uniquely-named temp file in dir (root-relative,
// "." for the root itself) for staging a Put. os.Root has no CreateTemp
// equivalent, so we roll a minimal one with O_CREATE|O_EXCL and a
// crypto/rand suffix. Returns the open file and its root-relative name.
//
// Retries on fs.ErrNotExist to close a narrow race: createTemp runs
// outside the cross-process lock (so the lock isn't held during the
// caller's body read), but a concurrent Delete's pruneEmptyDirs runs
// inside the lock and can remove the directory between MkdirAll and the
// OpenFile here. Recreating the directory and retrying makes the race
// invisible to callers. Once the temp file exists, the directory is
// non-empty and pruneEmptyDirs will leave it alone.
func (s *FileSystemBlobStore) createTemp(dir string) (*os.File, string, error) {
	for tries := 0; tries < 10; tries++ {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, "", err
		}
		base := fsTmpPrefix + hex.EncodeToString(b[:])
		name := base
		if dir != "." {
			name = filepath.Join(dir, base)
		}
		f, err := s.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return f, name, nil
		}
		switch {
		case errors.Is(err, fs.ErrExist):
			// 64-bit random collision: retry with a new name.
		case errors.Is(err, fs.ErrNotExist) && dir != ".":
			// Parent directory was concurrently pruned: recreate it.
			if merr := s.root.MkdirAll(dir, 0o700); merr != nil {
				return nil, "", merr
			}
		default:
			return nil, "", err
		}
	}
	return nil, "", errors.New("s3db: filesystem store: could not create temp file")
}

func (s *FileSystemBlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var keys []string
	// os.Root.FS() returns a forward-slash fs.FS scoped to the root, so
	// paths from WalkDir are already in blob-key form.
	fsys := s.root.FS()
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				// A directory was removed mid-walk by a concurrent
				// Delete/DeletePrefix; skip it.
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if isReservedKey(p) {
			return nil
		}
		if strings.HasPrefix(p, prefix) {
			keys = append(keys, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *FileSystemBlobStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	name := nativeName(key)
	return s.withLock(ctx, func() error {
		fi, err := s.root.Lstat(name)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // nonexistent key — no-op per contract
			}
			return err
		}
		if fi.IsDir() {
			// The path is an intermediate directory created by Put for
			// some deeper key, not a blob. There is no blob at this key,
			// so this is a no-op (and we must not remove the directory —
			// it may hold other keys).
			return nil
		}
		if err := s.root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		s.pruneEmptyDirs(filepath.Dir(name))
		return nil
	})
}

func (s *FileSystemBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// pruneEmptyDirs removes empty parent directories from dir (root-
// relative) up to but not including the store root. Best-effort;
// concurrent writers may recreate a directory we just removed, which
// is fine.
func (s *FileSystemBlobStore) pruneEmptyDirs(dir string) {
	for dir != "." && dir != "" {
		if err := s.root.Remove(dir); err != nil {
			return // not empty, or in use, or already gone — stop here
		}
		dir = filepath.Dir(dir)
	}
}

// mapFSErr converts fs.ErrNotExist into the package's ErrNotFound
// sentinel so callers don't need to know which BlobStore is in use.
func mapFSErr(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

// Compile-time check.
var _ BlobStore = (*FileSystemBlobStore)(nil)
