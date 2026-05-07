package s3db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func newFSStore(t *testing.T) BlobStore {
	t.Helper()
	s, err := NewFileSystemBlobStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileSystemBlobStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestFileSystemBlobStore(t *testing.T) {
	runBlobStoreConformanceTests(t, newFSStore)
}

func TestFileSystemBlobStore_PathTraversal(t *testing.T) {
	root := t.TempDir()
	s, err := NewFileSystemBlobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	bad := []string{
		"../escape",
		"a/../../escape",
		"..",
		"a/../..",
		"./../escape",
		".",
		"/abs/path",
		"a/",      // trailing separator
		"a//b",    // empty component
		"./a",     // leading "./"
		"a/./b",   // embedded "."
		"a/../b",  // internal ".." that cleans to a different key — would alias "b"
		"a/../",   // dir-form
		`a\b`,     // backslash — path separator on Windows, ordinary byte on Unix
		`..\x`,    // backslash traversal attempt
	}
	for _, key := range bad {
		t.Run(key, func(t *testing.T) {
			if _, err := s.Put(ctx, key, strings.NewReader("x"), NoCondition); err == nil {
				t.Errorf("Put(%q) should have been rejected", key)
			}
			if _, _, err := s.Get(ctx, key); err == nil {
				t.Errorf("Get(%q) should have been rejected", key)
			}
			if _, err := s.Stat(ctx, key); err == nil {
				t.Errorf("Stat(%q) should have been rejected", key)
			}
			if err := s.Delete(ctx, key); err == nil {
				t.Errorf("Delete(%q) should have been rejected", key)
			}
		})
	}

	// Sanity check: nothing escaped.
	parent := filepath.Dir(root)
	if _, err := os.Stat(filepath.Join(parent, "escape")); err == nil {
		t.Fatal("path traversal wrote outside the store root")
	}
}

// TestFileSystemBlobStore_SymlinkEscape verifies that a symlink planted
// inside the store root cannot be used to read or write files outside
// the root. This is the os.Root guarantee — a purely lexical Clean +
// prefix check would not catch it.
func TestFileSystemBlobStore_SymlinkEscape(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "store")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("topsecret"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewFileSystemBlobStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Plant a symlink inside the store root pointing outside it.
	if err := os.Symlink(outside, filepath.Join(root, "evil")); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	ctx := context.Background()

	// Reading through the symlink must fail.
	if _, _, err := s.Get(ctx, "evil/secret"); err == nil {
		t.Error("Get through symlink should have been rejected")
	}
	if _, err := s.Stat(ctx, "evil/secret"); err == nil {
		t.Error("Stat through symlink should have been rejected")
	}

	// Writing through the symlink must fail.
	if _, err := s.Put(ctx, "evil/planted", strings.NewReader("x"), NoCondition); err == nil {
		t.Error("Put through symlink should have been rejected")
	}
	if _, err := os.Stat(filepath.Join(outside, "planted")); err == nil {
		t.Fatal("Put through symlink wrote outside the store root")
	}

	// Deleting through the symlink must fail and leave the target intact.
	if err := s.Delete(ctx, "evil/secret"); err == nil {
		t.Error("Delete through symlink should have been rejected")
	}
	if _, err := os.Stat(filepath.Join(outside, "secret")); err != nil {
		t.Fatalf("Delete through symlink removed file outside the root: %v", err)
	}
}

func TestFileSystemBlobStore_ReservedNames(t *testing.T) {
	s, _ := NewFileSystemBlobStore(t.TempDir())
	defer s.Close()
	ctx := context.Background()

	bad := []string{
		// Basename matches.
		".s3db.lock",
		"sub/.s3db.lock",
		".s3db-tmp-x",
		"sub/.s3db-tmp-x",
		// Intermediate path component — Put would MkdirAll a directory
		// at the lock file's path.
		".s3db.lock/x",
		"a/.s3db.lock/b",
		".s3db-tmp-x/y",
		// Case variants — collide with the lock/temp files on
		// case-insensitive filesystems (APFS, NTFS).
		".S3DB.LOCK",
		"sub/.S3db.Lock",
		".S3DB-TMP-x",
	}
	for _, key := range bad {
		if _, err := s.Put(ctx, key, strings.NewReader("x"), NoCondition); err == nil {
			t.Errorf("Put(%q): expected reserved-name error", key)
		}
	}
}

// TestFileSystemBlobStore_DirectoryKey verifies that operations on a
// key whose path is an intermediate directory (not a blob) behave as if
// the key does not exist. The filesystem cannot have both a file "a"
// and a directory "a/", so when "a/b" exists, key "a" cannot — and the
// store must report it as nonexistent rather than leak filesystem
// errors (EISDIR, ENOTEMPTY, ...).
func TestFileSystemBlobStore_DirectoryKey(t *testing.T) {
	s, _ := NewFileSystemBlobStore(t.TempDir())
	defer s.Close()
	ctx := context.Background()

	// Put creates the intermediate directory "a".
	if _, err := s.Put(ctx, "a/b", strings.NewReader("x"), NoCondition); err != nil {
		t.Fatal(err)
	}

	// "a" is a directory, not a blob.
	if _, _, err := s.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of directory key: want ErrNotFound, got %v", err)
	}
	if _, err := s.GetRange(ctx, "a", 0, 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRange of directory key: want ErrNotFound, got %v", err)
	}
	if _, err := s.Stat(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stat of directory key: want ErrNotFound, got %v", err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Errorf("Delete of directory key should be a no-op, got %v", err)
	}

	// Conditional Put: there is no blob at "a", so IfMatch must fail
	// with ErrPreconditionFailed (not EISDIR or some other OS error),
	// and IfNoneMatch must NOT report ErrPreconditionFailed (the key
	// "a" does not exist as a blob — the conflict is with the directory,
	// reported as a different error).
	if _, err := s.Put(ctx, "a", strings.NewReader("y"), PutCondition{IfMatch: "deadbeef"}); !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("Put IfMatch on directory key: want ErrPreconditionFailed, got %v", err)
	}
	if _, err := s.Put(ctx, "a", strings.NewReader("y"), PutCondition{IfNoneMatch: true}); err == nil {
		t.Error("Put IfNoneMatch on directory key: want error (write impossible), got nil")
	} else if errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("Put IfNoneMatch on directory key: a directory is not a blob, must not be ErrPreconditionFailed; got %v", err)
	}
	// Unconditional Put on a directory key must fail with a clear error.
	if _, err := s.Put(ctx, "a", strings.NewReader("y"), NoCondition); err == nil {
		t.Error("Put on directory key should fail (write impossible)")
	}

	// "a/b" must survive all of the above.
	if _, _, err := s.Get(ctx, "a/b"); err != nil {
		t.Errorf("a/b should still exist: %v", err)
	}
}

func TestFileSystemBlobStore_ListSkipsInternalFiles(t *testing.T) {
	root := t.TempDir()
	s, _ := NewFileSystemBlobStore(root)
	defer s.Close()
	ctx := context.Background()

	// One real key plus the lock file (created lazily by the Put) and a
	// fake orphaned temp file.
	if _, err := s.Put(ctx, "real", strings.NewReader("x"), NoCondition); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, fsTmpPrefix+"orphan"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	keys, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "real" {
		t.Errorf("List = %v, want [real]", keys)
	}
}

func TestFileSystemBlobStore_PersistsAcrossReopen(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	s1, _ := NewFileSystemBlobStore(root)
	etag1, err := s1.Put(ctx, "mydb/manifest.json", strings.NewReader(`{"v":1}`), NoCondition)
	if err != nil {
		t.Fatal(err)
	}
	s1.Close()

	s2, _ := NewFileSystemBlobStore(root)
	defer s2.Close()
	rc, etag2, err := s2.Get(ctx, "mydb/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, _ := io.ReadAll(rc)
	if string(body) != `{"v":1}` {
		t.Errorf("body = %q", body)
	}
	if etag1 != etag2 {
		t.Errorf("etag changed across reopen: %q vs %q", etag1, etag2)
	}
}

func TestFileSystemBlobStore_DeletePrunesEmptyDirs(t *testing.T) {
	root := t.TempDir()
	s, _ := NewFileSystemBlobStore(root)
	defer s.Close()
	ctx := context.Background()

	if _, err := s.Put(ctx, "a/b/c/file", strings.NewReader("x"), NoCondition); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a/b/c/file"); err != nil {
		t.Fatal(err)
	}
	// a/, a/b/, a/b/c/ should all be gone.
	if _, err := os.Stat(filepath.Join(root, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected a/ to be pruned, stat err = %v", err)
	}
	// Root must survive.
	if _, err := os.Stat(root); err != nil {
		t.Errorf("root removed: %v", err)
	}
}

// crossProcEnvDir is set when the test binary is re-executed as a worker
// for TestFileSystemBlobStore_CrossProcessCAS.
const crossProcEnvDir = "S3DB_FS_CROSSPROC_DIR"

const (
	crossProcWorkers    = 4
	crossProcIncrements = 25
)

// TestFileSystemBlobStore_CrossProcessCAS verifies that the flock-based
// locking actually serializes conditional writes across OS processes,
// not just across goroutines. It re-execs the test binary as N child
// processes that each perform CAS increments against a shared counter
// in a temp directory, then checks the final value.
//
// When invoked with S3DB_FS_CROSSPROC_DIR set, this test runs as a
// worker: it does its increments and exits. Otherwise it is the
// orchestrator.
func TestFileSystemBlobStore_CrossProcessCAS(t *testing.T) {
	if dir := os.Getenv(crossProcEnvDir); dir != "" {
		// Child process. Do increments and exit. Errors are fatal; the
		// parent inspects our exit code.
		if err := crossProcWorker(dir, crossProcIncrements); err != nil {
			t.Fatal(err)
		}
		return
	}

	if testing.Short() {
		t.Skip("skipping cross-process test in -short mode")
	}

	dir := t.TempDir()
	s, err := NewFileSystemBlobStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Put(ctx, "counter", bytes.NewReader([]byte{0}), NoCondition); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	type worker struct {
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	workers := make([]worker, crossProcWorkers)
	for i := range workers {
		var out bytes.Buffer
		cmd := exec.Command(exe,
			"-test.run=^TestFileSystemBlobStore_CrossProcessCAS$",
			"-test.v=false",
		)
		cmd.Env = append(os.Environ(), crossProcEnvDir+"="+dir)
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		workers[i] = worker{cmd, &out}
	}
	for i, w := range workers {
		if err := w.cmd.Wait(); err != nil {
			t.Fatalf("worker %d failed: %v\n%s", i, err, w.out.String())
		}
	}

	rc, _, err := s.Get(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	want := byte(crossProcWorkers * crossProcIncrements % 256)
	if body[0] != want {
		t.Fatalf("final counter = %d, want %d (lost updates: cross-process locking is broken)", body[0], want)
	}
}

// crossProcWorker performs n CAS increments against dir/counter.
func crossProcWorker(dir string, n int) error {
	s, err := NewFileSystemBlobStore(dir)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		for {
			rc, etag, err := s.Get(ctx, "counter")
			if err != nil {
				return err
			}
			body, _ := io.ReadAll(rc)
			rc.Close()
			_, err = s.Put(ctx, "counter", bytes.NewReader([]byte{body[0] + 1}), PutCondition{IfMatch: etag})
			if err == nil {
				break
			}
			if !errors.Is(err, ErrPreconditionFailed) {
				return fmt.Errorf("unexpected Put error: %w", err)
			}
		}
	}
	return nil
}
