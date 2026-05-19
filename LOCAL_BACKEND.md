# Local filesystem backend

`FileSystemBlobStore` implements the same `BlobStore` interface against a local directory. Use it for local development, tests, CLI workflows, or single-machine apps that want the s3db model without S3:

```go
store, err := s3db.NewFileSystemBlobStore("/var/lib/myapp/db")
if err != nil { ... }
defer store.Close()
db, err := s3db.Open(ctx, store, "mydb/")
```

It is safe for concurrent access by **multiple goroutines and multiple processes on the same machine** — conditional writes are serialized with an advisory file lock (`flock(2)` on Unix, `LockFileEx` on Windows) on `<root>/.s3db.lock`, and writes are staged to a temp file and atomically renamed into place. Reads never lock.

Every operation is scoped with [`os.Root`](https://pkg.go.dev/os#Root), so no key — and no symlink someone plants inside the directory — can read or write outside the store root.

## ⚠️ Filesystem support

Advisory file locks only work when **a single OS kernel mediates all access** to the directory. That means:

| | Filesystem | Notes |
|---|---|---|
| ✅ | ext4, xfs, btrfs, zfs, APFS, NTFS, tmpfs | Local disk, any number of processes on one machine |
| ❌ | NFS (v3, v4) | `flock` is silently ignored or local-only depending on mount options. Two clients can both believe they hold the lock and **corrupt the manifest**. |
| ❌ | CIFS / SMB | Advisory locks not reliably honored across clients |
| ❌ | FUSE filesystems (sshfs, s3fs-fuse, …) | Lock support varies; most don't implement it |
| ❌ | Distributed FS (GlusterFS, CephFS, Lustre, GFS2) | `flock` is often a no-op or local-node-only |

**Do not point `FileSystemBlobStore` at a network filesystem.** It will appear to work and then silently lose writes under concurrent access. If you need multi-machine access, use `S3BlobStore` — that is what this library is for.

## Other limitations vs. S3

- **Durability.** `Put` fsyncs the data file before the atomic rename, so a crash never leaves a torn or partial object. The directory entry is *not* fsynced, so a power loss immediately after `Put` may roll back the rename — readers would see the previous version, not garbage. This is weaker than S3's durability guarantee.
- **ETags.** Computed as the MD5 of the content, recomputed on every `Get`/`Stat` by reading the file. Cheap for s3db's KB–MB objects; will show up if you `Stat` large blobs in a tight loop.
- **Crash leftovers.** A process killed mid-`Put` may leave a `.s3db-tmp-*` file behind. These are excluded from `List` and harmless, but accumulate.
- **Key namespace.** Keys map directly to filesystem paths, so a key and a path-prefix of it (e.g. `a` and `a/b`) cannot coexist — the filesystem cannot have a file and a directory at the same path. S3 has a flat namespace and allows both. s3db never generates such keys.
- **Reserved names.** The key `.s3db.lock` and any key whose basename starts with `.s3db-tmp-` are reserved.
- **Permissions.** Directories are created `0700` and files `0600` (owner-only). `chmod` the root if you need broader access.
