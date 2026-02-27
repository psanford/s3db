package s3db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// DB is a concurrency-safe SQLite database backed by a BlobStore. It holds
// a single long-lived local SQLite connection synced to the remote state.
//
// DB is safe for concurrent use — View and Update are serialized by an
// internal mutex. This is appropriate for Lambda (typically one goroutine
// per invocation) and prevents pointless intra-process CAS contention.
//
// Create a DB with Open. Close it when done to release the local file and
// connection.
type DB struct {
	mu sync.Mutex

	cfg       commitConfig
	st        commitState
	localPath string
	opts      options

	// ownLocalFile is true if we created a temp file and should delete it
	// on Close. False if the user specified a path (their responsibility).
	ownLocalFile bool
}

// Migration is a schema-evolution step. See WithMigrations and DESIGN.md.
type Migration struct {
	Version int
	Name    string
	Up      func(*sqlite.Conn) error
}

// Open connects to (or initializes) a database under the given prefix in
// the store. The prefix should end with "/" — e.g. "mydb/".
//
// If no manifest exists at prefix, Open creates one with an empty SQLite
// database as the initial snapshot (schema_version=0, seq=0). This is safe
// under concurrent Opens: the creation uses If-None-Match, and the loser
// of that race falls back to reading the winner's manifest.
//
// If WithMigrations is set, pending migrations are applied before Open
// returns. See DESIGN.md for the migration-as-forced-compaction model.
//
// The returned DB holds a local SQLite file. If WithLocalPath is not set,
// a temp file is used and deleted on Close. For Lambda, set WithLocalPath
// to something under /tmp so the file persists across warm starts.
func Open(ctx context.Context, store BlobStore, prefix string, opts ...Option) (*DB, error) {
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("s3db: prefix must end with /, got %q", prefix)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	manifestKey := prefix + "manifest.json"

	// Load or bootstrap the manifest.
	m, etag, err := loadManifest(ctx, store, manifestKey)
	if errors.Is(err, ErrNotFound) {
		m, etag, err = bootstrap(ctx, store, prefix, manifestKey)
	}
	if err != nil {
		return nil, err
	}

	// Set up local file.
	localPath := o.localPath
	ownLocalFile := false
	if localPath == "" {
		tmp, err := os.CreateTemp("", "s3db-*.sqlite")
		if err != nil {
			return nil, err
		}
		localPath = tmp.Name()
		tmp.Close()
		ownLocalFile = true
	}

	// Download snapshot and open connection.
	if err := downloadSnapshot(ctx, store, m.Snapshot.Key, m.Snapshot.Size, localPath); err != nil {
		if ownLocalFile {
			os.Remove(localPath)
		}
		return nil, err
	}
	conn, err := openLocalConn(localPath)
	if err != nil {
		if ownLocalFile {
			os.Remove(localPath)
		}
		return nil, fmt.Errorf("s3db: open local db: %w", err)
	}

	// Apply log to reach current state.
	localSeq, err := applyLog(ctx, store, conn, m.Log, m.Snapshot.Seq)
	if err != nil {
		conn.Close()
		if ownLocalFile {
			os.Remove(localPath)
		}
		return nil, err
	}

	// Determine expected schema version from migrations.
	schemaVer := 0
	for _, mig := range o.migrations {
		if mig.Version > schemaVer {
			schemaVer = mig.Version
		}
	}

	db := &DB{
		cfg: commitConfig{
			store:       store,
			prefix:      prefix,
			manifestKey: manifestKey,
			maxRetries:  o.maxRetries,
			schemaVer:   schemaVer,
		},
		st: commitState{
			conn:        conn,
			localSeq:    localSeq,
			snapshotKey: m.Snapshot.Key,
			manifest:    m,
			etag:        etag,
		},
		localPath:    localPath,
		opts:         o,
		ownLocalFile: ownLocalFile,
	}

	// Run pending migrations. Each migration is a forced compaction —
	// the Up function runs on the local DB, the result becomes a new
	// snapshot, and the manifest CAS bumps schema_version. Safe under
	// concurrent Open: the CAS loser sees the bumped version and skips.
	if err := db.runMigrations(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// bootstrap creates an initial empty database and manifest. Safe under
// concurrent callers via If-None-Match on the manifest — exactly one
// bootstrap succeeds; others fall back to loading the winner's manifest.
func bootstrap(ctx context.Context, store BlobStore, prefix, manifestKey string) (*manifest, string, error) {
	// Build an empty SQLite file in a temp location.
	tmp, err := os.CreateTemp("", "s3db-bootstrap-*.sqlite")
	if err != nil {
		return nil, "", err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	// OpenConn with OpenCreate doesn't write the file header until the
	// first operation. Run a no-op PRAGMA to force it — otherwise the
	// snapshot is 0 bytes, which collides with our Size=0-means-unknown
	// convention and is also a bit weird to have as a valid SQLite file.
	conn, err := sqlite.OpenConn(tmpPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		return nil, "", fmt.Errorf("bootstrap: create empty db: %w", err)
	}
	if err := sqlitex.Execute(conn, "PRAGMA user_version = 0", nil); err != nil {
		conn.Close()
		return nil, "", fmt.Errorf("bootstrap: init header: %w", err)
	}
	conn.Close()

	// Upload the empty snapshot. This key is deterministic (no UUID) so
	// concurrent bootstraps write the same content to the same key — the
	// second PUT is a harmless overwrite of identical bytes.
	snapKey := prefix + "snapshots/snap-init.sqlite"
	snapSize, err := uploadFile(ctx, store, snapKey, tmpPath)
	if err != nil {
		return nil, "", fmt.Errorf("bootstrap: upload snapshot: %w", err)
	}

	// CAS the manifest with If-None-Match.
	m := &manifest{
		Seq:           0,
		SchemaVersion: 0,
		Snapshot:      blobRef{Key: snapKey, Seq: 0, Size: snapSize},
		Log:           nil,
	}
	etag, err := putManifest(ctx, store, manifestKey, m, PutCondition{IfNoneMatch: true})
	if errors.Is(err, ErrPreconditionFailed) {
		// Someone else won the bootstrap race. Load their manifest.
		return loadManifest(ctx, store, manifestKey)
	}
	if err != nil {
		return nil, "", fmt.Errorf("bootstrap: put manifest: %w", err)
	}
	return m, etag, nil
}

// View runs fn against the current database state. It first syncs the local
// DB to the latest manifest, then invokes fn. fn should only perform reads;
// any writes it makes are local-only and will NOT be committed or rolled
// back (they'll be visible to subsequent View/Update calls on the same DB
// instance until the next sync). Don't write in View.
//
// View holds the DB mutex for its entire duration, serializing with other
// View and Update calls on the same DB instance.
func (db *DB) View(ctx context.Context, fn func(*sqlite.Conn) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := refreshManifest(ctx, &db.cfg, &db.st, db.localPath); err != nil {
		return err
	}
	return fn(db.st.conn)
}

// Update runs fn as a transaction against the current database state, then
// commits the resulting changeset via the CAS loop.
//
// fn may be invoked multiple times if concurrent writers cause rebase
// conflicts. fn must therefore be idempotent with respect to side effects
// OUTSIDE the database — don't send emails or make external API calls
// inside fn. This is the same contract as a retry loop around a serializable
// transaction.
//
// If fn performs only reads (no INSERTs/UPDATEs/DELETEs, or ones that change
// nothing), Update returns nil without touching the store.
//
// Returns ErrConflict if CAS retries are exhausted, ErrSchemaMismatch or
// ErrSchemaTooNew if the manifest's schema version doesn't match this
// client's migrations, or fn's error if fn fails.
func (db *DB) Update(ctx context.Context, fn func(*sqlite.Conn) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := refreshManifest(ctx, &db.cfg, &db.st, db.localPath); err != nil {
		return err
	}

	if err := doUpdate(ctx, &db.cfg, &db.st, db.localPath, fn); err != nil {
		return err
	}

	// Auto-compact hook. Runs synchronously in the lock — simple and
	// correct, and compaction of a small DB is fast. Compaction errors
	// are swallowed: the Update succeeded, and compaction can be retried
	// later (by the next Update, or an explicit Compact call).
	if db.opts.autoCompactAfter > 0 && len(db.st.manifest.Log) >= db.opts.autoCompactAfter {
		_ = db.compactLocked(ctx)
	}

	return nil
}

// Close releases the local SQLite connection and, if the local file was
// created by Open (no WithLocalPath), deletes it. The DB is unusable after
// Close.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var err error
	if db.st.conn != nil {
		err = db.st.conn.Close()
		db.st.conn = nil
	}
	if db.ownLocalFile && db.localPath != "" {
		os.Remove(db.localPath)
	}
	return err
}

// Stats describes the current state of the database. All fields are
// point-in-time snapshots from the last-seen manifest — other writers
// may have advanced the database since.
type Stats struct {
	// Seq is the logical version the local DB is synced to. Each
	// committed write increments it by exactly 1.
	Seq int64

	// SchemaVersion is the highest migration version applied.
	SchemaVersion int

	// SnapshotSize is the current snapshot's size in bytes.
	// 0 if unknown (old manifest).
	SnapshotSize int64

	// LogEntries is the number of changesets not yet compacted.
	LogEntries int

	// LogBytes is the total size of uncompacted changesets in bytes.
	// May undercount if any log entries have unknown size.
	LogBytes int64
}

// Stats returns a snapshot of the database's current state. Useful for
// diagnostics, logging, and deciding when to trigger Compact/GC.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()

	m := db.st.manifest
	var logBytes int64
	for _, e := range m.Log {
		logBytes += e.Size
	}
	return Stats{
		Seq:           db.st.localSeq,
		SchemaVersion: m.SchemaVersion,
		SnapshotSize:  m.Snapshot.Size,
		LogEntries:    len(m.Log),
		LogBytes:      logBytes,
	}
}

// Seq returns the sequence number the local DB is currently synced to.
// Equivalent to Stats().Seq but cheaper. Useful for diagnostics.
func (db *DB) Seq() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.st.localSeq
}

// openLocalConn opens a read-write connection to the local SQLite file.
// Centralized here so all opens use identical flags. We use explicit
// OpenReadWrite (without OpenWAL) because WAL mode requires a -wal file
// alongside the main file, which complicates snapshot upload/download.
// DELETE journal mode (the SQLite default) keeps everything in one file.
func openLocalConn(path string) (*sqlite.Conn, error) {
	return sqlite.OpenConn(path, sqlite.OpenReadWrite)
}