package s3db

import "time"

// Option configures a DB at Open time.
type Option func(*options)

type options struct {
	localPath        string
	maxRetries       int
	autoCompactAfter int // compact when len(log) >= this; 0 = never
	migrations       []Migration
	gcGracePeriod    time.Duration
	onCompactError   func(error)
}

func defaultOptions() options {
	return options{
		localPath:        "", // empty → temp file
		maxRetries:       10,
		autoCompactAfter: 0,
		gcGracePeriod:    5 * time.Minute,
	}
}

// WithLocalPath sets the path for the local SQLite file. If unset, a temp
// file is used and cleaned up on Close.
//
// Note: the file is re-downloaded from the store on every Open — this
// option only controls WHERE the file lives during one Open→Close cycle.
// For Lambda warm-start caching, keep the *DB handle itself alive across
// invocations (Open once in init(), never Close) rather than relying on
// file reuse.
//
// Two DB instances must not share the same localPath concurrently — there
// is no lock file and the results are undefined.
func WithLocalPath(path string) Option {
	return func(o *options) { o.localPath = path }
}

// WithMaxRetries sets how many CAS attempts Update will make before returning
// ErrConflict. Default is 10. Each attempt is one round-trip to the store.
// Values less than 1 are clamped to 1.
func WithMaxRetries(n int) Option {
	return func(o *options) {
		if n < 1 {
			n = 1
		}
		o.maxRetries = n
	}
}

// WithAutoCompact enables automatic compaction. When the log reaches
// threshold entries, Update triggers a compaction SYNCHRONOUSLY after a
// successful commit (inside the Update call, holding the DB lock). For
// large databases this adds noticeable latency to every Nth write.
//
// Compaction errors do not affect the Update result — the write has
// already committed. Use WithCompactErrorHandler to observe them.
//
// Default is 0 (disabled — call Compact explicitly).
func WithAutoCompact(threshold int) Option {
	return func(o *options) { o.autoCompactAfter = threshold }
}

// WithCompactErrorHandler sets a callback invoked when auto-compaction
// fails. Without this, auto-compact errors are silently discarded (the
// Update that triggered compaction has already succeeded). Useful for
// logging/alerting when compaction is consistently failing.
func WithCompactErrorHandler(fn func(error)) Option {
	return func(o *options) { o.onCompactError = fn }
}

// WithGCGracePeriod sets how old an unreferenced snapshot must be before
// GC will delete it. This protects in-flight readers who loaded an old
// manifest just before a compaction replaced the snapshot they're about
// to download. Default is 5 minutes. Set to 0 to disable (not recommended
// if GC runs close in time to Compact).
func WithGCGracePeriod(d time.Duration) Option {
	return func(o *options) { o.gcGracePeriod = d }
}

// WithMigrations registers schema migrations to be run on Open. Each
// migration is a forced compaction — see DESIGN.md. Migrations must have
// strictly increasing Version numbers. The max Version becomes the client's
// expected schema version; Update will reject writes if the manifest's
// schema version doesn't match.
func WithMigrations(ms []Migration) Option {
	return func(o *options) { o.migrations = ms }
}
