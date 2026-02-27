package s3db

// Option configures a DB at Open time.
type Option func(*options)

type options struct {
	localPath        string
	maxRetries       int
	autoCompactAfter int // compact when len(log) >= this; 0 = never
	migrations       []Migration
}

func defaultOptions() options {
	return options{
		localPath:        "", // empty → temp file
		maxRetries:       10,
		autoCompactAfter: 0,
	}
}

// WithLocalPath sets the path for the local SQLite file. If unset, a temp
// file is used and cleaned up on Close. For Lambda, set this to a path under
// /tmp so the file survives across warm invocations.
func WithLocalPath(path string) Option {
	return func(o *options) { o.localPath = path }
}

// WithMaxRetries sets how many CAS attempts Update will make before returning
// ErrConflict. Default is 10. Each attempt is one round-trip to the store.
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

// WithAutoCompact enables automatic compaction. When the log reaches
// threshold entries, Update will trigger a compaction in the background
// after a successful commit. Compaction errors are logged but don't affect
// the Update result. Default is 0 (disabled — call Compact explicitly).
func WithAutoCompact(threshold int) Option {
	return func(o *options) { o.autoCompactAfter = threshold }
}

// WithMigrations registers schema migrations to be run on Open. Each
// migration is a forced compaction — see DESIGN.md. Migrations must have
// strictly increasing Version numbers. The max Version becomes the client's
// expected schema version; Update will reject writes if the manifest's
// schema version doesn't match.
func WithMigrations(ms []Migration) Option {
	return func(o *options) { o.migrations = ms }
}
