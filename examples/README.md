# Examples

Each example is a standalone `main` package. All compile with `go build ./...`.

## `inmemory/`

**Run this first** — it needs no AWS credentials and demonstrates the
concurrency model in ~30ms:

```
go run ./examples/inmemory
```

Spawns 8 concurrent workers, each opening its own `*s3db.DB` against a
shared in-memory store, all incrementing the same counter. Verifies the
final value is exactly `workers × increments` — no lost updates.

## `basic/`

Full API tour against real S3 (or MinIO): migrations, `Update`, `View`,
`Compact`, `GC`. See the package comment for invocation.

## `lambda/`

The recommended Lambda deployment pattern:
- `*s3db.DB` as a package-level var, opened in `init()`
- `WithLocalPath("/tmp/...")` so warm invocations reuse the local file
- External side effects **outside** the `Update` closure (since it may retry)

Doesn't depend on `aws-lambda-go` so it builds as a plain binary; swap in
`lambda.Start(HandleRequest)` for real deployment.
