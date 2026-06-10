# SDK Go SDK

The Go SDK lives at [`pkg/ledgerdbsdk/`](../tree/main/pkg/ledgerdbsdk) and is the in-process surface for embedders. It opens the same git store and the same SQLite sidecar that the CLI opens, calls the same application services in `internal/app/*`, and returns Go values to the caller. There is no subprocess, no IPC, no serialisation overhead on the hot path. The package is small on purpose: one `Client`, a handful of typed result structs, a sentinel-error set, and the index-watch lifecycle. Everything else is the manifest-driven configuration of the underlying services.

This page documents the public surface in source order: configuration in `pkg/ledgerdbsdk/config.go`, lifecycle in `client.go`, document operations in `doc.go`, query and index operations in `index.go`, errors in `errors.go`. The end of the page contrasts `pkg/ledgerdbsdk/` with the lower-level `pkg/ledgerdb/` core package and walks one end-to-end worked example.

## Configuration

`Config` in [`pkg/ledgerdbsdk/config.go`](../tree/main/pkg/ledgerdbsdk/config.go) is a plain struct. The required field is `RepoPath`; everything else has a sensible default. `AutoSync` controls whether each write is wrapped in `git fetch` ... `git push` (default `true`); `AutoWatch` controls whether `Open` starts the polling indexer on its own (default `false`). `SignCommits` and `SignKey` enable git commit signing through the underlying `gitrepo.Store`. `StreamLayout` and `HistoryMode` are normally left empty so the SDK reads them from the repository's `db.yaml`; if they are set explicitly they must match the manifest or `New` returns `ErrManifestMismatch`.

The nested `IndexConfig` controls the SQLite sidecar: `DBPath` (default `<repo>/index.db`), `Mode` (`IndexModeState` or `IndexModeHistory`), `Interval` and `Jitter` for the watch loop, `BatchCommits` for SQLite transaction grouping, `Fast` to relax WAL durability, `Fetch` to pull from `origin` before each sync, `OnlyChanges` to suppress watch results when nothing applied, and `EmitResults` to send summaries down the `WatchResults()` channel.

`DefaultConfig(repoPath)` returns an opinionated profile: state mode, 1-second poll, 200 commits per SQLite transaction, fast WAL, fetch on, only-changes on, results on. The CLI's defaults differ slightly — they target one-shot operators rather than embedded watchers — but the SDK defaults are tuned for the case where a long-running process wants near-real-time SQLite updates.

```go
cfg := ledgerdbsdk.DefaultConfig("/var/lib/myapp/ledger.git")
cfg.AutoWatch = true                // start watch in Open
cfg.Index.Interval = 500 * time.Millisecond
cfg.SignCommits = true
cfg.SignKey = "ABCD1234"
```

`normalizeConfig` in the same file fills missing defaults at `New`/`Open` time and returns `ErrRepoPathRequired` if `RepoPath` is empty. The check is a guardrail rather than a deep validation; the repository's actual readiness is established when the git store and SQLite index are opened.

## Lifecycle: New, Open, Close

The lifecycle in [`pkg/ledgerdbsdk/client.go`](../tree/main/pkg/ledgerdbsdk/client.go) is deliberately two-stage. `New(cfg)` normalises the config, loads the manifest from `db.yaml`, reconciles any manifest overrides, and constructs the `gitrepo.Store` with the chosen sign and history options. It does not open the SQLite index and it does not start the watch. `Open(ctx, cfg)` is the convenience wrapper that calls `New`, then `OpenIndex`, then (if `cfg.AutoWatch`) `StartIndexWatch`. The split exists because some callers — a CLI that only needs `Get` and `Put` from the ledger, an integrity verifier — never want the SQLite index opened at all, and paying the open cost just to immediately close it is wasteful.

`OpenIndex(ctx)` opens (or no-ops if already open) the SQLite store through `sqliteindex.OpenWithOptions` with the `Fast` flag honoured from config. It then `PingContext`s the DB to surface configuration errors early and stashes both `*sqliteindex.Store` and the underlying `*sql.DB` on the client behind a mutex. `Close()` reverses the order: stop the watch, drop the channels, close the SQLite store. There is no separate `CloseIndex`; the index is conceptually owned by the client and shared with the watch loop.

The watch lifecycle is the only piece that uses goroutines. `StartIndexWatch(ctx)` launches a single polling goroutine that calls `service.Sync(...)` on each tick, applies `Jitter` to the interval, pushes summaries onto a buffered `results` channel (capacity 1) when `EmitResults` is on and `OnlyChanges` does not suppress it, and returns the first non-`context.Canceled` error on the `errs` channel before closing both. `WatchResults()` and `WatchErrors()` expose the channels; `StopIndexWatch()` cancels the context, drains the error channel, and returns the last error if any.

One detail worth flagging: `StartIndexWatch` honours the `ctx` passed in by deriving a child context from it, but it then closes the result and error channels when the goroutine returns. Callers that select on `WatchResults()` should also select on `WatchErrors()` and on their own `Done()` so they do not block forever after `Close()`.

## Document operations

The document surface in [`pkg/ledgerdbsdk/doc.go`](../tree/main/pkg/ledgerdbsdk/doc.go) mirrors `ledgerdb doc *`. Every method takes a `context.Context`, a `collection` and `docID`, and returns either a typed result or a sentinel error.

`Get(ctx, collection, docID)` returns a `Doc{ Payload, TxHash, TxID, Op }`. `GetInto(ctx, collection, docID, target)` is the same call followed by `json.Unmarshal` into the supplied target; it returns a `DocMeta` carrying the same identifiers without the raw payload. Both go through `docapp.NewGetService` and replay the doc stream the same way the CLI does.

`Put(ctx, collection, docID, payload)` writes a snapshot and returns `PutResult{ CommitHash, TxHash, TxID }`. `PutJSON` is the marshal-then-Put convenience. Internally the call is wrapped in `withAutoSync`: if `cfg.AutoSync` is true, the SDK calls `store.Fetch` before the put and `store.Push` after; otherwise it just runs the put. The fetch and push are blocking and synchronous against the same goroutine.

`Patch` (and `PatchJSON`) takes RFC 6902 patch operations and applies them through `docapp.NewPatchService`. `Delete` writes a tombstone. `Revert(ctx, collection, docID, opts)` rewinds the document by writing a new snapshot whose contents match an earlier transaction identified by `TxID` or `TxHash`.

`Log(ctx, collection, docID)` returns the full document history as `[]LogEntry`. `LogPaginated(ctx, collection, docID, opts)` returns a `LogPage` with `Entries` and `NextCursor`; `opts.Cursor` is the opaque base64 token returned as `NextCursor` from a previous call (the same shape used by the CLI; see `docapp.LogOptions`). The cursor is meaningful only for the same doc on the same repository state; the SDK returns `ErrInvalidCursor` if it is corrupted or version-mismatched.

`Fetch(ctx)` and `Push(ctx)` are exposed for callers that want explicit control of replication and have therefore set `AutoSync: false`. They are thin wrappers over `store.Fetch` and `store.Push`.

## The Query surface

[`pkg/ledgerdbsdk/index.go`](../tree/main/pkg/ledgerdbsdk/index.go) exposes three query primitives. `DB()` returns the underlying `*sql.DB` once `OpenIndex` has succeeded. `Query(ctx, sql, args...)` is a thin pass-through to `db.QueryContext`. Both are for callers who want the full `database/sql` surface — streaming `*sql.Rows`, custom scan, prepared statements — without the SDK pretending to be an ORM.

`QueryPaginated(ctx, sql, args, cursor, limit)` is the cursor-paginated variant. It enforces three constraints: the query must include `ORDER BY` (best-effort textual check via `hasOrderBy`) or it returns `ErrMissingOrderBy`; the query must not include its own `LIMIT`/`OFFSET` because the SDK appends them; and the cursor must be one the SDK itself emitted (it returns `ErrInvalidCursor` for anything else). Rows are materialised into `[]QueryRow` (`map[string]any`) because pagination already implies bounded page size; use `Query()` for streaming reads. The default page size when `limit <= 0` is `DefaultQueryPageLimit = 100`. Cursors are versioned (`QueryCursorVersion = 1`) and base64-encoded JSON of `{ v, off }`; older cursors will be rejected at decode time when the schema changes.

The query path routes through the SQLite sidecar — there is no query path that goes against the git store directly. That is the load-bearing decision behind the SDK's `Open` lifecycle: a `Client` with no index opened can do all the document operations but not the query operations, and any `Query*` call against such a client returns `ErrIndexNotOpen` from `indexDB()`.

`GetIndexed(ctx, collection, docID)` is the key-value read against the SQLite mirror rather than against the git store. It is the right call when the watch loop has already applied the relevant transactions and the caller wants the latency of a SQLite point lookup. `GetIndexedInto` is the unmarshal-into convenience. Both surface `ErrNotFound` when the row is absent and `ErrIndexNotOpen` when the index has not been opened.

## The Index watch lifecycle

`SyncIndex(ctx)` runs a single index sync and returns an `IndexSyncResult` summary. It is the right entry point for callers who want to drive the sync cadence themselves — for example, from a custom scheduler — without standing up the polling goroutine.

`StartIndexWatch(ctx)` is the polling form. It validates that `Interval > 0` and `Jitter >= 0`, returns `ErrWatchRunning` if a watch is already active, and otherwise spawns the goroutine described in the lifecycle section above. Multiple watches per client are not supported; one repository, one watcher.

`StopIndexWatch()` cancels the watch context, waits for the goroutine to drain the error channel, and returns any non-`Canceled` error. `Close()` calls it for you. The contract is that after `Close()` returns, the SQLite handle is gone and `WatchResults()` returns `nil`.

## Error model

[`pkg/ledgerdbsdk/errors.go`](../tree/main/pkg/ledgerdbsdk/errors.go) defines a small sentinel set: `ErrRepoPathRequired`, `ErrIndexNotOpen`, `ErrWatchRunning`, `ErrNotFound`, `ErrManifestMismatch`, `ErrInvalidCursor`, `ErrMissingOrderBy`. They are the values to `errors.Is` against. Document-layer errors from `internal/app/doc` are translated through `mapDocErr` so the SDK boundary returns SDK errors; `ErrDocNotFound` becomes `ErrNotFound`, `ErrInvalidCursor` propagates as such. Other errors pass through as-is because they are typically unwrapped operating-system or git errors that the caller may want to inspect.

The SDK does not invent new error categories beyond what the underlying services return. If a write fails because the manifest disallows it, the underlying domain error surfaces; if a SQLite query fails because the table does not exist (i.e. the watch has not run against a new collection yet), the `*sql` error surfaces. Callers should not over-classify.

## Concurrency model

A single `Client` serialises through one repository on disk. Document writes go through `gitrepo.Store`, which in turn invokes the `git` binary; concurrent writes from the same client are not coordinated by the SDK and will collide on `.git/index.lock`. The pragmatic guarantee is: one Client, one writer goroutine. Reads are cheap and concurrent at the SQLite layer (the sidecar is opened in WAL mode), but the underlying git store is still subject to the same lock for any path that touches the working tree.

The watch loop is the one exception. It runs in its own goroutine and synchronises with the rest of the client through the SQLite-store mutex and through Go channels. As long as the watch is reading and the foreground is writing, they do not contend; both go through the per-table SQLite locks and the small mutex on `Client.mu` for the store handles.

## `pkg/ledgerdbsdk/` versus `pkg/ledgerdb/`

The lower-level core package at `pkg/ledgerdb/` is two files. `doc.go` is a package comment. `execute.go` exports one function: `Execute() int`, which forwards to `internal/cli.Execute()`. Its real purpose is to be the build root for `-buildmode=archive` and `-buildmode=shared` so a foreign binary can link the LedgerDB CLI as a library (see the `build-core` and `build-core-shared` targets in the [`Makefile`](../tree/main/Makefile)). It is not the embedder API. Anything in a Go program that wants `Get`, `Put`, `Query`, `Log` should depend on `pkg/ledgerdbsdk/` instead and never reach into `internal/`.

## Worked example

The following program opens a repository, applies a collection, writes a handful of documents, runs a paginated query against the SQLite sidecar, and exits cleanly.

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os/exec"
    "time"

    "github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

func main() {
    ctx := context.Background()

    // Collection apply is not (yet) part of the Go SDK surface; shell out for it.
    must(exec.Command("ledgerdb", "--repo", "./data.git",
        "collection", "apply", "tasks",
        "--schema", "./schemas/task.json",
        "--indexes", "status,assignee").Run())

    cfg := ledgerdbsdk.DefaultConfig("./data.git")
    cfg.AutoWatch = true
    client, err := ledgerdbsdk.Open(ctx, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    for i := 0; i < 5; i++ {
        if _, err := client.PutJSON(ctx, "tasks", fmt.Sprintf("t%d", i), map[string]any{
            "title":  fmt.Sprintf("task %d", i),
            "status": "open",
        }); err != nil {
            log.Fatal(err)
        }
    }

    // Wait for the watcher to apply the new commits.
    select {
    case <-client.WatchResults():
    case <-time.After(2 * time.Second):
    }

    rows, next, err := client.QueryPaginated(ctx,
        `SELECT doc_id, payload FROM collection_tasks WHERE json_extract(payload,'$.status') = ? ORDER BY doc_id`,
        []any{"open"}, "", 3)
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range rows {
        var payload map[string]any
        _ = json.Unmarshal(row["payload"].([]byte), &payload)
        log.Println(row["doc_id"], payload["title"])
    }
    log.Println("next cursor:", next)
}

func must(err error) { if err != nil { log.Fatal(err) } }
```

The example shells out for `collection apply` because the current `pkg/ledgerdbsdk/` does not expose a `Collection.Apply` method; the document and query surface is what is in the SDK today, and collection management is on the CLI path. That is one of the two places where the Go SDK is currently less than complete with respect to the CLI; the other is the disaster-recovery commands (`backup`, `restore`, `truncate`), which are also CLI-only today.

## See also

- [SDK CLI Reference](SDK-CLI-Reference) — the canonical surface for everything in this package.
- [SDK TypeScript SDK](SDK-TypeScript-SDK) — the Node bridge that shells out to the CLI; useful contrast.
- [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain) — how the same query path is used interactively.
- [Querying and Indexing Strategy](Concepts-Indexing) — design context for the SQLite sidecar.
- [Versioning and Conflict Resolution](Concepts-Versioning-And-Causality) — the per-doc stream model behind `Get`/`Put`/`Patch`.
