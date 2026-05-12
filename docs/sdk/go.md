# Go SDK (`pkg/ledgerdbsdk`)

The Go SDK is the **first-class, in-process** binding for LedgerDB. It calls
the same domain services that the `ledgerdb` CLI uses — there is no IPC, no
external binary, and no FFI.

- Import path: `github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk`
- Source: [`pkg/ledgerdbsdk/`](../../pkg/ledgerdbsdk/)
- Tier: Tier 1 (see [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)).

## Install

```bash
go get github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk
```

Go 1.21+ is recommended. The SDK pulls in `go-git`, the SQLite driver, and
the LedgerDB core packages transitively.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

type Task struct {
	Title  string `json:"title"`
	Status string `json:"status"`
}

func main() {
	ctx := context.Background()
	cfg := ledgerdbsdk.DefaultConfig("/path/to/ledgerdb.git")
	cfg.AutoWatch = true // keep SQLite sidecar fresh in the background

	client, err := ledgerdbsdk.Open(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if _, err := client.PutJSON(ctx, "tasks", "task_0001", Task{
		Title:  "Ship v1",
		Status: "todo",
	}); err != nil {
		log.Fatal(err)
	}

	var t Task
	if _, err := client.GetInto(ctx, "tasks", "task_0001", &t); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", t)
}
```

## Constructing a client

There are two entry points:

- `ledgerdbsdk.New(cfg)` — build a client without opening the SQLite sidecar.
  Use this when you only need raw ledger ops (`Put`/`Get`/`Patch`/...).
- `ledgerdbsdk.Open(ctx, cfg)` — build a client *and* open the SQLite index.
  If `cfg.AutoWatch` is true, this also starts the index watch loop.

`Config` is defined in [`pkg/ledgerdbsdk/config.go`](../../pkg/ledgerdbsdk/config.go):

```go
cfg := ledgerdbsdk.Config{
	RepoPath:    "/srv/ledgers/tasks.git",
	AutoSync:    true,    // fetch before writes, push after writes
	AutoWatch:   true,    // start index watch on Open
	SignCommits: false,   // GPG-sign commits (requires SignKey)

	StreamLayout: ledgerdbsdk.StreamLayoutSharded, // or Flat
	HistoryMode:  ledgerdbsdk.HistoryModeAppend,   // or Amend

	Index: ledgerdbsdk.IndexConfig{
		Mode:         ledgerdbsdk.IndexModeState, // or History
		Interval:     time.Second,
		BatchCommits: 200,
		Fast:         true,
		Fetch:        true,
		OnlyChanges:  true,
		EmitResults:  true,
	},
}
```

`DefaultConfig(repoPath)` returns sensible defaults for near real-time
indexing — start there and tweak only what you need.

## Document operations

### Put a snapshot

```go
// Raw bytes (already JSON-encoded):
res, err := client.Put(ctx, "tasks", "task_0001", []byte(`{"title":"Ship v1"}`))

// Or marshal a Go value for you:
res, err = client.PutJSON(ctx, "tasks", "task_0001", Task{Title: "Ship v1"})

fmt.Println(res.CommitHash, res.TxHash, res.TxID)
```

### Get a document

```go
doc, err := client.Get(ctx, "tasks", "task_0001")
// doc.Payload is json.RawMessage; doc.Op is "put" | "patch" | "delete".

var t Task
meta, err := client.GetInto(ctx, "tasks", "task_0001", &t)
_ = meta // meta.TxHash, meta.TxID, meta.Op
```

### Patch (RFC 6902)

```go
ops := []map[string]any{
	{"op": "replace", "path": "/status", "value": "done"},
}
if _, err := client.PatchJSON(ctx, "tasks", "task_0001", ops); err != nil {
	log.Fatal(err)
}
```

### Delete (tombstone)

```go
if _, err := client.Delete(ctx, "tasks", "task_0001"); err != nil {
	log.Fatal(err)
}
```

`Delete` appends a tombstone transaction. The document's history is preserved
and can still be read with `Log` / `Revert`.

### Log

```go
entries, err := client.Log(ctx, "tasks", "task_0001")
for _, e := range entries {
	fmt.Println(e.Timestamp, e.Op, e.TxID, e.TxHash)
}
```

### Revert

```go
_, err := client.Revert(ctx, "tasks", "task_0001", ledgerdbsdk.RevertOptions{
	TxID: "01HXXXXXXXXXXXXXXXXXXXXXXX",
})
```

Pass either `TxID` or `TxHash` (or both — they must agree). The SDK appends a
new transaction that restores the document to that point.

### Remote sync

When `cfg.AutoSync` is true, writes are wrapped in a `Fetch` → write →
`Push` cycle automatically. You can also drive the remote manually:

```go
_ = client.Fetch(ctx)
_ = client.Push(ctx)
```

## Querying via the SQLite sidecar

The SDK ships an embedded SQLite "sidecar" that mirrors ledger state into a
relational schema (see [`docs/05_QUERYING.md`](../05_QUERYING.md)). Once
`OpenIndex` has been called (which `Open` does for you), you get a standard
`*sql.DB`:

```go
db, _ := client.DB()
rows, err := db.QueryContext(ctx,
	`SELECT doc_id, payload FROM "collection_tasks" WHERE deleted = 0`)
```

Convenience helpers:

```go
// Point read against the sidecar (faster than the ledger Get):
doc, err := client.GetIndexed(ctx, "tasks", "task_0001")

// Run an ad-hoc query:
rows, err := client.Query(ctx,
	`SELECT doc_id FROM "collection_tasks" WHERE updated_at > ?`, since)
```

Manage sync explicitly when you don't want `AutoWatch`:

```go
result, err := client.SyncIndex(ctx)
fmt.Println(result.Commits, result.DocsUpserted)
```

Or run a watch loop and consume events:

```go
if err := client.StartIndexWatch(ctx); err != nil {
	log.Fatal(err)
}
defer client.StopIndexWatch()

for r := range client.WatchResults() {
	fmt.Printf("sync: commits=%d upserts=%d deletes=%d\n",
		r.Commits, r.DocsUpserted, r.DocsDeleted)
}
```

## Error handling

The SDK exports a small set of sentinel errors in
[`pkg/ledgerdbsdk/errors.go`](../../pkg/ledgerdbsdk/errors.go):

| Error                              | When it fires                                                |
| ---------------------------------- | ------------------------------------------------------------ |
| `ledgerdbsdk.ErrRepoPathRequired`  | `Config.RepoPath` is empty.                                  |
| `ledgerdbsdk.ErrIndexNotOpen`      | You called an index method without calling `OpenIndex`.      |
| `ledgerdbsdk.ErrWatchRunning`      | You called `StartIndexWatch` twice without stopping.         |
| `ledgerdbsdk.ErrNotFound`          | The document does not exist (or was tombstoned).             |
| `ledgerdbsdk.ErrManifestMismatch`  | `Config` disagrees with the on-disk `db.yaml` manifest.      |

Use `errors.Is` to branch on these. Other failures (Git, SQLite, IO) bubble
up wrapped; the CLI applies the same taxonomy in
[`internal/cli/errors.go`](../../internal/cli/errors.go), which maps domain
errors to exit codes:

| Kind         | Exit | Example                                            |
| ------------ | ---- | -------------------------------------------------- |
| `internal`   | 1    | Unclassified IO / Git failure.                     |
| `validation` | 2    | Missing collection, bad JSON, invalid hash, ...    |
| `not_found`  | 3    | `docapp.ErrDocNotFound`, `inspectapp.ErrBlobNotFound`. |
| `conflict`   | 4    | `domain.ErrHeadChanged`, `domain.ErrSyncConflict`. |

Library callers can use that same classification by inspecting the wrapped
sentinel with `errors.Is`.

## See also

- [`pkg/ledgerdbsdk/doc.go`](../../pkg/ledgerdbsdk/doc.go) — every public
  ledger operation.
- [`pkg/ledgerdbsdk/index.go`](../../pkg/ledgerdbsdk/index.go) — index sync
  and watch API.
- [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md) — SDK conformance contract.
