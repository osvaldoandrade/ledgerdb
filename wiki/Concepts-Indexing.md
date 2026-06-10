# Indexing

A LedgerDB repository on its own can answer "give me document X by ID" cheaply. It cannot answer "give me all documents where `org_id = 42` and `status = active`" without scanning every document in the collection. The SQLite sidecar exists to close that gap. It is an external file maintained by `ledgerdb index sync` (one-shot) or `ledgerdb index watch` (continuous) that mirrors the current document state of each collection into a SQL table, declares the indexes the collection's `indexes.json` requested, and lets ad-hoc queries run against ordinary SQL.

The sidecar is not the source of truth. The git repository is. The SQLite is a derived projection, rebuildable from scratch at any time. This page explains how the projection is built, what shape it takes on disk, what the two sync modes (state and history) do differently, and how the watch loop interacts with declared indexes, the audit log, and the optional Prometheus endpoint.

## What this page covers

The SQLite sidecar (`internal/infra/sqliteindex/store.go`), the sync service (`internal/app/index/service.go`), the `watch` command including its observer hooks for metrics and audit, and the declared-index pipeline that turns `indexes.json` entries into `CREATE INDEX` statements. It does not cover the on-disk layout of the source data — that is [Storage Layout](Concepts-Storage-Layout) — nor the transaction format that is being decoded — that is [Transactions and TxV3](Concepts-Transactions-And-TxV3).

## What it does not cover

The SQL query surface itself — column names, the `ledgerdb query` CLI command, and the SDK's Go-side query helpers — belongs to the SDK and CLI reference. This page is the conceptual map from "the ledger" to "a queryable database".

## The SQLite schema

`sqliteindex.Store.initSchema` (`internal/infra/sqliteindex/store.go:148-178`) creates two bootstrap tables on first open:

```sql
CREATE TABLE IF NOT EXISTS ledger_index_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_commit     TEXT NOT NULL DEFAULT '',
    last_state_tree TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS collection_registry (
    collection TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE
);
```

`ledger_index_state` is a single-row table tracking the last commit hash and last state-tree hash the indexer applied. The sync service reads it (`Store.GetState` at `store.go:76-87`) to know where to resume from, and writes it back at the end of every successful batch (`StoreTx.SetState` at `store.go:396-406`). The state-tree hash is used by the state-mode sync to detect a no-op pull (`StateTxsSince` at `internal/infra/gitrepo/index_source.go:169-171`).

`collection_registry` maps a logical collection name to its physical SQL table name. The table name is `collection_<name>` (`tableNameForCollection` at `store.go:449-451`). When a transaction for a new collection arrives, the indexer calls `StoreTx.EnsureCollection` (`store.go:297-318`), which creates the per-collection table on demand:

```sql
CREATE TABLE IF NOT EXISTS "collection_users" (
    doc_id         TEXT PRIMARY KEY,
    payload        BLOB,
    tx_hash        TEXT NOT NULL,
    tx_id          TEXT NOT NULL,
    op             TEXT NOT NULL,
    schema_version TEXT,
    updated_at     INTEGER NOT NULL,
    deleted        INTEGER NOT NULL CHECK (deleted IN (0, 1))
);
```

Each row in the collection table is one document's current state. The `payload` column holds the canonical JSON bytes; SQLite's JSON1 extension can index into them via `json_extract(payload, '$.field')`. `doc_id` is the primary key, so upserts (`StoreTx.UpsertDoc` at `store.go:354-394`) use SQLite's `ON CONFLICT(doc_id) DO UPDATE SET ...` pattern to replace in place. Deletes set `deleted = 1` rather than removing the row, preserving the tombstone for consumers that want to see tombstones; the SDK's query path filters them by default.

## Declared indexes

The `EnsureIndexes` method (`store.go:239-260`) is what turns `collections/<name>/indexes.json` into actual SQL indexes. The IndexSpecReader (`internal/infra/gitrepo/collection.go:55-68`) loads the JSON, the index service calls `EnsureIndexes` per collection at sync time (`internal/app/index/service.go:424-436`), and the SQLite store emits one statement per spec:

```go
return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
    unique,
    quoteIdent(indexName),
    quoteIdent(tableName),
    strings.Join(exprs, ", "),
), nil
```

For a single-field spec `{"name": "email", "fields": ["email"]}` against the users collection, the result is:

```sql
CREATE INDEX IF NOT EXISTS "idx_collection_users_email"
ON "collection_users" (json_extract(payload, '$.email'));
```

For a composite unique spec `{"name": "org_email", "fields": ["org_id", "email"], "unique": true}`:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS "idx_collection_users_org_email"
ON "collection_users" (json_extract(payload, '$.org_id'), json_extract(payload, '$.email'));
```

The composite and unique forms were added in commit `cdc4a85` (`feat(collection): support composite indexes with unique modifier`). SQLite enforces the `UNIQUE` constraint during the upsert in the next sync iteration — if the constraint is violated by an applied transaction, the upsert errors out and the whole batch rolls back. The error then propagates up through `Sync`, and the indexer stops. There is no auto-resolution; the operator's tools are `ledgerdb integrity verify` (to find the offending data) and re-running `ledgerdb collection apply` to relax the constraint if it was declared in error.

Indexes are materialised lazily — `EnsureIndexes` runs once per collection per sync invocation, when the indexer first sees a transaction for that collection (`SyncService.applyTxs` at `internal/app/index/service.go:312-317`). On a fresh database where the schema has never been applied, no indexes exist; the first sync that processes a tx for that collection creates them. On a sidecar that is being reset via `Store.Reset` (`store.go:97-146`), all collection tables are dropped, and the next sync recreates both tables and indexes.

## State mode vs history mode

There are two ways the indexer can build the projection. The mode is controlled by `--mode` on the CLI (`internal/cli/commands.go:425`) and defaults to `state` since the state tree was introduced.

**State mode** reads the materialised `state/` subtree directly. The source `Store.StateTxsSince` (`internal/infra/gitrepo/index_source.go:122-252`) compares the current state-tree hash to the last-recorded one. If they match, the indexer returns early — no transactions were applied. If they differ, it walks `object.DiffTreeContext` between the two state trees and emits one `CommitTx` per added or modified tx file. Deletes in the state tree are skipped (the `merkletrie.Delete` case at line 234) because the state tree drops the file on document deletion, not a `DELETE` tx; deletes show up as `MERGE` ops on the history side and the state side keeps a tombstone tx instead.

State mode is fast because it reads exactly the current state — no history replay, no patch application, no chain walking. A clone that re-indexes from scratch processes one tx per document, not one per historical revision.

**History mode** reads the commit graph. `Store.ListCommitHashes` (`internal/infra/gitrepo/index_source.go:62-112`) returns every commit since the last-applied one. For each commit, `Store.CommitTxs` (`internal/infra/gitrepo/index_source.go:114-116`, calling `commitTxsForRoot` with the documents root) diffs the commit's tree against its parent and returns the added or modified tx files under `documents/`. The indexer decodes each, sorts by `(timestamp, tx_id)` (`SyncService.decodeTxs` at `internal/app/index/service.go:283-304`), and applies them.

History mode is necessary when you want every transaction to flow through the observer pipeline — for audit logging, for downstream replay, for migrations that operate at the transaction level. The performance cost is roughly N times state mode when N is the average history length per document.

Both modes share the same `applyTxs` loop (`internal/app/index/service.go:306-370`) and the same `UpsertDoc` write path. The difference is purely in what transactions get fed in. State mode produces a sparser stream (only the latest state per changed doc since last sync); history mode produces every transaction since last sync.

## The watch loop

`ledgerdb index watch` (`internal/cli/commands.go:432-593`) is a foreground process that loops: sync, sleep, sync, sleep, until the user cancels it. The loop body is straightforward (`commands.go:537-572`):

```go
for {
    result, err := service.Sync(ctx, repoPath, syncOptions)
    if err != nil { return err }
    if !quiet && (!onlyChanges || hasIndexChanges(result)) {
        writeIndexSyncResult(...)
    }
    if once { return nil }
    wait := interval
    if jitter > 0 { wait += rand.Int63n(int64(jitter)) }
    select {
    case <-ctx.Done(): return nil
    case <-time.After(wait):
    }
}
```

The default interval is 5 seconds. The `--jitter` flag spreads the wake-up time of multiple watchers so they do not all hit the same remote at the same wall-clock tick.

The `--fetch` flag (default true) makes each iteration's `Sync` call a `git fetch` against `origin` before walking the new commits. This is what makes the sidecar follow a remote primary writer; without it, the watch only sees local commits.

The `--batch-commits` flag (default 1) groups multiple commits into one SQLite transaction. For high-volume history-mode replays, raising this to 10 or 100 reduces per-commit `BEGIN`/`COMMIT` overhead, at the cost of larger memory footprint and longer rollback windows on error.

The `--fast` flag turns on aggressive SQLite pragmas (`applyPragmas` at `store.go:214-232`): `journal_mode=WAL`, `synchronous=NORMAL`, `temp_store=MEMORY`, `cache_size=-20000` (20MB cache). This trades a few milliseconds of crash-safety for a substantial throughput boost on bulk indexing. The default leaves the pragmas alone — full sync mode — so the sidecar survives a crash without losing committed transactions.

## Metrics and audit

The watch command has two opt-in observability surfaces, both wired through the `Observer` interface (`internal/app/index/ports.go:70-75`):

```go
type Observer interface {
    OnTxApplied(event TxEvent)
    OnSyncError(collection, reason string)
    OnSyncDuration(seconds float64)
    OnReplicationFetch(commitsObserved int)
}
```

`--metrics-addr` enables a Prometheus endpoint on `127.0.0.1:9090` (or the operator-specified host/port). The implementation in `internal/app/index/metrics.go` builds a private `prometheus.Registry` — not the global default — so it does not collide with other Prometheus collectors that might be embedded in the same binary. The collectors exported are:

- `ledgerdb_tx_applied_total{collection}` — counter
- `ledgerdb_sync_errors_total{collection,reason}` — counter, with `reason` drawn from `classifyErr` (`internal/app/index/service.go:95-116`): `canceled`, `commit_not_found`, `fetch_unavailable`, `missing_document`, `patch_unsupported`, `state_unavailable`, `merge_unsupported`, `other`
- `ledgerdb_replication_lag_seconds` — gauge, reset to zero when a fetch brings in new commits and ticking up otherwise
- `ledgerdb_index_sync_duration_seconds` — histogram, observed at the top of every Sync call (`SyncService.Sync` at `internal/app/index/service.go:51-63`)
- `ledgerdb_cas_retries_observed_total` — counter

The endpoint refuses to bind to non-loopback addresses unless `--metrics-allow-public` is set (`assertLoopback` at `metrics.go:175-188`). The default posture matches the rest of LedgerDB v0.2.x: loopback-only by default.

`--audit-log <path>` enables a JSON Lines append-only file (or stdout when `path == "-"`). Implementation at `internal/app/index/audit.go`. Every applied transaction emits one record:

```json
{"ts":"2026-06-10T12:34:56.789Z","tx_id":"01H...","collection":"users","doc_id":"alice","op":"patch","actor":"index-watch"}
```

The `actor` field is always `index-watch` — the audit log is tied to this command, not the originating writer (which the indexer cannot determine from the tx). A background goroutine flushes the buffered writer on a tick (default 1 second; configurable via `--audit-flush-interval`). The audit logger only fills in `OnTxApplied`; the other observer hooks are deliberate no-ops so the audit file remains a record of *what was applied*, not *how the sync went*.

When both `--metrics-addr` and `--audit-log` are set, the CLI combines them with `NewCombinedObserver` (`internal/app/index/audit.go:166-189`), which fans out callbacks to both. The combine is safe because all observers are required to be non-blocking — they run inside the sync transaction.

## Sync, fetch, and reset

The two SQLite-side errors that signal "we lost track of where we are" are `ErrCommitNotFound` and `ErrStateUnavailable`. The first means the recorded `last_commit` no longer exists in the git log — typically because the upstream did a force-push or a history rewrite. The second means the state tree we expected is no longer reachable.

The `--allow-reset` flag (`internal/app/index/types.go:67`) tells the sync service to handle these by wiping the SQLite tables and re-indexing from scratch (`SyncService.syncHistory` at `internal/app/index/service.go:127-138`, `syncState` at `internal/app/index/service.go:219-227`). The CLI ties this to `historyMode == amend` automatically (`internal/cli/commands.go:543`) because amend repositories rewrite history routinely; append repositories would only see this on operator intervention and the default (no reset) is the safer choice — surface the error and let the operator decide.

## What the sidecar is and is not

It is **a derived materialisation** of the latest committed state of every document, plus the declared indexes. It is a SQL queryable surface for analytics, joins, and ad-hoc reads. It is the natural place to hang full-text search, aggregations, or any computation that does not fit in the per-document model.

It is **not** the source of truth. Anything that depends on the sidecar's contents being authoritative is structurally wrong — the sidecar can be deleted, regenerated, or arbitrarily desynchronised, and the git repository is still the canonical record. It is also **not replicated by git**. Two replicas of the same LedgerDB repo each maintain their own sidecar; the sidecars converge by each running watch against the same upstream, not by syncing to each other.

## See also

- [Storage Layout](Concepts-Storage-Layout) for the source `state/` and `documents/` trees
- [Transactions and TxV3](Concepts-Transactions-And-TxV3) for the blob format being decoded
- [Documents and Collections](Concepts-Documents-And-Collections) for how `indexes.json` is declared
- [Replication](Concepts-Replication) for how the watch loop interacts with `git fetch`
- [Architecture Overview](Concepts-Architecture-Overview) for the sidecar's place in the full call stack
