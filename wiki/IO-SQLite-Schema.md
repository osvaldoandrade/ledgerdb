# IO SQLite Schema

The SQLite sidecar is the only piece of LedgerDB persistence that is not in the git repository. It is a single file — typically `<repo>/index.db` — that holds a materialised projection of the `state/` tree into rows that a query engine can scan. Everything in it is derivable from the git repository, so it is treated as a cache: if it gets corrupted or schema-drifts, the operator deletes it and rebuilds with `ledgerdb index watch`. This page documents the tables the sidecar emits, the DDL strings that produce them, and how secondary indexes and `EXPLAIN QUERY PLAN` interact.

The implementation is a single file: `internal/infra/sqliteindex/store.go`. It is roughly 460 lines and depends only on `modernc.org/sqlite` (a pure-Go SQLite that avoids cgo) and the application-level interfaces declared in `internal/app/index/ports.go`. The sidecar never opens more than one connection (`store.go:47-48`) because SQLite's write serialisation is a single writer regardless, and pinning to one connection makes the pragma state and the table cache predictable.

## What the sidecar is for

The sidecar is read-mostly. Writes happen only during `ledgerdb index watch`, which decodes the TxV3 blobs from the `state/` tree (or the `documents/` tree in history mode) and upserts them into the per-collection tables. Reads happen from the application code that runs SQL queries against the file directly. There is no LedgerDB-mediated query API on top of the sidecar; clients open `<repo>/index.db` with their own SQLite driver and issue SQL.

This is deliberate. The sidecar's job is to make the indexed view of `state/` accessible to anything that can speak SQL — Python notebooks, BI tools, Go services with `database/sql`, JavaScript with `better-sqlite3`. The git repository is the source of truth; the sidecar is the queryable lens.

## The bookkeeping tables

Two tables exist regardless of how many collections are in the database. Both are created by `Store.initSchema` at `internal/infra/sqliteindex/store.go:148-178`.

### `ledger_index_state`

```sql
CREATE TABLE IF NOT EXISTS ledger_index_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_commit     TEXT NOT NULL DEFAULT '',
    last_state_tree TEXT NOT NULL DEFAULT ''
);
```

The `CHECK (id = 1)` makes this a singleton table. There is always exactly one row. `last_commit` is the SHA-1 of the most recent commit the indexer applied, and `last_state_tree` is the SHA-1 of the `state/` subtree that produced it. The indexer reads this row on every sync (`Store.GetState` at `store.go:76-87`) to decide where to resume from, and writes it back at the end (`store.SetState` at `store.go:396-406`).

The `last_state_tree` column was added after the initial schema. `ensureStateColumns` (`store.go:180-212`) handles the migration: it inspects `PRAGMA table_info(ledger_index_state)`, sees the column is missing, and runs `ALTER TABLE ... ADD COLUMN`. This is the only schema migration the sidecar has ever needed. The indexer remains correct without it — the state-mode path falls back to the cheap path on the second sync — but the column being present is what allows the first sync to short-circuit.

### `collection_registry`

```sql
CREATE TABLE IF NOT EXISTS collection_registry (
    collection TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE
);
```

This is the map from logical collection name (the value of the TxV3 `collection` field) to the SQLite table name that holds its rows. The table name is derived as `"collection_" + collection` by `tableNameForCollection` (`store.go:449-451`); the indirection exists so that future renaming or sharding can change the table without rewriting every consumer.

The collection is registered the first time the indexer sees a transaction touching it. `storeTx.EnsureCollection` (`store.go:297-318`) first consults its in-memory `tableCache`, then queries `collection_registry`, and only creates the table if neither finds it. This makes registration idempotent across restarts and across concurrent sync attempts (which are serialised by SQLite's single writer anyway).

## The per-collection tables

For every collection the indexer has ever seen, one table exists with a fixed shape. `storeTx.createCollectionTable` at `store.go:430-447`:

```sql
CREATE TABLE IF NOT EXISTS "collection_<name>" (
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

The columns mirror the TxV3 fields plus the materialised payload:

- `doc_id` — the document key, primary key of the table.
- `payload` — the canonicalised JSON snapshot bytes. Stored as BLOB rather than TEXT because the canonicaliser produces UTF-8 bytes and SQLite's TEXT affinity has its own collation rules that the indexer wants to bypass.
- `tx_hash` — the SHA-256 of the source TxV3 blob (see `internal/infra/hash/sha256.go`). This is how an integrity check can correlate a sidecar row back to the canonical wire bytes.
- `tx_id` — the ULID from the source transaction.
- `op` — the string form of the op (`"put"`, `"patch"`, `"merge"`, `"delete"`), produced by `domain.TxOp.String()` at `internal/domain/tx.go:42-55`.
- `schema_version` — the opaque application schema marker carried verbatim.
- `updated_at` — the unix-nanos timestamp from the source transaction.
- `deleted` — `0` for live documents, `1` for tombstones. A tombstone is kept rather than deleted from the table so consumers can detect deletion explicitly.

The upsert is `INSERT ... ON CONFLICT(doc_id) DO UPDATE SET ...` (`store.go:362-394`). Because `doc_id` is the primary key, a re-replay of the same transaction is a no-op against this table — the row's `tx_hash` would just be set to the same value. This is what makes the indexer safe to re-run from any earlier `last_commit`.

There is no foreign key from per-collection tables back to `collection_registry`, even though `PRAGMA foreign_keys = ON` is enabled at `store.go:149-151`. The registry exists for naming, not for referential integrity. The tables exist independently and would still be queryable if the registry were dropped (though `Store.Reset` would lose its enumeration source — see below).

## Secondary indexes

Collections may declare secondary indexes in their `collections/<name>/indexes.json` file inside the git repository. The shape is documented in `internal/domain/index.go:7-11`:

```go
type IndexSpec struct {
    Name   string   `json:"name"`
    Fields []string `json:"fields"`
    Unique bool     `json:"unique,omitempty"`
}
```

When the indexer first observes a collection during a sync, it calls `s.materializeIndexes` (`internal/app/index/service.go:424-436`), which reads the index specs from the git repo via `gitrepo.Store.ReadCollectionIndexes` (`internal/infra/gitrepo/collection.go:55-68`) and forwards them to `storeTx.EnsureIndexes` (`store.go:239-260`). The DDL emitted per spec is built by `buildIndexStatement` at `store.go:262-287`:

```sql
CREATE [UNIQUE] INDEX IF NOT EXISTS "idx_<table>_<name>"
    ON "<table>" (json_extract(payload, '$.field1'), json_extract(payload, '$.field2'), ...);
```

The `json_extract(payload, '$.field')` expression is the load-bearing part. The `payload` column is a JSON BLOB; the indexer does not normalise per-field columns out of it. Instead, every secondary index is an *expression index* over `json_extract` calls, which SQLite stores as a B-tree of the extracted scalar values. Composite indexes are just multi-column expression indexes — `CREATE INDEX ... ON table (json_extract(payload, '$.a'), json_extract(payload, '$.b'))`.

The `UNIQUE` modifier becomes a `UNIQUE` index, which means an attempted upsert that would violate the constraint fails the SQLite transaction. The indexer surfaces this back through the sync error path.

The naming convention `idx_<table>_<name>` keeps every index in a flat namespace within the database, avoids collisions across collections, and makes the names predictable for an operator inspecting the schema directly with `.schema` in the `sqlite3` shell.

The single-column legacy form is supported: an entry that is just a bare string in `indexes.json` is parsed by `collection.decodeIndexItem` (`internal/app/collection/indexes.go:46-66`) as a single-column index named after the field. The richer object form is preferred for new collections because it carries a stable name independent of the field path.

## What the indexer writes per op

The per-tx loop is `SyncService.applyTxs` (`internal/app/index/service.go:306-369`):

- **PUT** — canonicalise `tx.Snapshot`, build a `DocRecord` with `Deleted=false`, upsert.
- **PATCH** — read the existing document, apply the JSON patch to its payload, canonicalise, upsert. If the document does not exist or is tombstoned, return `ErrMissingDocument`.
- **MERGE** — if `tx.Snapshot` is set, treat as PUT; otherwise treat as PATCH.
- **DELETE** — upsert with `Deleted=true`, `payload=nil`.

In state-mode sync, the indexer never sees `PATCH` or `MERGE` ops on the `state/` blobs because the writer materialises every change into a `PUT` snapshot before mirroring. The PATCH and MERGE code paths are only exercised in history-mode sync.

## Pragmas

The `Fast` open option enables four pragmas (`store.go:214-232`):

```sql
PRAGMA journal_mode = WAL;     -- write-ahead log instead of rollback journal
PRAGMA synchronous = NORMAL;   -- fsync at commit, not on every page write
PRAGMA temp_store = MEMORY;    -- temp B-trees in RAM
PRAGMA cache_size = -20000;    -- ~20 MiB page cache
```

The default (no `Fast`) leaves SQLite at its built-in defaults: rollback journal, `synchronous=FULL`, disk-based temp storage. The default is the right choice for one-shot CLI operations where the sidecar is opened, written to, and closed within seconds. `Fast` is the right choice for `index watch` running as a long-lived process, where WAL mode and the larger cache amortise across many syncs.

`PRAGMA foreign_keys = ON` is always set (`store.go:149-151`). It is a no-op for the current schema (no foreign keys declared) but is set defensively in case future migrations introduce them.

## Reset

`Store.Reset` (`store.go:97-146`) is the operation that empties the sidecar without deleting the file. It iterates `collection_registry`, drops each per-collection table, clears the registry, and resets the `ledger_index_state` row to zeros. After Reset the sidecar is structurally a fresh database; the next sync will repopulate from `LastCommit=""` (a full state-tree scan).

Reset is invoked automatically when the indexer's `LastCommit` references a commit that no longer exists in the git repository — typically because of a `truncate` that rewrote history. The indexer returns `ErrCommitNotFound`, and `SyncService.syncInternal` at `internal/app/index/service.go:126-138` calls Reset (with `opts.AllowReset`) and re-runs the sync from scratch.

## `ledgerdb query explain`

The CLI command at `internal/cli/cmd_query_explain.go:14-93` is a thin wrapper around `EXPLAIN QUERY PLAN`. It opens the sidecar read-only (`file:<path>?mode=ro`), runs `EXPLAIN QUERY PLAN <user-sql>`, and prints the three-column result (id, parent, detail) either as a table or as JSON.

The reason this is the right primitive is that all the sidecar's query intelligence is just SQLite. There is no LedgerDB query planner; there is no override of how SQLite picks indexes. Whether a query uses an index, scans a table, or relies on an automatic covering index is entirely SQLite's decision, and `EXPLAIN QUERY PLAN` is the standard way to see what it picked. An operator who has declared a composite index on `(status, created_at)` and wants to confirm that a query `WHERE status='ACTIVE' AND created_at > ?` uses it just runs:

```
$ ledgerdb query explain "SELECT * FROM collection_users WHERE json_extract(payload,'$.status')='ACTIVE'"
id  parent  detail
2   0       SEARCH collection_users USING INDEX idx_collection_users_status (...)
```

Because the indexes are expression indexes over `json_extract`, the query must use `json_extract` in the WHERE clause to hit them. A query that writes `WHERE payload->>'status' = 'ACTIVE'` will not use the expression index because the expression does not match textually. This is a SQLite limitation; the workaround is to write queries that mirror the indexed expression exactly.

## Reads from outside

The sidecar is a regular SQLite file. Any consumer can open it with any SQLite driver. The schema is stable across LedgerDB minor versions; column additions are backward-compatible and indexes added by the indexer use `IF NOT EXISTS`. Consumers should not write to the sidecar — concurrent writes from outside would race against `ledgerdb index watch` — but multiple readers are fine, and read transactions tolerate concurrent writers under WAL mode.

The sidecar file lives wherever the operator put it. The CLI defaults to `<repo>/index.db` (`resolveSidecarPath` at `internal/cli/cmd_query_explain.go:53-61`) but `--db` accepts an explicit path. It is reasonable to keep the sidecar inside the repo directory for single-machine use and outside the repo for shared-storage deployments where the git repo is on a slow filesystem and the sidecar is on a faster one.

## What this page does not cover

The TxV3 protobuf format the indexer decodes from is on [IO-TxV3-Format](IO-TxV3-Format). The git tree the indexer reads from is on [IO-Git-Object-Layout](IO-Git-Object-Layout) and [IO-State-Tree](IO-State-Tree). The CLI for running indexing in a loop, the metrics, and the audit log are on [Operations-and-CLI-Strategy](SDK-CLI-Reference).

The application-level query patterns and the index-design guidance for collections — what to index, when to make an index `UNIQUE`, how to evolve the index set safely — are on [Querying-and-Indexing-Strategy](Concepts-Indexing).

## See also

- [IO-Overview](IO-Overview)
- [IO-State-Tree](IO-State-Tree)
- [IO-TxV3-Format](IO-TxV3-Format)
- [Querying-and-Indexing-Strategy](Concepts-Indexing)
