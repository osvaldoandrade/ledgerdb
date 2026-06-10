# Get Started: Run With Sidecar Index

The bare repo answers `doc get <collection> <doc_id>` in constant time. Anything more selective — "every task with `status = 'todo'` assigned to `alice`" — needs an index. LedgerDB does not maintain indexes inside the ledger; it produces them out of band into a SQLite sidecar file that any process can read. This page walks through bringing the sidecar up, keeping it fresh, and querying it.

This is the topology to pick when you have one writer (CLI or SDK) and one or more readers that need SQL. The sidecar is single-file SQLite; it lives next to the bare repo, it is recreated from scratch any time you delete it, and it never participates in the write path. The trade is straightforward: queries are fast and arbitrary, but they are eventually consistent with the ledger — exactly as fresh as the last sync.

## 1. What the sidecar is

The sidecar is a SQLite database with one table per collection (`collection_<name>`) and a small set of housekeeping tables that track the last commit and the last state tree synced. Each per-collection table carries `doc_id` (primary key), `payload` (the canonical JSON blob), `tx_hash`, `tx_id`, `op`, `schema_version`, `updated_at` (unix nanoseconds), and `deleted` (0/1). Secondary indexes declared in `indexes.json` materialize as SQLite indexes over `json_extract(payload, '$.field')` so queries that filter on those fields hit an index. The DDL lives in `internal/infra/sqliteindex/`.

The sidecar engine is `modernc.org/sqlite` — pure-Go, no system SQLite required, no CGO. The file works with any SQLite client (`sqlite3 ./index.db`, DBeaver, etc.) because it is a stock SQLite 3 database.

## 2. One-shot sync

`ledgerdb index sync` reads commits from the bare repo, decodes the TxV3 blobs, and applies them to the sidecar. It is a one-shot command: it runs, it exits, the sidecar is fresh up to whatever the bare repo's `refs/heads/main` was when it started.

```bash
ledgerdb --repo ./ledgerdb.git index sync \
  --db ./index.db \
  --mode state \
  --batch-commits 200 \
  --fast
```

The implementation is `internal/app/index/service.go`. The five flags worth knowing on a first read:

- `--db <path>` is the sidecar file. The command refuses to run without it (`MarkFlagRequired` at `internal/cli/commands.go:426`).
- `--fetch` (default `true`) does a `git fetch` against `origin` before applying. Set `--fetch=false` for a fully offline sync.
- `--mode state|history` picks the source: `state` reads the materialized snapshots under `state/<collection>/` and only applies docs whose state hash changed since the last sync; `history` walks every TxV3 commit since the last sync and applies each one in order.
- `--batch-commits N` groups N source commits into one SQLite transaction. Higher values reduce per-tx SQLite overhead at the cost of larger memory and larger rollback windows on error. `200` is a reasonable starting point for medium workloads.
- `--fast` flips SQLite to `PRAGMA synchronous=NORMAL` and `PRAGMA journal_mode=WAL` for faster writes. The trade is that a crash between flush and `fsync` can lose recently-applied sidecar rows; the ledger is unaffected.

When the command finishes it prints (or returns as JSON with `--json`) the number of commits processed, the number of TxV3s applied, the number of docs upserted and deleted, the number of collections touched, and the `last_commit` it advanced the sidecar to. Re-running the command is idempotent and cheap when nothing changed.

## 3. State mode vs history mode

The two modes solve different problems.

`--mode state` (the default — `string(indexapp.ModeState)` at `internal/cli/commands.go:425`) is the recommended mode for near real-time indexing. It compares the current `state/` tree hash against the last one applied, walks only the documents whose entries differ, and writes one upsert per changed doc. The cost is `O(changes)` per sync regardless of how much history accumulated in between. State mode does not replay history; it only ever sees the latest snapshot per document. If you only need to answer "what is true now?", this is what you want.

`--mode history` walks every TxV3 commit between the sidecar's last-seen commit and the current head. It applies them in order, one per call. This is the mode to use when you want the sidecar to observe every individual transition — for example, if you want to compute per-tx counters or feed downstream consumers a strict ordered stream. The cost is `O(commits since last sync)`; for a chatty workload that means more work per sync but no missed events.

When you change collection schemas or alter the index spec, state mode automatically rebuilds the affected tables on the next run. If state mode reports `ErrStateUnavailable` (the materialized state is missing or unusable), the service falls back to history mode for that sync — the fallback is at `internal/app/index/service.go:79`.

## 4. The watch loop

`ledgerdb index watch` runs `sync` on a polling interval. It is the long-lived version of the one-shot command.

```bash
ledgerdb --repo ./ledgerdb.git index watch \
  --db ./index.db \
  --mode state \
  --interval 5s \
  --jitter 1s \
  --batch-commits 200 \
  --fast \
  --only-changes \
  --quiet
```

The full flag set is at `internal/cli/commands.go:575`:

- `--interval D` is the polling cadence. Required unless `--once` is set; the command rejects a non-positive interval with `ErrInvalidInterval`.
- `--jitter D` adds a uniform random delay in `[0, jitter)` to each sleep. Useful when multiple watchers share a remote so they do not stampede.
- `--once` runs a single sync and exits — equivalent to `index sync` but reuses the watch's loop body for parity in scripts.
- `--only-changes` suppresses output for no-op iterations. The default is to emit a result line every poll; `--only-changes` only emits when at least one commit, tx, or doc actually changed.
- `--quiet` suppresses output entirely. Errors are still surfaced.
- `--batch-commits`, `--fast`, `--mode`, `--fetch` behave exactly as in `index sync`.

The watch loop holds the SQLite file open across iterations, so re-opening cost amortizes to zero. The git fetch on each iteration is `git fetch origin` against the remote (`internal/infra/gitrepo/index_source.go:20`); when no remote is configured the fetch is a no-op.

For an application that wants the sidecar always fresh, the typical recipe is `--mode state --interval 1s --jitter 500ms --batch-commits 100 --fast --only-changes`. That produces a sub-second-stale view, keeps SQLite writes batched, and stays quiet when nothing is happening.

## 5. Opt-in metrics

The watch command can expose a Prometheus `/metrics` endpoint. The feature is opt-in via `--metrics-addr`; nothing binds unless you pass it. The implementation landed in commit `7179d14` and lives at `internal/app/index/metrics.go`.

```bash
ledgerdb --repo ./ledgerdb.git index watch \
  --db ./index.db \
  --metrics-addr 127.0.0.1:9090 \
  --interval 1s
```

The endpoint serves the standard Prometheus text format on `/metrics`. The exported collectors are:

- `ledgerdb_tx_applied_total{collection}` — counter, one increment per TxV3 applied to the sidecar.
- `ledgerdb_sync_errors_total{collection,reason}` — counter, labelled by the low-cardinality reason returned by `classifyErr` (`canceled`, `network`, `conflict`, `other`, etc.).
- `ledgerdb_replication_lag_seconds` — gauge, seconds since the last fetch that brought in new commits.
- `ledgerdb_index_sync_duration_seconds` — histogram, wall time per sync iteration.
- `ledgerdb_cas_retries_observed_total` — counter, best-effort count of CAS retries.

Two safety knobs apply. The literal `--metrics-addr auto` resolves to `127.0.0.1:9090` as a convenience. By default the metrics endpoint refuses to bind to any non-loopback host (`internal/app/index/metrics.go:144`, `ErrPublicBindRefused`); pass `--metrics-allow-public` to override. The default posture matches LedgerDB v0.2's "loopback only unless you say otherwise" rule.

Each `Metrics` instance registers its collectors against a private `prometheus.Registry`, not the global default registry. That keeps the CLI well behaved when embedded or invoked repeatedly in tests.

## 6. Opt-in audit log

Alongside metrics, the watch command can write a JSON Lines audit log — one record per TxV3 applied. The feature lives at `internal/app/index/audit.go` and is enabled with `--audit-log`.

```bash
ledgerdb --repo ./ledgerdb.git index watch \
  --db ./index.db \
  --audit-log ./audit.jsonl \
  --audit-flush-interval 1s
```

Each record is a single JSON object on one line:

```json
{"ts":"2026-06-10T14:23:01.123456789Z","tx_id":"01HZ...","collection":"tasks","doc_id":"task_0001","op":"patch","actor":"index-watch"}
```

The actor field is hard-coded to `index-watch` (`auditActor` at `internal/app/index/audit.go:16`) so consumers can tell audit records from this loop apart from any other source. `--audit-log -` writes to stdout instead of a file; any other value is opened with `O_APPEND|O_CREATE|O_WRONLY` mode `0o644`. The buffered writer flushes on `--audit-flush-interval` (default `1s`) and at shutdown.

The audit logger only writes per-tx events — `OnTxApplied`. Sync-level errors deliberately do not appear in the file (see the comment at `internal/app/index/audit.go:132`); they go to slog and to the metrics exporter so the audit file stays a faithful record of applied transactions. Metrics and audit can be enabled together; the watch command composes them through `NewCombinedObserver` (`internal/cli/commands.go:529`).

## 7. Query the sidecar

Once the sidecar has rows, any SQLite client can read it. The CLI ships two query surfaces: `ledgerdb query explain` for plan inspection and the Go SDK's `Client.Query` for runtime queries. The repo also ships an interactive `ledgerdb repl` (introduced in `a7b784c`) for ad-hoc exploration.

### Plan inspection

`ledgerdb query explain "<sql>"` opens the sidecar read-only and runs `EXPLAIN QUERY PLAN` on the given statement. The implementation is at `internal/cli/cmd_query_explain.go`.

```bash
ledgerdb --repo ./ledgerdb.git query explain \
  --db ./index.db \
  "SELECT doc_id, payload FROM collection_tasks WHERE json_extract(payload, '\$.status') = 'todo'"
```

The output is the SQLite plan, indented by parent-child links. If you see `SCAN TABLE collection_tasks` you know the index spec is wrong (or missing) for this query; if you see `SEARCH TABLE collection_tasks USING INDEX idx_tasks_status` the secondary index from `collections/tasks/indexes.json` is being used. The default `--db` resolves to `<repo>/index.db` when omitted (`resolveSidecarPath` at `internal/cli/cmd_query_explain.go:51`).

### Runtime queries via the SDK

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

func main() {
    cfg := ledgerdbsdk.DefaultConfig("./ledgerdb.git")
    cfg.AutoWatch = true
    cfg.Index.Interval = 1 * 1_000_000_000 // 1s in nanoseconds via time.Duration in real code
    cfg.Index.OnlyChanges = true

    ctx := context.Background()
    client, err := ledgerdbsdk.Open(ctx, cfg)
    if err != nil {
        log.Fatalf("open: %v", err)
    }
    defer client.Close()

    rows, err := client.Query(ctx,
        `SELECT doc_id, json_extract(payload, '$.title') AS title
         FROM collection_tasks
         WHERE json_extract(payload, '$.status') = ?
         ORDER BY updated_at DESC
         LIMIT 50`,
        "todo",
    )
    if err != nil {
        log.Fatalf("query: %v", err)
    }
    defer rows.Close()

    for rows.Next() {
        var docID, title string
        if err := rows.Scan(&docID, &title); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("%s\t%s\n", docID, title)
    }
}
```

With `AutoWatch = true`, `Open` opens the sidecar, starts the watch loop in a goroutine, and exposes the result and error channels through `client.WatchResults()` and `client.WatchErrors()` (`pkg/ledgerdbsdk/index.go:291`). For paginated queries use `Client.QueryPaginated`, which requires an `ORDER BY` and returns an opaque base64 cursor (`pkg/ledgerdbsdk/index.go:111`).

For key-value reads against the sidecar — same shape as `doc get` but served from SQLite — use `Client.GetIndexed` (`pkg/ledgerdbsdk/index.go:214`). It returns the canonical payload plus the tx hash, tx id, op, schema version, and deletion flag.

## 8. When to use which mode

```mermaid
flowchart TD
  Q{What do you need?}
  Q -->|Latest state only| S[--mode state]
  Q -->|Every transition observed| H[--mode history]
  S --> SR[O(changes) per sync<br/>cheaper, eventually consistent]
  H --> HR[O(commits) per sync<br/>full event ordering preserved]
  SR --> Common[Set --interval, --jitter,<br/>--only-changes, --batch-commits]
  HR --> Common
  Common --> Maybe[Optional: --metrics-addr,<br/>--audit-log]
```

State mode is the default and the right answer for read-side caches, dashboards, and any consumer that only cares about the current snapshot. History mode is the right answer for analytics pipelines, CDC-style downstream consumers, and any consumer that needs to see every patch in order.

## 9. Operational notes

The sidecar is throwaway. If it gets corrupted, if it falls too far behind, or if you change the schema in a way that makes the existing tables wrong, delete the file and re-run `index sync`. A fresh sync from `state` mode against a non-trivial repo typically completes in seconds to a few minutes depending on document count; the bottleneck is JSON parsing and SQLite insert throughput, not the bare repo.

The sidecar and the bare repo do not need to live on the same host. The bare repo can be on a network mount (slow but valid) or accessed via a clone with auto-fetch. The sidecar should live on a local disk for SQLite to perform well.

Multiple watch loops against the same sidecar will fight for the SQLite write lock and waste work. Run one per sidecar file. Multiple readers against the same sidecar — including the watch loop itself — are fine because SQLite WAL mode allows concurrent readers with one writer.

## 10. Where to go next

If you want the sidecar populated by a watch loop inside a container, read [Run In Docker](Get-Started-Run-In-Docker). The page walks the canonical `ledgerdb index watch` container pattern, including how to expose the metrics endpoint to a host-side Prometheus.

If you want multiple nodes writing to the same database and sharing a sidecar per node, read [Run Distributed](Get-Started-Run-Distributed). Each node runs its own `index watch` against its local clone; the bare repos converge via git, and each sidecar derives from its local clone independently.

## See also

- [Get Started: Overview](Get-Started-Overview)
- [Run Locally](Get-Started-Run-Locally)
- [Run Distributed](Get-Started-Run-Distributed)
- [Run In Docker](Get-Started-Run-In-Docker)
