# Performance: Tuning Knobs

This page is the catalog of every knob that has been observed to move LedgerDB's performance. For each: what it changes mechanically, what it costs, a reasonable starting value, and the file or CLI flag that sets it. The knobs split into four families: collection topology (layout, history mode), write-path behavior (sync, sign, CAS retries), `index watch` behavior (interval, batch, fast, mode), and the schema-level index declarations.

The reference for collection settings is `internal/domain/config.go` and `internal/domain/manifest.go`; the reference for CLI defaults is `internal/cli/root.go` and `internal/cli/commands.go`; the reference for CAS internals is `internal/infra/gitrepo/tx_store.go`. Line numbers cited below are stable enough to use as anchors but will drift as the code evolves.

## What this page covers

- Every operationally-visible performance knob in v0.2.x.
- The mechanical effect, the cost, and the recommended starting value for each.
- A short tuning order for fresh deployments.

## What this page does not cover

- Knobs that exist in code as compile-time constants but are not yet flag-surfaced (e.g., `casMaxRetries`). They are listed at the end with the caveat that changing them requires a rebuild.
- Application-layer knobs (e.g., how many concurrent writers the calling application runs). Those live one level above LedgerDB.
- Knobs that only move correctness, not performance (e.g., `--sign-key`).

## Collection topology

### `--layout flat | sharded`

The Git tree shape for a collection's documents. Set at collection creation time (`ledgerdb collection create --layout sharded`) and recorded in the manifest at `internal/domain/manifest.go:11`. The repo-wide default applied to new collections lives at `internal/domain/config.go:15` (`DefaultStreamLayout = StreamLayoutSharded`).

Mechanically:

- **`flat`** stores documents at `collections/<col>/<doc-id>.pb`. One tree level. Every commit that touches the collection rewrites the single tree blob, whose size scales linearly with document count.
- **`sharded`** stores documents at `collections/<col>/<aa>/<bb>/<doc-id>.pb`, where `aa/bb` are derived from the SHA-256 of the document ID. Three tree levels. Each rewritten tree is small (bounded by the fan-out per level), so per-commit tree cost stays roughly constant as the collection grows.

Cost: sharded layouts pay slightly more per write at low cardinality (more trees to rewrite) and amortize better at high cardinality (each tree stays small). Flat layouts win at small scale and degrade at large scale.

Recommended starting value: `sharded` for any collection expected to grow past 10,000 documents, `flat` for low-cardinality reference data. `docs/PERFORMANCE.md` §1 walks the threshold methodology in detail; the 10k figure is empirical guidance, not a hard cutoff.

Migration: switching layouts is a one-shot rewrite, documented in `docs/08_OPS.md`. Plan it as a maintenance window.

### `--history-mode append | amend`

Whether each write extends the commit chain or rewrites the head. Set at collection creation time (`ledgerdb collection create --history amend`) and recorded in the manifest at `internal/domain/manifest.go:12`. Defaults to `append` at `internal/domain/config.go:46`.

Mechanically:

- **`append`** creates a new commit with the prior head as its parent. The full lineage is preserved.
- **`amend`** rewrites the head commit so only the current state is reachable from `refs/heads/main`. Older commits become unreachable and are collected by `git gc`.

Cost: amend mode saves storage growth (bounded by the current document set), `git log` walking time (constant rather than linear), and integrity-verify time (walks the tree only — see `docs/PERFORMANCE.md` §2). The per-write hot-path cost is comparable to append. The audit trail is destroyed in amend mode.

Recommended starting value: `append`. Flip to `amend` only for cache-like or scratchpad collections where retention is explicitly undesirable. Compliance-bound workloads must stay in append.

A second effect: in amend mode, the `index watch` sync service is allowed to issue resets when it encounters `ErrCommitNotFound` (see `internal/app/index/service.go:127-138` and the CLI wiring at `internal/cli/commands.go:543`). In append mode, the watcher refuses to reset because doing so would destroy the audit trail; the operator has to intervene manually.

## Write-path behavior

### `--sync` (or `LEDGERDB_AUTO_SYNC`)

Whether each write auto-fetches before and auto-pushes after. Persistent root-command flag, default `true`. Declared at `internal/cli/root.go:69`; default sourced from `LEDGERDB_AUTO_SYNC` (`internal/cli/root.go:32`).

Mechanically: with `--sync=true`, every `put`, `patch`, or `delete` runs `git fetch` first to pull upstream commits and `git push` after to publish the new commit. With `--sync=false`, writes are local-only — the operator pushes explicitly with `ledgerdb push` when ready.

Cost: each round-trip is single-digit to tens of milliseconds against a local-network remote, hundreds of milliseconds across the public internet. Disabling collapses the write hot path to the local Git work measured by `bench/writes_test.go`.

Recommended starting value: `true` for interactive workstations and most production replicas. `false` for batch ingestion pipelines that write many documents per second and push periodically (every N writes or every N seconds). The pattern is application-aware.

### `--sign` / `--sign-key` (or `LEDGERDB_GIT_SIGN` / `LEDGERDB_GIT_SIGN_KEY`)

Whether to sign each commit, and with which key. Persistent root-command flags. Declared at `internal/cli/root.go:67-68`; defaults sourced from `LEDGERDB_GIT_SIGN` (false) and `LEDGERDB_GIT_SIGN_KEY` (`internal/cli/root.go:30-31`). Providing a `--sign-key` implicitly enables signing (`internal/cli/root.go:42-44`).

Mechanically: each commit goes through the configured Git signing backend (SSH, GPG, or HSM-backed) before the CAS attempt on `refs/heads/main`.

Cost (from `docs/PERFORMANCE.md` §3): SSH 30-60ms per commit, GPG 50-100ms, HSM 100-400ms. Signing serializes through the signing device, so concurrent writers contend on the signer too. A 100ms signing budget caps single-writer throughput at roughly 10 commits/sec before any I/O.

Recommended starting value: SSH signing (`git config gpg.format ssh`) for production. Disable signing only for ephemeral/test repositories. Mixing signed and unsigned commits on the same ref defeats the audit guarantee.

### CAS retry budget (compile-time)

`internal/infra/gitrepo/tx_store.go:31-32`:

```go
casMaxRetries  = 5
casBackoffBase = 25 * time.Millisecond
```

Mechanically: every write ends with a compare-and-swap on `refs/heads/main`. On conflict, the loop sleeps a jittered exponential backoff (`jitteredBackoff` at `tx_store.go:276-283`) and retries. After exhausting the budget, the loser surfaces `domain.ErrHeadChanged` to the caller.

Cost: the worst-case cumulative wait at the default settings is 25 + 50 + 100 + 200 + 400 = 775ms; the average is roughly half of that. Raising the retry count to 10 with the same base extends the worst-case wait to roughly 25 seconds (the next doublings are 800ms, 1.6s, 3.2s, 6.4s, 12.8s).

Recommended starting value: leave the defaults until `ledgerdb_cas_retries_observed_total` shows sustained contention (see [Observability Metrics](Observability-Metrics)). At that point, three mitigations beat raising the retry count: shard the hot collection, batch writes upstream into one writer, or route same-document writes to one writer. `docs/PERFORMANCE.md` §6 walks all three.

Changing the constants requires a rebuild. The recommendation is not to.

## `index watch` behavior

The flags on `ledgerdb index watch` are declared in `internal/cli/commands.go:575-588`.

### `--interval` (default `5s`)

Polling interval between sync passes. Mechanically: after each sync pass, the watcher sleeps `--interval` (plus optional `--jitter`) before the next pass. A shorter interval means lower replication lag and more CPU; a longer interval means higher lag and less CPU.

Cost: at `--interval 5s` the watcher does one pass per 5 seconds regardless of whether the upstream has new commits. Each pass costs at least one `git fetch` if `--fetch=true` (the default), which is bounded by the remote's latency and the size of the delta. A quiet repo on a fast network costs single-digit milliseconds per empty pass.

Recommended starting values (from `docs/PERFORMANCE.md` §4):

- Interactive / dev: 250ms - 1s.
- Production replica: 1 - 5s.
- High-fanout fleet: 5 - 30s with `--jitter` set to at least 25% of the interval.

### `--jitter` (default `0`)

Random offset added to each interval. Mechanically: each pass sleeps `interval + uniform(0, jitter)`. Spreads polling across the fleet so multiple watchers do not synchronize their fetches and create periodic spikes on the upstream.

Recommended starting value: at least 10% of `--interval` when running more than two watchers against a shared upstream. Zero is fine for a single watcher.

### `--batch-commits` (default `1`)

Commits per SQLite transaction during sync. Mechanically: the watcher coalesces up to `--batch-commits` consecutive commits into one SQLite transaction, then commits. Larger batches amortize the per-transaction SQLite fsync across more applied records; smaller batches make crash recovery cheaper (only one batch of work is at risk).

Recommended starting values:

- Steady-state replicas with tight replication-lag SLOs: `1` (the default). Every commit is durable in the sidecar before the next one is read.
- Busy replicas where occasional re-replay on crash is acceptable: `64` - `256`.
- Cold-start catch-up: `1024+`. Drop back once caught up.

### `--fast` (default `false`)

Relax SQLite durability. Mechanically: opens the sidecar with `sqliteindex.OpenWithOptions{Fast: true}`, which sets `PRAGMA synchronous=OFF` and similar tweaks (`internal/cli/commands.go:465`). A crash mid-catch-up loses applied transactions; the watcher will re-apply them on restart from the recorded high-water mark.

Recommended starting value: `false`. Flip to `true` only for catch-up scenarios; revert and restart once caught up.

### `--mode state | history` (default `state`)

Source-of-truth strategy. Mechanically:

- **`state`** consults the materialized state tree at the head commit and applies the diff between the recorded state hash and the head's state hash. O(documents changed).
- **`history`** walks the commit chain since the last recorded commit and applies each transaction. O(commits since last apply).

The watcher falls back from `state` to `history` automatically when state mode is unavailable (`internal/app/index/service.go:79-90`).

Recommended starting value: `state` for steady-state replicas. `history` for cold-start catch-up on a replica that is far behind, because the state-diff path effectively rebuilds the entire current state on every pass while the history walk processes only the actual transactions.

### `--metrics-addr`, `--metrics-allow-public`, `--audit-log`, `--audit-flush-interval`

Observability flags rather than performance flags. Documented on the [Observability Metrics](Observability-Metrics) and [Observability Audit Log](Observability-Audit-Log) pages.

## Schema-level indexes

Declared via `ledgerdb schema` (see `internal/domain/index.go`). Each declared index materializes in the SQLite sidecar via `EnsureIndexes` (`internal/app/index/ports.go:35`) the first time the watcher encounters the collection per pass.

Mechanically: every applied PUT/PATCH for an indexed collection updates each declared index. More indexes means more SQLite work per applied transaction in `index watch`, and faster query-time access for queries that use the indexed columns.

Cost: linear in the number of declared indexes per applied tx. A collection with five declared indexes pays five times the per-tx index-update cost of a collection with one index, in the watcher. Query-side savings vary by query shape.

Recommended starting value: declare an index when the queries against the column are frequent enough that per-query savings exceed per-write cost. Composite indexes (multiple columns) and unique-modifier indexes are supported as of commit `cdc4a85` ("feat(collection): support composite indexes with unique modifier (#120)").

## A short tuning order

For an operator tuning a fresh deployment:

1. Pick `--layout` per collection based on expected cardinality. `sharded` is the safe default for anything past 10k documents.
2. Pick `--history-mode` per collection based on retention requirements. `append` for compliance, `amend` for caches.
3. Pick `--sync` for the writer process based on ingestion shape. `true` for interactive, `false` for batched.
4. Configure signing (`--sign-key`) per the security posture. SSH for most production.
5. Leave CAS retry constants at defaults until metrics show sustained contention.
6. For each replica running `index watch`:
   - `--interval 1-5s` for production, `--jitter` 10-25% of interval if fleet > 2.
   - `--batch-commits 1` for tight SLOs, higher for throughput.
   - `--mode state`, fall back to `--mode history --fast --batch-commits 256` for cold starts.
7. Declare indexes per collection only for columns the application actually filters or aggregates on.
8. Schedule `maintenance gc` and `maintenance snapshot` periodically based on `ledgerdb_loose_objects_count` and `git log` chain depth (see `docs/PERFORMANCE.md` §5 and `docs/ALERTS.md`).

## See also

- [Performance Overview](Performance-Overview)
- [Performance Write Throughput](Performance-Write-Throughput)
- [Performance Read And Query](Performance-Read-And-Query)
- [Performance Bench Harness](Performance-Bench-Harness)
- [Observability Metrics](Observability-Metrics)
- [Observability Audit Log](Observability-Audit-Log)
- `internal/domain/config.go` — layout and history-mode constants.
- `internal/domain/manifest.go` — the per-repo manifest that persists collection-level settings.
- `internal/cli/root.go` — persistent flag and env-var wiring.
- `internal/cli/commands.go` — `index watch` flag wiring (lines 575-588).
- `internal/infra/gitrepo/tx_store.go` — CAS retry constants (lines 31-32).
- `docs/PERFORMANCE.md` — the operator-facing tuning guide; this page references it for the empirical thresholds and the signing-cost table.
