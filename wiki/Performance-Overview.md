# Performance Overview

LedgerDB's performance story is shaped by the medium it stores data in. Every write is a Git commit on `refs/heads/main`, every commit lives in the on-disk LSM-style object database that Git provides, every read either resolves a blob through the Git object store or falls through to a SQLite sidecar that an `index watch` loop has populated. The cost model is therefore the cost model of Git commits plus the cost model of SQLite — not the cost model of a purpose-built KV store, and not the cost model of a relational database. The numbers are different, the bottlenecks are different, and the tuning knobs are different.

This section frames how to reason about LedgerDB's performance, names the bench files that exercise the system, and is honest about what is and is not measured today.

## What this page covers

- The cost model for the four primary operations: write (`put`/`patch`/`delete`), read by key (`get`), query through the sidecar, and `index watch` catch-up.
- The role of CAS contention as the dominant scaling boundary for concurrent writers.
- What the benches under `bench/` exercise and what they do not.
- The set of knobs that move performance, with pointers to the page that documents each one.
- An explicit list of what this section does not claim.

## What this page does not cover

- Marketing numbers. The bench harness under `bench/` is a Go testing-package benchmark, and it does not currently print absolute throughput figures of the form "X ops/sec on hardware Y." The Go benchmark output is `ns/op` plus `B/op` plus `allocs/op`; converting to throughput is up to the reader. Where existing documentation (`docs/PERFORMANCE.md`) cites real numbers, this section quotes them with the citation; everything else is left to the reader's own run.
- Cluster-mode performance. LedgerDB is a single-process system with a Git remote for replication; there is no internal RAFT, no internal sharding, no replication-protocol cost to quantify. Cross-replica behavior is the cost of `git fetch` plus the cost of `index watch` applying the fetched commits.
- Latency distributions for production workloads. The benchmarks produce per-iteration timings; converting to p50/p99 for a real workload requires a separate harness, which is not present today.

## The cost model

Four operations dominate the wall-clock budget for a LedgerDB deployment.

**Write (`put`, `patch`, `delete`).** Each write pays for: (1) canonicalizing the new document payload (`internal/infra/canonicaljson`), (2) encoding a transaction record with `internal/infra/txv3` (protobuf), (3) writing the transaction blob to the Git object store, (4) updating the per-document stream tree, (5) writing a new commit object referencing the parent, (6) optionally signing the commit (cost depends on the signing backend; SSH adds 30-60ms, GPG 50-100ms, HSM 100-400ms per `docs/PERFORMANCE.md` §3), (7) compare-and-swap on `refs/heads/main` (`internal/infra/gitrepo/tx_store.go` with `casMaxRetries = 5` and `casBackoffBase = 25 * time.Millisecond` at lines 31-32), and (8) optionally auto-pushing to the remote when `--sync` is true.

A single uncontested write on a workstation-class NVMe disk with no signing typically completes in single-digit to low-double-digit milliseconds. The largest fixed cost in production is commit signing; the largest variable cost is auto-push (one network round-trip plus the remote's processing time). Under concurrent writers, CAS retries dominate: the retry schedule (25ms → 50ms → 100ms → 200ms → 400ms with full jitter, see `jitteredBackoff` at `internal/infra/gitrepo/tx_store.go:276`) bounds the worst-case wait at roughly 775ms before surfacing `ErrHeadChanged` to the caller.

**Read by key (`get`).** A point lookup against the Git object store. The cost is one tree walk to locate the document's stream path plus one blob read for the transaction body. Sharded layouts (`--layout sharded`, see `internal/domain/config.go:11-12`) add two levels of hex-prefix directory walk; flat layouts have one. There is no per-read commit work, no CAS, no signing — reads are cheap relative to writes. The benchmark `BenchmarkGetFlat` / `BenchmarkGetSharded` in `bench/reads_test.go:54-55` exercises this path.

**Query through the sidecar.** Filter and aggregate operations go through the SQLite sidecar, populated by `ledgerdb index watch`. The cost is whatever the SQLite query optimizer chooses; for indexed columns it is point lookups or range scans, for unindexed columns it is a sequential scan over the populated rows. The relevant tradeoff is that querying requires the sidecar to be caught up: a query against a freshly-cloned replica without a sidecar must first wait for `index watch` to apply the history, which is linear in the number of commits since the last snapshot.

**`index watch` catch-up.** The sync loop walks the commit history from the last recorded state forward, decodes each transaction, and applies it to the sidecar inside a SQLite transaction. The cost is approximately linear in `(commits since last state) × (txs per commit)`, with constants depending on the operation mix (PUT is cheap, PATCH requires loading the prior document state to apply the JSON Patch, see `internal/app/index/service.go:372-388`). The `--batch-commits` flag (`internal/cli/commands.go:582`) controls how many commits are coalesced into one SQLite transaction; larger values amortize SQLite fsync cost but increase the worst-case re-replay window on crash recovery.

## CAS contention

The single most operationally consequential property of LedgerDB's write path is that `refs/heads/main` is the serialization point for *all* writes in the repo. Every commit ends with a compare-and-swap on that ref; concurrent writers race; losers retry with exponential backoff.

For a single-writer workload, CAS retries do not happen. For two writers occasionally touching disjoint collections, they happen rarely. For two writers fighting over the same document, they happen on every concurrent attempt — and the retry budget is exhausted in under a second of wall time. The test at `internal/infra/gitrepo/tx_store_concurrency_test.go::TestCASRetryDistribution_Ramp` exercises the exact distribution: with one writer the retry count is zero; with sixteen or more writers some retries appear on either path; with a "hot document" (multiple writers patching the same `doc_id`) the parent-hash check rejects most attempts before the CAS loop runs, surfacing `domain.ErrHeadChanged` faster than the CAS retry path would.

The mitigation set is documented in `docs/PERFORMANCE.md` §6: shard the collection so writers spread across more tree paths, batch upstream into one writer process that serializes at the application layer, or raise the retry budget (a compile-time constant at `tx_store.go:31-32`). The right answer depends on whether contention is a real correctness signal (two writers should not both be authoring the same document) or an incidental scaling pain (many writers, naturally disjoint, occasionally racing on the ref).

## What the benches exercise

The `bench/` package contains Go testing-package benchmarks that drive the public SDK (`pkg/ledgerdbsdk`) end-to-end against a fresh repository in `b.TempDir()`. The bench harness is `bench/bench_test.go:setupRepo`, which initializes a repo with a configurable `StreamLayout` and `HistoryMode`, opens the SQLite index, and disables `AutoSync` because the bench repos have no remote configured.

Five bench files:

- `bench/writes_test.go` — `BenchmarkPutFlat`, `BenchmarkPutSharded`, `BenchmarkPatchFlat`, `BenchmarkPatchSharded`. Each runs in both `append` and `amend` history modes. Measures the full write path: encode, blob write, tree update, commit, CAS, no auto-push (the bench repos have no remote).
- `bench/reads_test.go` — `BenchmarkGetFlat`, `BenchmarkGetSharded`. Seeds `BENCH_READ_CORPUS` documents (default 1,000), then performs random-key lookups against the populated repo.
- `bench/snapshot_test.go` — `BenchmarkSnapshotCompaction`. Measures `maintenance snapshot` cost at chain depths of 5, 50, and 500. Uses `internal/app/maintenance` directly because the SDK does not yet expose a `Snapshot` method (`TODO(#27)`).
- `bench/sync_test.go` — `BenchmarkIndexSyncFlat`, `BenchmarkIndexSyncSharded`. Pre-populates 200 commits, then times one `SyncIndex` call to apply them to a fresh sidecar.
- `bench/bench_test.go` — the harness. No `Benchmark*` functions itself; provides `setupRepo` for the others.

Variance per iteration is dominated by SQLite fsync timing and Git object-store I/O. The benches do not pin CPU frequency, do not disable Turbo Boost, do not drop the page cache between iterations, and do not include the auto-push step that production deployments typically run. They are useful for measuring the *shape* of the cost (does adding indexes increase per-write cost? does sharded layout amortize better at 50k documents?), less useful for absolute throughput targets.

See [Performance Bench Harness](Performance-Bench-Harness) for the full operator's reference on running these.

## What moves performance

Every knob that has been observed to move LedgerDB's performance is documented on its own page. The summary:

- `--layout flat|sharded` — Git tree topology. Sharded amortizes per-commit tree-rewrite cost as collection cardinality grows. See [Performance Tuning Knobs](Performance-Tuning-Knobs).
- `--history-mode append|amend` — whether each commit chains or rewrites the head. `amend` reduces the storage growth rate and the integrity-verify cost at the price of destroying audit history. See [Performance Tuning Knobs](Performance-Tuning-Knobs).
- `--sync` — whether writes auto-fetch before and auto-push after. Disabling removes the network round-trip from the write hot path. See [Performance Tuning Knobs](Performance-Tuning-Knobs).
- `index watch --batch-commits N` — SQLite transactions per sync pass. See [Performance Tuning Knobs](Performance-Tuning-Knobs) and [Performance Read And Query](Performance-Read-And-Query).
- `index watch --fast` — relaxed SQLite durability for catch-up. See [Performance Read And Query](Performance-Read-And-Query).
- `index watch --mode state|history` — whether to apply via the materialized state tree or via the full history. See [Performance Read And Query](Performance-Read-And-Query).
- `--sign` / `--sign-key` — commit signing. Each signed commit pays the signing backend's per-commit cost. See [Performance Write Throughput](Performance-Write-Throughput) and `docs/PERFORMANCE.md` §3.
- Collection indexes declared via `ledgerdb schema` — each declared index adds per-PUT/PATCH cost to the watcher's apply path. See [Performance Read And Query](Performance-Read-And-Query).
- CAS retry budget — compile-time constants at `internal/infra/gitrepo/tx_store.go:31-32`. See [Performance Write Throughput](Performance-Write-Throughput).

## What this section deliberately does not claim

LedgerDB does not currently publish absolute throughput numbers in this wiki. The reason is honesty rather than reticence: the bench harness emits per-iteration timings, and converting to "X writes per second on Y hardware" requires a stable hardware reference and a tuning posture that the project does not currently maintain. `docs/PERFORMANCE.md` §3 quotes order-of-magnitude signing costs (SSH 30-60ms, GPG 50-100ms, HSM 100-400ms) as guidance; everything else is left to the reader's own run.

A few claims that are *not* made:

- There is no published p99 write latency. The bench files do not record latency distributions; they record `ns/op` aggregates.
- There is no published replica catch-up rate. `BenchmarkIndexSyncFlat`/`BenchmarkIndexSyncSharded` measure the cost of applying 200 commits to a fresh sidecar; converting to "commits per second of catch-up" depends on the size and shape of each commit.
- There is no published memory-under-load number. The benches report `B/op` and `allocs/op` per iteration, not steady-state heap residency.
- There is no published cold-start cost. Every bench runs in a fresh `b.TempDir()`, so the harness *includes* cold-start cost in each iteration; teasing out the steady-state cost requires reading `setupRepo` and separately accounting for the init phase.

The path forward, captured in Epic [#8](https://github.com/osvaldoandrade/ledgerdb/issues/8) ("Performance, Benchmarks & Tuning"), is a real bench harness with hardware pinning and JSON-output throughput numbers (`docs/PERFORMANCE.md` §7). Until that lands, the discipline is to publish methodology rather than figures.

## Mental model

The shortest mental model is three layers. The bottom layer is the Git object store — content-addressed blobs, packed into packfiles, accessed through `go-git` (`internal/infra/gitrepo/`). Reads and writes at this layer are bounded by disk I/O and the Git pack-delta machinery. The middle layer is the LedgerDB tx model — every domain operation is encoded as a `domain.Transaction`, written as a blob, and referenced from a commit. This layer adds canonicalization, encoding, and the CAS loop. The top layer is the SQLite sidecar — populated by `ledgerdb index watch`, queried by `ledgerdb query`, and the only source of efficient filter/aggregate access.

Performance work in LedgerDB is almost always work in one of those three layers in isolation. Writes are slow → look at the Git layer and the CAS retry rate. Queries are slow → look at the SQLite layer and the index declarations. Catch-up is slow → look at the cross-layer cost (commit walk plus tx decode plus SQLite apply). The bench files map cleanly to each: `writes_test.go` exercises layers one and two, `reads_test.go` exercises layer one, `sync_test.go` exercises all three together.

## See also

- [Performance Write Throughput](Performance-Write-Throughput)
- [Performance Read And Query](Performance-Read-And-Query)
- [Performance Tuning Knobs](Performance-Tuning-Knobs)
- [Performance Bench Harness](Performance-Bench-Harness)
- [Observability Metrics](Observability-Metrics)
- `docs/PERFORMANCE.md` — the operator-facing tuning guide. Quoted figures (signing costs, layout thresholds) originate there.
- `bench/` — the Go benchmark harness this section describes.
- `internal/infra/gitrepo/tx_store.go` — the CAS retry loop and the compile-time retry budget.
