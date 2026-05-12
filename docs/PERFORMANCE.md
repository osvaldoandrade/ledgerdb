# LedgerDB Performance Tuning Guide

This document describes the knobs that affect throughput, latency, and storage
footprint in a LedgerDB deployment, and how to reason about each tradeoff. The
guidance below targets LedgerDB v0.2.3 (TxV3 protobuf transactions, SQLite
sidecar, CAS-protected fast-forward on `refs/heads/main`).

The defaults shipped in the CLI are tuned for a single-writer workstation
scenario (one `ledgerdb index watch` process per repository). Production
deployments — especially multi-writer or replicated topologies — will usually
want to revisit at least the layout, history mode, and watch interval knobs.

---

## 1. When to choose sharded vs. flat layout

LedgerDB stores documents under a per-collection prefix in the Git tree. Two
layouts are supported today:

| Layout    | Filesystem shape                              | Best for                          |
|-----------|-----------------------------------------------|-----------------------------------|
| `flat`    | `collections/<col>/<doc-id>.pb`               | Low-cardinality collections       |
| `sharded` | `collections/<col>/<aa>/<bb>/<doc-id>.pb`     | High-cardinality, hot collections |

The sharded layout splits documents into two levels of hex prefix directories
(derived from the SHA-256 of the document ID). This bounds the number of
entries per directory and keeps tree-walking, `git status`, and pack-file
delta windows tractable as the collection grows.

### Rule of thumb

- Use **flat** when a collection holds **fewer than 10,000 documents** and is
  primarily read- or audit-oriented. Flat layouts are easier to inspect with
  plain `ls` and friendlier to ad-hoc `git log -- path/` queries.
- Use **sharded** for any collection expected to exceed **10,000 documents**,
  any collection with frequent concurrent writes, or any collection that will
  be mirrored to a filesystem with directory-entry limits (ext4 hashed
  directories degrade beyond ~50k entries; APFS slows down sooner).

### Why the threshold matters

Two pressures grow with collection cardinality:

1. **Tree blob size**: each directory in Git is a tree object whose serialized
   size scales linearly with the number of entries. A flat collection with
   50k documents materializes a ~3 MB tree blob on every commit touching the
   collection, even if only one document changed. Sharded layouts cap each
   tree at ~256 entries per level.
2. **Pack delta efficiency**: Git's delta compression is sensitive to entry
   ordering. Sharded layouts give the packer locality (documents that share a
   prefix tend to pack together), which reduces pack size and improves clone
   and fetch throughput on replicas.

### Migration

Switching layouts requires a one-shot rewrite. See `docs/08_OPS.md` for
the current migration procedure: it is a single commit, so readers see
the old layout until the commit lands and the new layout after.

### Benchmark methodology

To choose a threshold for your workload:

1. Provision two empty repositories with identical hardware.
2. Generate N synthetic documents (e.g. 1k, 10k, 50k, 100k) using the
   `bench/seed` harness from C1.
3. For each size, run the read/write/walk mix described in `bench/README.md`.
4. Compare:
   - p50/p95/p99 commit latency
   - pack size after `git gc --aggressive`
   - cold-cache `ledgerdb get` latency
   - `ledgerdb history` walk time on a random document

Plot the four curves against N for both layouts; the crossover point is your
layout threshold for that hardware class.

---

## 2. Append vs. amend history mode

LedgerDB supports two history modes, configurable per collection:

- **`append`** (default): every update creates a new commit that references
  its parent. The full lineage is preserved.
- **`amend`**: each update rewrites the head commit so only the latest state
  of each document is reachable from `refs/heads/main`. Older versions become
  unreachable and are collected by `git gc`.

### Tradeoffs

| Property                    | append                      | amend                          |
|-----------------------------|------------------------------|--------------------------------|
| Audit trail                 | Full, cryptographic          | Latest only                    |
| Storage growth              | Linear in #updates           | Roughly constant in #documents |
| `ledgerdb history` cost     | O(history length)            | O(1)                           |
| Integrity verify cost       | O(commits) — walks chain     | O(documents) — walks tree only |
| Replication catch-up cost   | Linear in commits since base | Single fast-forward            |
| Suitable for compliance     | Yes                          | No (history is destroyed)      |

### When to pick append

- Any regulated or compliance-bound workload (financial ledgers, audit logs,
  consent records). The append history is signed and tamper-evident; that
  property is what makes LedgerDB a "ledger" rather than just a document
  store.
- Workloads where downstream consumers tail the commit stream
  (`ledgerdb index watch`, change-data-capture pipelines).

### When to pick amend

- Cache-like or scratchpad collections that are recomputed from another
  source of truth.
- Short-lived per-request state where retention is explicitly undesirable.
- Collections that exist only to feed a downstream materialized view and
  where the view itself is the audit boundary.

### Integrity verify cost

`ledgerdb integrity verify` re-derives commit hashes and signature chains.
On an append-mode repository the cost is proportional to the number of
commits; on amend-mode collections only the current tree is verified. For a
mixed repository, scope the verify with `--collection` to keep the cost
predictable in CI.

---

## 3. Signing cost

Commit signing is the largest per-commit fixed cost in LedgerDB after
fsync. Measured on a 2024-class laptop:

| Mode             | Per-commit overhead | Notes                                 |
|------------------|---------------------|---------------------------------------|
| No signing       | ~0 ms               | CI scratch, ephemeral envs            |
| SSH signing      | 30 – 60 ms          | `gpg.format = ssh`, hardware-backed   |
| GPG signing      | 50 – 100 ms         | `gpg.format = openpgp`                |
| YubiKey / HSM    | 100 – 400 ms        | Network round-trip to the device      |

### Implications

- A 100 ms signing budget caps single-writer throughput at ~10 commits/sec
  even before any I/O. If you need higher throughput, batch updates with
  `--batch-commits` (see §4) so signing amortizes over many documents.
- Replicas only verify, they do not re-sign. Verification is cheap (sub-ms
  for SSH/GPG) and runs as part of the normal Git transport.
- HSM-backed keys serialize through the device; concurrent writers will
  contend. Use a dedicated signing identity per writer or fall back to SSH
  signing with a process-local key for high-throughput paths.

### Tuning

- Set `commit.gpgSign = true` and `gpg.format = ssh` for a balanced default.
- Use `--no-sign` only for ephemeral or test repositories. Mixing signed and
  unsigned commits on the same ref defeats the audit guarantee.
- Cache the GPG agent: a cold `gpg-agent` start adds ~200 ms to the first
  commit in a session.

---

## 4. Sync interval tuning for `ledgerdb index watch`

`ledgerdb index watch` is the single long-lived process in a typical
deployment (see `internal/cli/commands.go:1120-1176`). It polls the
underlying Git repository, applies new commits to the SQLite sidecar, and
exposes Prometheus metrics for replication lag and throughput.

The relevant flags:

| Flag                | Default | Purpose                                                       |
|---------------------|---------|---------------------------------------------------------------|
| `--interval`        | `5s`    | Time between scan passes                                      |
| `--jitter`          | `0`     | Random offset added to each interval to avoid thundering herd |
| `--batch-commits`   | `1`     | Commits per SQLite transaction (>= 1)                         |
| `--only-changes`    | `false` | Only emit output when new data is applied                     |

### Choosing `--interval`

- **Interactive / dev**: 250 ms – 1 s. Keeps the UI responsive.
- **Production replica**: 1 – 5 s. Trades a few seconds of replication lag
  for materially lower CPU and FS-watch noise on quiet repos.
- **High-fanout fleet** (many watchers, one upstream): 5 – 30 s with jitter
  set to ≥ 25 % of the interval. Prevents synchronized polling spikes on the
  upstream when a commit lands.

### `--jitter`

Always set jitter to at least 10 % of `--interval` when running more than
two watchers against a shared upstream. The jitter is sampled per-pass, not
per-process, so the effective desynchronization is geometric.

### `--batch-commits`

Each batch is one SQLite transaction. Larger batches amortize sidecar
fsync cost but increase the worst-case rebuild latency on crash recovery.

- Default `1` is the safest setting and the right choice for tight
  replication-lag SLOs: every commit is durable in the sidecar before
  the next one is read.
- Raise to `64` – `256` for steady-state throughput on busy replicas
  where occasional re-replay on crash recovery is acceptable.
- Raise further (`1024`+) only for cold-start catch-up on a replica
  that has been offline. Drop back to a small value once it is caught
  up.

### `--only-changes`

Suppress output (and downstream notifications) on passes where no new
data was applied. On quiet repositories this dramatically reduces log
volume and the work consumers do per poll. Enable unconditionally
unless you are debugging the watcher itself and need to see every
pass.

---

## 5. Snapshot threshold for `maintenance snapshot`

`ledgerdb maintenance snapshot` collapses a chain of patch commits into a
single snapshot commit. The current document state is preserved; the chain
of intermediate diffs is detached from `refs/heads/main` (and eventually
collected by `git gc`).

### Why snapshots matter

Rebuilding the sidecar from scratch — for example after a disaster, or
when bringing up a brand-new replica — walks every commit on the main
branch. The cost is roughly linear in chain length. A snapshot resets the
effective base of that walk to "now."

Empirically, rebuild cost is roughly halved each time the chain depth is
halved by snapshotting. A common policy:

- Snapshot when a collection's chain depth exceeds **10,000 commits**, or
- Snapshot weekly on busy collections regardless of depth, or
- Snapshot before any planned replica bootstrap.

### What snapshots cost

The snapshot operation itself is roughly the cost of a single full tree
read plus one commit. It blocks new writes for the duration of the commit
(typically < 1 s for collections under 100k documents). Plan for a brief
write pause; readers are unaffected.

### Interaction with append mode

Snapshots are compatible with append mode but reduce the audit guarantee:
once `git gc` collects the unreachable patch chain, the intermediate
versions are gone. If your compliance posture forbids this, keep the
chain reachable from a permanent tag (e.g. `audit/2026-Q1`) before
snapshotting.

---

## 6. CAS retry tuning

LedgerDB uses compare-and-swap on `refs/heads/main` to serialize writes
without a separate lock service. The current retry policy lives at
`internal/infra/gitrepo/tx_store.go:28-32`:

```go
const (
    casMaxRetries  = 5
    casBackoffBase = 25 * time.Millisecond
)
```

Each retry doubles the delay (25 ms, 50 ms, 100 ms, 200 ms, 400 ms), so
the policy bounds total wait at ~775 ms before surfacing the
CAS-contention error to the caller.

### When to raise the retry count

The current policy is sized for the common case of a single writer plus
occasional sidecar updates. Raise it when:

- You observe `ledgerdb_cas_retries_total` climbing into the tens per second
  (see `docs/ALERTS.md` — `LedgerDBCASContention`).
- You expect predictable bursts of concurrent writes to the same
  collection (e.g. ingestion fan-in from a queue).
- End-to-end latency is dominated by failed retries rather than network
  or fsync.

A retry count of `10` with base delay `25 ms` caps total wait at ~25 s and
generally absorbs realistic bursts.

### When to lower the retry count

Lower it when contention indicates a real correctness problem (e.g. two
writers fighting over the same document ID) and you want fast failure so
the caller can dedupe at the application layer. A retry count of `2`
turns CAS contention into a quickly-surfaced error.

### Mitigations besides retry tuning

- Shard hot collections (see §1) so writes spread across more tree paths
  and conflict less at the parent-tree level.
- Batch writes through `ledgerdb index watch --batch-commits` (see §4) so
  many document updates land in one commit and one CAS round-trip.
- Route writes for the same document ID to a single writer instance to
  serialize at the application layer rather than the storage layer.

---

## 7. Benchmark methodology

All quantitative claims in this guide should be validated against your
own hardware and workload. The reference harness lives under `bench/`
(introduced in C1) and supports four scenarios:

1. **`bench seed`** — populate a fresh repository with N synthetic
   documents in a configurable layout.
2. **`bench write`** — drive a steady write rate against the repository,
   reporting p50/p95/p99 commit latency and CAS retry counts.
3. **`bench replay`** — point a fresh `ledgerdb index watch` at the
   repository and measure cold catch-up time.
4. **`bench mixed`** — interleave reads and writes at a configurable
   ratio.

### Running the benchmarks

```sh
# Populate
go run ./bench seed --count 50000 --layout sharded --out /tmp/ldb-bench

# Steady-state write
go run ./bench write --target 100/s --duration 60s --repo /tmp/ldb-bench

# Replica catch-up
go run ./bench replay --repo /tmp/ldb-bench --report cold-replay.json
```

Recommended environment for repeatable numbers:

- Pin the benchmark to a fixed CPU set (`taskset`/`cpuset`).
- Disable Turbo Boost and frequency scaling
  (`cpupower frequency-set -g performance`).
- Run on a dedicated SSD partition; record `fstrim` state and filesystem
  options.
- Drop page cache between runs: `echo 3 | sudo tee /proc/sys/vm/drop_caches`.

### Reporting

Each `bench` subcommand emits JSON suitable for ingestion into the
Grafana dashboard described in `dashboards/grafana/ledgerdb-watch.json`.
The same metric names are exposed by `ledgerdb index watch` so the
dashboard works both for production observation and for offline
benchmarking.

---

## Appendix: Quick-reference defaults

| Setting                          | Default       | Where to change                                          |
|----------------------------------|---------------|----------------------------------------------------------|
| Collection layout                | `flat`        | `ledgerdb collection create --layout sharded`            |
| History mode                     | `append`      | `ledgerdb collection create --history amend`             |
| Commit signing                   | inherited     | `git config commit.gpgSign true`                         |
| `index watch --interval`         | `5s`          | CLI flag                                                 |
| `index watch --jitter`           | `0`           | CLI flag                                                 |
| `index watch --batch-commits`    | `1`           | CLI flag                                                 |
| `index watch --only-changes`     | `false`       | CLI flag                                                 |
| CAS max retries                  | `5`           | `internal/infra/gitrepo/tx_store.go:28-32`               |
| CAS base delay                   | `25ms`        | `internal/infra/gitrepo/tx_store.go:28-32`               |
| Snapshot trigger depth           | manual        | `ledgerdb maintenance snapshot --threshold <N>`          |
