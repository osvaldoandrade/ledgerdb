# Operations: Replication and HA

This page describes how to deploy LedgerDB across regions, the consistency trade-offs of each topology, and the operational playbooks for failover and recovery. It is a companion to [Replication](Concepts-Replication), which covers the underlying Git wire-protocol mechanics; this page focuses on **what to deploy and how to run it**.

## Mental model

LedgerDB is a Git-native immutable document database. Every replica is a **full Git mirror** of the canonical repo. There is no separate replication log, no leader-election protocol, no quorum: replication is whatever `git push` and `git fetch` give you.

This has three direct consequences for HA design:

1. **Reads are always local.** Every replica holds the entire history. A regional reader never needs to cross the WAN for a read, and read latency is bounded by local disk plus the SQLite sidecar (`internal/app/index/`).
2. **Writes are CAS-serialized on `refs/heads/main`.** The write path in `internal/infra/gitrepo/tx_store.go` performs a compare-and-swap on the main ref (the retry budget is `casMaxRetries = 5` with `casBackoffBase = 25ms` at `tx_store.go:31-32`). CAS protects the *single* repo it runs against; it does **not** coordinate across hosts.
3. **Propagation is asynchronous and pull-based by default.** A replica is only as fresh as its last `git fetch`.

In short: LedgerDB gives you a Merkle-DAG of TxV3 blobs that any Git participant can copy, validate, and re-host. Choose your topology accordingly.

## Recommended pattern: primary-replica via Git mirror

This is the **default recommendation** for cross-region deployments that need durability, regional read latency, and a predictable write path.

### Topology

```
                    +---------------------+
                    |  Primary (writable) |
                    |  region: us-east-1  |
                    |  refs/heads/main    |  <-- CAS happens here
                    +----------+----------+
                               |
                 git push      |    git fetch (cron)
            (app writers)      |
                               v
            +------------------+------------------+
            |                  |                  |
   +--------+--------+ +-------+--------+ +-------+--------+
   | Mirror (RO)     | | Mirror (RO)    | | Mirror (RO)    |
   | region: eu-west | | region: ap-se  | | region: sa-east|
   +-----------------+ +----------------+ +----------------+
```

- **One writable origin.** All writers (`ledgerdb doc put`, application servers, SDKs) target this repo. CAS in `PutTx` linearises concurrent writes within that repo.
- **N read-only mirrors.** Each region clones with `git clone --mirror` and runs a periodic `git fetch` (cron, systemd timer, or Kubernetes CronJob).
- **Local reads.** Applications in each region read from their local mirror. The SQLite sidecar is rebuilt locally from the fetched objects.

### Setup

On each replica host:

```bash
git clone --mirror https://primary.internal/ledgerdb/orders.git \
    /var/lib/ledgerdb/orders.git

# /etc/cron.d/ledgerdb-fetch (every 30s; tune per RPO target)
* * * * * ledgerdb cd /var/lib/ledgerdb/orders.git && git fetch --quiet --prune
```

Point regional readers at the mirror via the same config knobs they would use for the primary (the repo path is the only thing that changes).

### Trade-offs

| Property | Value |
|----------|-------|
| **RPO** | Seconds to minutes, bounded by `git fetch` interval |
| **RTO** | Seconds (DNS / config flip) to minutes (promotion + writer reconfig) |
| **Write latency** | Single-region (writers may pay WAN to reach primary) |
| **Read latency** | Local in every region |
| **Failure mode if primary lost** | Read-only until a mirror is promoted |
| **Complexity** | Low — just Git |

This pattern is appropriate for the vast majority of LedgerDB deployments: audit logs, event stores, regulatory ledgers, and any workload where a few seconds of replication lag is acceptable.

## Active-active via per-region collections

When *every* region must accept writes locally (edge deployments, sovereign data residency), you cannot point all writers at a single primary. The LedgerDB-native pattern for this is **per-region collection prefixes**.

### Sharding by collection

Each region owns a disjoint slice of the collection namespace:

```
collections/
  us-east-1/orders/...      <- only writable in us-east-1
  eu-west-1/orders/...      <- only writable in eu-west-1
  ap-southeast-1/orders/... <- only writable in ap-southeast-1
```

Writers in different regions only touch different paths in the Git tree, so their commits never collide on the same `refs/heads/main` history. They collide on the *ref* itself, which is what Git merges resolve cheaply.

### Convergence

Pairwise `git fetch` plus merge between regions produces a single converged DAG. This works today for **disjoint writes**; it does **not** safely converge *overlapping* writes to the same doc until the planned CRDT modes ship under Epic [#88](https://github.com/osvaldoandrade/ledgerdb/issues/88).

**Until CRDT lands, the strong recommendation is single-writer primary (above) for any collection where two regions might touch the same doc.**

### When this is appropriate

- Geographically partitioned data with no cross-region writers per document.
- Edge or disconnected operation where regional autonomy outweighs global ordering.
- Workloads where the partition key is naturally regional (tenant ID maps to region, etc.).

## Conflict scenarios

### Concurrent writes within a single repo

Two writers in the same region (or two pods talking to the same primary) attempt to advance `refs/heads/main` simultaneously.

- `PutTx` calls `CheckAndSetReference`.
- The loser receives `storage.ErrReferenceHasChanged`.
- The store retries up to `casMaxRetries` (5) with exponential backoff (`casBackoffBase = 25ms`, doubling per attempt, jittered).
- If all retries fail, the caller gets `domain.ErrHeadChanged` — surface this to the application as a retryable conflict.

This is the **happy path** for write conflicts. CAS keeps the per-repo history linear; the application sees a clean retry signal.

### Concurrent writes across diverging branches (split brain)

Two replicas were promoted to writable simultaneously (e.g. botched failover). Each one accepted writes. When you try to reconcile, `git push` from one to the other will fail with a non-fast-forward error.

- CAS in `tx_store.go` **does not help here** — it only sees its local ref. Both sides happily advanced their own `refs/heads/main`.
- Recovery options, in order of preference:
  1. **Pick a winner** (usually the side with more committed business transactions or stricter audit requirements) and quarantine the other side's commits as a backup branch.
  2. **Manual reconciliation** via `ledgerdb doc revert` to re-apply the loser's transactions on top of the winner's history. Each revert produces a new TxV3 with a fresh `tx_id` and proper `ParentHash` chaining, so the integrity verifier remains happy.
  3. **Re-run `ledgerdb integrity verify --deep`** on the merged repo to confirm the hash chain is intact (`internal/app/integrity/verify_service.go`, issue codes `IssueChain` and `IssueOrphanTx`).

Split brain is *preventable* — see the failover playbook below. It is not something CAS can fix after the fact.

### Push rejected at the primary

A regional writer's push is rejected because the primary has advanced.

- Auto-fetch-then-retry (the default behaviour when `--sync` is on, see [Replication](Concepts-Replication) §2.1) handles this transparently for non-conflicting writes.
- If the rejected push touches the **same doc** that advanced upstream, CAS on the next attempt will see `ParentHash` no longer matches the current head and return `ErrHeadChanged`. The application must re-read the doc, re-apply its mutation against the new state, and retry.

## Failover playbook

Use this when the primary region is unreachable, degraded, or being taken offline for maintenance.

### Pre-flight (run continuously, not at incident time)

- Each mirror runs `git fetch` on a known interval (see Setup above).
- `ledgerdb_replication_lag_seconds` is scraped per replica (see [Observability Metrics](Observability-Metrics)).
- A documented promotion candidate exists per region (do not pick at 3 AM).
- Writers' configs are templated so the origin URL can be swapped via config reload rather than a code deploy.

### During the incident

1. **Detect.** Alert on:
   - Primary health-check failure.
   - `ledgerdb_replication_lag_seconds` exceeds threshold on **all** mirrors simultaneously (indicates primary is the source, not the network).
   - Application write error rate spike.
2. **Stop the bleeding.** Pause writers (drain the queue, return 503, or buffer locally — see [Replication](Concepts-Replication) on the local commit buffer). This is the single most important step: **do not let writers fan out to multiple replicas while the situation is unclear.**
3. **Pick the promotion target.** The mirror with the *lowest* `ledgerdb_replication_lag_seconds` immediately before the incident is the canonical choice. Verify by comparing `git rev-parse refs/heads/main` across mirrors — the candidate should be at or beyond every other mirror.
4. **Promote.** On the chosen mirror:
   ```bash
   # If it was a --mirror clone, convert it to a normal repo
   git config --unset remote.origin.mirror
   # Update other mirrors to fetch from the new primary
   git remote set-url origin https://new-primary.internal/ledgerdb/orders.git
   ```
   Update DNS / load-balancer / app config so writers target the new primary.
5. **Notify writers.** Reload configs. Resume the writer pool. Any locally buffered writes (from step 2) push to the new primary now.
6. **Verify.** Run `ledgerdb integrity verify --deep` against the new primary. This walks every stream, re-hashes every TxV3, and validates the parent chain. **A clean verify is the success criterion for the failover.**

### Post-incident

- Re-mirror the recovered ex-primary from the new primary (do **not** let it rejoin with its old refs — that is the path to split brain).
- Document the actual RPO/RTO observed and update alerts accordingly.
- If `integrity verify --deep` surfaced any `IssueChain` or `IssueOrphanTx`, open an incident ticket — that indicates real data loss or corruption, not just lag.

## Observability

Minimum metrics to scrape on every replica:

| Metric | Source | Why |
|--------|--------|-----|
| `ledgerdb_replication_lag_seconds` | `index watch` (see [Observability Metrics](Observability-Metrics)) | Time since last successful fetch; the primary signal for failover readiness. |
| `git fetch` success/failure counter | systemd / cron exit codes | Detect network or auth regressions before they become lag. |
| `refs/heads/main` commit hash (gauge / info metric) | `git rev-parse refs/heads/main` | Compare DAG tips across regions without parsing logs. |
| `ledgerdb integrity verify --deep` exit code (daily) | Cron + alerting | Catches silent corruption regardless of lag. |
| Local disk free on the repo volume | node exporter | Git is append-only; replicas grow forever until you GC. |

Alerting rules of thumb (the cookbook on [Alerts](Ops-Alerts) makes these concrete):

- **Warn** when any replica's lag exceeds `2x fetch_interval`.
- **Page** when *all* replicas' lag is growing simultaneously (primary problem, not network).
- **Page** on any non-zero exit from `integrity verify --deep`.

## Anti-patterns

These cause real, reproducible data loss or corruption. Avoid them.

- **`git push --force` against a shared repo.** This rewrites history that other replicas have already fetched and indexed. Their SQLite sidecars point at commits that no longer exist on the new history. The integrity verifier will flag this as `IssueChain` or `IssueOrphanTx` if you are lucky; if you are unlucky, you silently lose TxV3 blobs that were never re-fetched.
- **Sharing working trees across hosts** (NFS, EFS, shared block storage). Git's object database is not designed for concurrent writers on the same filesystem. The CAS in `tx_store.go` operates on the in-process `Storer`; it cannot serialise two processes mutating the same `.git/` directory through the filesystem.
- **Treating CAS as cross-replica serialisation.** CAS is per-repo. If your design assumes `PutTx` will block a concurrent writer in another region, you are designing for failure. Use a single writable primary, partition by collection prefix, or wait for CRDT (Epic [#88](https://github.com/osvaldoandrade/ledgerdb/issues/88)).
- **Promoting a mirror without verifying its DAG tip.** A mirror that is hours behind is *not* a viable primary. Always compare `git rev-parse refs/heads/main` and `ledgerdb_replication_lag_seconds` before swapping origin URLs.
- **Re-attaching an ex-primary without re-mirroring.** After failover, the old primary's refs are stale and may conflict. Always wipe and `git clone --mirror` from the new primary.
- **Skipping `integrity verify --deep` after recovery.** A repo that *looks* fine after a failover may have orphaned TxV3 blobs or broken parent chains. The deep verifier is the only authoritative check.

## Summary

- **Default:** primary-replica via Git mirror. Simple, predictable, gives you regional read latency and a clear failover path.
- **Edge / sovereign:** active-active via per-region collection prefixes, with single-writer-per-collection until CRDT lands (Epic [#88](https://github.com/osvaldoandrade/ledgerdb/issues/88)).
- **Conflicts within a repo:** handled by CAS in `tx_store.go`. Surface `ErrHeadChanged` to applications as retryable.
- **Split brain:** prevent it via the failover playbook; recover via manual `ledgerdb doc revert` plus `integrity verify --deep`.
- **Observe:** `ledgerdb_replication_lag_seconds`, ref-tip gauges, daily deep verify.
- **Don't:** force-push, share working trees, treat CAS as global serialisation.

## See also

- [Replication](Concepts-Replication) — the wire-protocol mechanics under this guide.
- [Conflict Resolution](Concepts-Conflict-Resolution) — what happens at the document level when two writes race.
- [Integrity and Verification](Concepts-Integrity-And-Verification) — the deep verifier referenced throughout the failover playbook.
- [Alerts](Ops-Alerts) — the Prometheus alerting cookbook keyed to the metrics above.
- [PITR](Ops-PITR) — recovery beyond "promote the freshest mirror," for the case where the canonical history itself is corrupted.
- `internal/infra/gitrepo/tx_store.go` — the CAS loop, the retry budget, and the auto-fetch / auto-push pair.
- `internal/app/integrity/verify_service.go` — the `--deep` verifier.
