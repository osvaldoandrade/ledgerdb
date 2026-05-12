# Point-in-Time Recovery (PITR) — Design Proposal

**Status:** Proposal (no implementation in this revision)
**Audience:** Maintainers, SREs, operators planning recovery procedures
**Related:** [`REPLICATION_HA.md`](REPLICATION_HA.md), [`06_INTEGRITY.md`](06_INTEGRITY.md), [`08_OPS.md`](08_OPS.md)
**Feeds into:** Future epic (TBD); coordinates with `ledgerdb truncate` (#81, deferred)

---

## 1. Problem Statement

LedgerDB today has no native point-in-time recovery. The only supported
"restore" path is:

1. Take a periodic backup (essentially `git clone --mirror` of the canonical
   repo).
2. On disaster, copy the backup into place and restart.

This gives you **the state at the moment the backup was taken**, nothing else.
There is no way to say "roll the live repo to the state it was in immediately
after transaction `tx_id=…` was applied" or "give me the state as of
2026-05-12T14:30:00Z".

### 1.1 Definition

We define **PITR** as:

> Given a target `tx_id` *or* a wall-clock timestamp, produce a repo state
> equivalent to the moment **just after that transaction was applied** to the
> canonical `refs/heads/main` history.

Two things make this tractable in LedgerDB:

* Every transaction is a `TxV3` blob with deterministic `TxID` and
  `Timestamp` fields (`internal/domain/tx.go`). Replay is reproducible.
* Every commit on `refs/heads/main` corresponds to exactly one transaction
  (the commit message is `ledgerdb tx <txID>`, see
  `tx_store.go::writeUnsignedCommit`). The Git DAG **is** the transaction log.

### 1.2 Why this is needed

* **Recover from a bad write.** A buggy migration applies 50k incorrect
  patches. Without PITR, restoring means re-loading yesterday's backup and
  manually re-applying everything else that happened since. With PITR, you
  rewind to the transaction immediately before the bad batch.
* **Investigate a historical state.** Audit / forensic / compliance queries
  often need "what did the ledger look like at time T". Today this requires
  spinning up a fresh repo from a backup that happened to be near T.
* **Test/staging snapshots.** Recreating production state at a known
  transaction boundary is the standard way to reproduce a customer issue.

---

## 2. Approaches

Three approaches were considered. They are not mutually exclusive — the
recommendation in §3 combines two of them.

### 2.1 Approach A — Replay from backup

**Idea.** Keep a periodic full backup. On PITR request:

1. Restore the backup into a recovery repo.
2. Walk forward through `refs/heads/main` (in the live repo *or* a fresher
   backup), replaying TxV3 blobs into the recovery repo until reaching the
   target `tx_id` or timestamp.

**Pros.**

* Non-destructive to the live repo — you build a separate recovery output.
* Replay is reproducible because TxV3 is deterministic
  (`Timestamp`, `TxID`, `ParentHash` — see `internal/domain/tx.go`).
* Works for "I want a snapshot at T to investigate" use cases where you
  *don't* want to disturb production.

**Cons.**

* **Bounded by backup retention.** If your oldest backup is 30 days old
  and you need a state from 60 days ago, this approach fails.
* **Replay cost is linear in the number of transactions** since the backup.
  A multi-million-tx history makes this slow.
* Requires backup infrastructure (out of scope of LedgerDB itself).

### 2.2 Approach B — Rewind in place

**Idea.** Use Git directly:

1. Find the commit on `refs/heads/main` whose message is
   `ledgerdb tx <target_tx_id>`.
2. `git reset --hard <commit-before-target>` to rewind the live repo.

**Pros.**

* Cheap: O(1) regardless of history depth.
* Doesn't need a separate backup as a starting point.

**Cons.**

* **Destructive.** Drops every transaction after the target. If you wanted
  "a snapshot of state at T" rather than "permanently rewind production",
  this is the wrong tool.
* **Breaks replication.** Mirrors that already fetched the later commits
  will reject the new history as non-fast-forward (see `REPLICATION_HA.md`
  §7 on `git push --force`). Recovering replication means re-mirroring
  every replica.
* **Must be preceded by a full backup** — otherwise there is no escape
  hatch if the rewind was wrong.

### 2.3 Approach C — Snapshot branches / tags

**Idea.** Run a cron job that tags `refs/heads/main` daily:

```
pitr/2026-05-12   -> <commit hash of main at midnight UTC>
pitr/2026-05-11
pitr/2026-05-10
...
```

PITR by day is then:

```bash
git checkout -b recovery-2026-05-12 pitr/2026-05-12
```

**Pros.**

* Trivial to implement (one cron + `git tag`).
* O(1) lookup; no replay cost for day-granularity recovery.
* Tags propagate over `git fetch`, so every mirror has them automatically.
* Non-destructive — checking out a tag into a branch leaves
  `refs/heads/main` alone.

**Cons.**

* **Coarse granularity.** A daily tag gives you 24h of imprecision. If a
  bad write landed at 14:30 and the next snapshot is at midnight, you've
  lost 9.5 hours of legitimate transactions if you stop at the snapshot.
* Tags accumulate forever unless you prune them (operational chore).

---

## 3. Recommendation: A + C combined

Neither approach alone is satisfactory. The combination is:

1. **Daily snapshot tags (Approach C)** as the coarse-grained checkpoint.
2. **Forward replay from the nearest snapshot (Approach A)** to reach the
   exact target `tx_id` or timestamp.

### 3.1 Recovery flow

Given a target (either `tx_id=…` or `time=…`):

```
1. Find the most recent pitr/<date> tag at or before the target.
2. Create a recovery branch from that tag:
     git checkout -b recovery-<id> pitr/<date>
3. Walk forward through refs/heads/main commits, in order, applying their
   TxV3 blobs to the recovery branch via the normal PutTx path
   (internal/infra/gitrepo/tx_store.go).
4. Stop at the target tx_id (exact match) or at the last tx whose
   Timestamp <= target_time.
5. Run `ledgerdb integrity verify --deep` on the recovery branch to
   confirm the hash chain is intact
   (internal/app/integrity/verify_service.go).
```

### 3.2 Why this combination

* **Bounded replay cost.** Replay is bounded by the snapshot interval
  (24h of transactions max), not by the full history depth.
* **Non-destructive.** Production `refs/heads/main` is untouched. The
  recovery branch is a separate ref you can inspect, copy out, or promote.
* **Reproducible.** Deterministic TxV3 fields guarantee that replaying the
  same commits in order produces the same hashes — verified by the
  integrity service.
* **Works on every replica.** Tags fetch normally, so any mirror can serve
  the recovery without round-tripping to the primary.

### 3.3 Configurable snapshot cadence

The daily cadence is a default, not a constraint. Operators tune the tag
frequency to their RPO target for granular recovery:

* High-volume / low-RPO: hourly tags (`pitr/2026-05-12T14`).
* Audit / cold storage: weekly tags.

---

## 4. Wire-Format Implications

PITR depends critically on TxV3 being **deterministic**:

* `TxID` is canonical and stable per transaction.
* `Timestamp` is set at write time and carried in the blob.
* `ParentHash` chains each transaction to its predecessor on the same
  stream (`internal/domain/tx.go::Transaction`).
* The integrity verifier already re-derives every hash from blob content
  alone and validates the chain (`IssueChain`, `IssueOrphanTx` in
  `internal/app/integrity/verify_service.go`).

Because of these properties:

* Replaying the same TxV3 blobs in commit order on top of the same
  snapshot **must** produce a bit-identical history. If it doesn't, that's
  a bug in the replay machinery, not an ambiguity in the wire format.
* No new wire-format fields are required for PITR. The existing TxV3 is
  sufficient.

This is an important property to preserve: any future TxV3 evolution that
introduces non-determinism (random IDs, server-time-of-replay fields,
etc.) **breaks PITR** and must be reviewed against this document.

---

## 5. Open Questions

These are explicitly *open* and should be resolved when the implementation
epic is opened.

1. **Cost of replaying multi-million-tx histories.** Even bounded by a
   24h snapshot interval, a high-throughput LedgerDB instance could have
   hundreds of thousands of transactions per day. Is sequential
   `PutTx`-style replay fast enough, or do we need a bulk-import path
   that skips per-tx CAS retries?
2. **Integration with `ledgerdb truncate` (#81, deferred).** `truncate`
   removes old history beyond a retention horizon. If a PITR target falls
   *before* the truncation point, it must fail loudly (or fall back to
   cold backup). The two features must agree on a shared retention
   metadata field.
3. **CLI surface.** Proposed:
   ```
   ledgerdb pitr --to <tx_id>           # recover to exact tx
   ledgerdb pitr --to <RFC3339-time>    # recover to nearest tx <= time
   ledgerdb pitr --to <tx_id> --branch recovery-foo
   ledgerdb pitr --list-snapshots
   ```
   Open: should `--to` default to "recovery branch" or "in-place rewind"?
   Strong recommendation: branch by default; in-place must require an
   explicit `--destructive` flag.
4. **State-tree updates during replay.** The state path
   (`tx_store.go::PutTx` `statePath` block) is updated alongside each
   transaction. Replay must reproduce these state updates faithfully —
   confirm that re-running the same TxV3 against the recovery branch
   produces the same state blob hashes.
5. **Signed commits.** When `SignCommits` is enabled
   (`tx_store.go::writeSignedCommit`), replayed commits will have new
   signatures (different committer time / signer key). Is this acceptable
   for audit purposes, or does the recovery branch need to carry the
   *original* signed commits via `git cherry-pick`?
6. **Replication of recovery branches.** Should `pitr/*` tags and
   `recovery-*` branches propagate to mirrors automatically, or be
   namespaced so operators opt in?

---

## 6. Out of Scope

* **Implementation.** This document is a design proposal. No code lands in
  this revision. The implementation epic will be opened separately and
  must resolve the open questions in §5 first.
* **Backup tooling.** PITR assumes backups exist; producing them is an
  ops concern handled by your standard Git mirror / object-storage
  pipeline.
* **GC interactions.** If `git gc --prune` removes objects referenced by a
  `pitr/*` tag, the snapshot is no longer recoverable. Operational
  guidance for `gc` must be updated when PITR ships.

---

## 7. Summary

* **Today:** restore = full backup clone. No granular recovery.
* **Proposed:** daily `pitr/<date>` snapshot tags (cheap, propagate via
  Git) + forward replay of deterministic TxV3 blobs to the exact target.
* **Why it works:** TxV3 determinism + the integrity verifier give us
  reproducible replay with end-to-end validation.
* **Next step:** open the implementation epic, resolve §5, then ship
  `ledgerdb pitr --to <tx_id|time>`.
