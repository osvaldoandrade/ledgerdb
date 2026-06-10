# Operations: Point-in-Time Recovery

> **Status:** Design proposal. No implementation in the v0.2.x series; see the open questions at the end and Epic [#7](https://github.com/osvaldoandrade/ledgerdb/issues/7) for the implementation tracker.

LedgerDB today has no native point-in-time recovery. The only supported restore path is "take a periodic backup (essentially `git clone --mirror` of the canonical repo); on disaster, copy the backup into place and restart." That gives you the state at the moment the backup was taken, nothing else. There is no way to say "roll the live repo to the state it was in immediately after transaction `tx_id=…` was applied" or "give me the state as of `2026-05-12T14:30:00Z`."

This page documents the planned approach. The mechanism it leans on — replay of deterministic TxV3 blobs onto a snapshot — is already possible by hand today; what is missing is the CLI and the snapshot-tagging cron job.

## Definition

We define PITR as:

> Given a target `tx_id` *or* a wall-clock timestamp, produce a repo state equivalent to the moment **just after that transaction was applied** to the canonical `refs/heads/main` history.

Two LedgerDB properties make this tractable:

- Every transaction is a TxV3 blob with deterministic `TxID` and `Timestamp` fields (see [TxV3 Format](IO-TxV3-Format) and `internal/domain/tx.go`). Replay is reproducible.
- Every commit on `refs/heads/main` corresponds to exactly one transaction (the commit message is `ledgerdb tx <txID>`, written by `writeUnsignedCommit` in `internal/infra/gitrepo/tx_store.go:410-434`). The Git DAG **is** the transaction log.

## Why this is needed

- **Recover from a bad write.** A buggy migration applies 50k incorrect patches. Without PITR, restoring means re-loading yesterday's backup and manually re-applying everything else that happened since. With PITR, you rewind to the transaction immediately before the bad batch.
- **Investigate a historical state.** Audit, forensic, and compliance queries often need "what did the ledger look like at time T". Today this requires spinning up a fresh repo from a backup that happened to be near T.
- **Test/staging snapshots.** Recreating production state at a known transaction boundary is the standard way to reproduce a customer issue.

## Approaches considered

Three approaches were considered. They are not mutually exclusive — the recommendation combines two of them.

### A. Replay from backup

Keep a periodic full backup. On PITR request, restore the backup into a recovery repo and walk forward through `refs/heads/main` (in the live repo or a fresher backup), replaying TxV3 blobs into the recovery repo until reaching the target `tx_id` or timestamp.

- Non-destructive to the live repo — you build a separate recovery output.
- Replay is reproducible because TxV3 is deterministic.
- Works for "I want a snapshot at T to investigate" use cases where you do not want to disturb production.

Costs: bounded by backup retention (if your oldest backup is 30 days old and you need a state from 60 days ago, this fails) and linear in transactions since the backup.

### B. Rewind in place

Use Git directly: find the commit whose message is `ledgerdb tx <target_tx_id>`, `git reset --hard <commit-before-target>` to rewind the live repo.

- Cheap: O(1) regardless of history depth.
- Does not need a separate backup as a starting point.

Costs: destructive (drops every transaction after the target), and breaks replication — mirrors that already fetched the later commits will reject the new history as non-fast-forward (see [Replication and HA](Ops-Replication-HA) §7 on `git push --force`). Recovering replication means re-mirroring every replica.

### C. Snapshot branches / tags

Run a cron job that tags `refs/heads/main` daily:

```
pitr/2026-05-12   -> <commit hash of main at midnight UTC>
pitr/2026-05-11
pitr/2026-05-10
```

PITR by day is then `git checkout -b recovery-2026-05-12 pitr/2026-05-12`. Trivial to implement (one cron + `git tag`), O(1) lookup, tags propagate over `git fetch` so every mirror has them automatically, non-destructive.

Costs: coarse granularity (a daily tag gives 24h of imprecision), and tags accumulate forever unless pruned.

## Recommendation: A + C combined

Neither approach alone is satisfactory. The combination is:

1. **Daily snapshot tags (Approach C)** as the coarse-grained checkpoint.
2. **Forward replay from the nearest snapshot (Approach A)** to reach the exact target `tx_id` or timestamp.

### Recovery flow

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

### Why this combination

- **Bounded replay cost.** Replay is bounded by the snapshot interval (24h of transactions max), not by the full history depth.
- **Non-destructive.** Production `refs/heads/main` is untouched. The recovery branch is a separate ref you can inspect, copy out, or promote.
- **Reproducible.** Deterministic TxV3 fields guarantee that replaying the same commits in order produces the same hashes — verified by the integrity service.
- **Works on every replica.** Tags fetch normally, so any mirror can serve the recovery without round-tripping to the primary.

### Configurable snapshot cadence

Daily is a default, not a constraint. Operators tune the tag frequency to their RPO target for granular recovery: high-volume / low-RPO deployments tag hourly (`pitr/2026-05-12T14`), audit / cold storage workloads tag weekly. The tradeoff is tag accumulation vs replay window.

## Wire-format implications

PITR depends critically on TxV3 being deterministic:

- `TxID` is canonical and stable per transaction.
- `Timestamp` is set at write time and carried in the blob.
- `ParentHash` chains each transaction to its predecessor on the same stream (`internal/domain/tx.go`).
- The integrity verifier already re-derives every hash from blob content alone and validates the chain (`IssueChain`, `IssueOrphanTx` in `internal/app/integrity/verify_service.go`).

Because of these properties:

- Replaying the same TxV3 blobs in commit order on top of the same snapshot **must** produce a bit-identical history. If it does not, that is a bug in the replay machinery, not an ambiguity in the wire format.
- No new wire-format fields are required for PITR. The existing TxV3 is sufficient.

This is an important property to preserve: any future TxV3 evolution that introduces non-determinism (random IDs, server-time-of-replay fields, etc.) **breaks PITR** and must be reviewed against this design.

## Open questions

These are open and should be resolved when the implementation epic is opened.

1. **Cost of replaying multi-million-tx histories.** Even bounded by a 24h snapshot interval, a high-throughput LedgerDB instance could have hundreds of thousands of transactions per day. Is sequential `PutTx`-style replay fast enough, or does the implementation need a bulk-import path that skips per-tx CAS retries?
2. **Integration with `ledgerdb truncate`** (#81, deferred). `truncate` removes old history beyond a retention horizon. If a PITR target falls *before* the truncation point, it must fail loudly (or fall back to cold backup). The two features must agree on a shared retention metadata field.
3. **CLI surface.** Proposed:
   ```
   ledgerdb pitr --to <tx_id>           # recover to exact tx
   ledgerdb pitr --to <RFC3339-time>    # recover to nearest tx <= time
   ledgerdb pitr --to <tx_id> --branch recovery-foo
   ledgerdb pitr --list-snapshots
   ```
   Open: should `--to` default to "recovery branch" or "in-place rewind"? Strong recommendation: branch by default; in-place must require an explicit `--destructive` flag.
4. **State-tree updates during replay.** The state path (the `statePath` block in `PutTx` at `internal/infra/gitrepo/tx_store.go`) is updated alongside each transaction. Replay must reproduce these state updates faithfully — confirm that re-running the same TxV3 against the recovery branch produces the same state blob hashes.
5. **Signed commits.** When `SignCommits` is enabled (the `writeSignedCommit` path at `tx_store.go:440-486`), replayed commits will have new signatures (different committer time / signer key). Is this acceptable for audit purposes, or does the recovery branch need to carry the *original* signed commits via `git cherry-pick`?
6. **Replication of recovery branches.** Should `pitr/*` tags and `recovery-*` branches propagate to mirrors automatically, or be namespaced so operators opt in?

## Out of scope

- **Implementation.** This page is a design statement. No code lands until the implementation epic resolves the open questions above.
- **Backup tooling.** PITR assumes backups exist; producing them is an ops concern handled by your standard Git mirror / object-storage pipeline. The sample workflow at `.github/workflows/backup.yml.example` shows the expected shape.
- **GC interactions.** If `git gc --prune` removes objects referenced by a `pitr/*` tag, the snapshot is no longer recoverable. Operational guidance for `gc` must be updated when PITR ships.

## See also

- [Replication and HA](Ops-Replication-HA) — the failover playbook that PITR complements.
- [Integrity and Verification](Concepts-Integrity-And-Verification) — the deep verifier that asserts replay produced a coherent chain.
- [TxV3 Format](IO-TxV3-Format) — the deterministic on-disk format PITR depends on.
- [History Modes](Concepts-History-Modes) — append vs amend; amend-mode collections do not retain the history PITR rewinds through.
- `internal/infra/gitrepo/tx_store.go` — the write path that replay must mimic.
- `internal/app/integrity/verify_service.go` — the verifier that validates the replayed branch.
