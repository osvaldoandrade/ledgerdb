# History Modes

A LedgerDB repository operates in one of two history modes, declared at init time and stored in the manifest. The mode is a per-repository constant — it is not switchable per collection or per write — and it changes how the put, patch, and delete services interact with the history tree under `documents/`. The two modes are `append` and `amend`, defined as constants in `internal/domain/config.go:41-46`:

```go
const (
    HistoryModeAppend HistoryMode = "append"
    HistoryModeAmend  HistoryMode = "amend"
)
const DefaultHistoryMode = HistoryModeAppend
```

`append` is the default and is what makes LedgerDB look like a ledger. Every transaction adds a new file under `tx/`, the parent-hash chain grows by one link, and the full history is recoverable forever. `amend` is the lean alternative: every transaction collapses the prior history for that document, so the chain stays at length one and the working tree holds only `tx/current.txpb` per document.

## What this page covers

This page explains the two modes, the user-visible differences in behaviour, and the implementation points that enforce them. It does not cover the on-wire transaction format ([Transactions and TxV3](Concepts-Transactions-And-TxV3)) or the state-tree mechanism that `doc get` uses regardless of mode ([Storage Layout](Concepts-Storage-Layout)).

## How `append` works

In `append` mode every write is a new tx file with a deterministic name derived from the timestamp and op: `txFileName` (`internal/infra/gitrepo/tx_store.go:493-506`) returns `<unix_ns>_<op>.txpb`. The store writes the file under `documents/.../tx/` and updates the `HEAD` pointer to its relative path. The new tx carries `parent_hash` set to the SHA-256 of the previous head tx's encoded bytes, loaded by `LoadStreamHead` in the put/patch/delete services. The chain therefore grows by one link per write.

The CAS check at `internal/infra/gitrepo/tx_store.go:153-157` is the load-bearing line:

```go
if s.historyMode() != domain.HistoryModeAmend {
    if currentHead != write.Tx.ParentHash {
        return doc.PutResult{}, domain.ErrHeadChanged
    }
}
```

In `append` mode the write is rejected if the document's head has moved between the service's `LoadStreamHead` and the store's `PutTx`. The outer CAS loop on the git ref then retries (`tx_store.go:139-200`); the inner head check fires only when another writer has prepended to this specific document since the read. Either retry path eventually reads the new head and either rebases on top (the common case in [Conflict Resolution](Concepts-Conflict-Resolution)) or bubbles `ErrHeadChanged` up to the caller for explicit handling.

The tx file naming is monotonic by timestamp, so a `ls` of the `tx/` directory is approximately ordered by write time. The actual order in the chain is not given by filename — it is given by `parent_hash`. The integrity verifier (`internal/app/integrity/verify_service.go:93-126`) walks via `parent_hash`, not by filename, so a clock skew that produced an out-of-order filename does not affect chain correctness.

The cost of `append` is storage. Every patch is preserved forever. A document that sees a thousand small patches accumulates a thousand tx files plus the state-tree snapshot. Git's packfile deduplication helps — a small patch is genuinely small — but the working tree still has a thousand files in one directory. The [Storage Layout](Concepts-Storage-Layout) page covers the directory sharding that limits per-directory bloat across documents, but the per-document tx directory still grows linearly with patch count. The `ledgerdb maintenance snapshot` command (`internal/app/maintenance/snapshot_service.go`) is the operator tool for capping that growth: it converts a long patch chain into a single snapshot put without losing tracking of the prior history, on demand.

## How `amend` works

In `amend` mode every transaction overwrites the prior tx file. The store sets the tx file name to a fixed constant — `domain.TxCompactFile` is `"current.txpb"` (`internal/domain/storage.go:8`) — and the put/patch/delete services do not bother loading the prior head hash (`PutService.Put` at `internal/app/doc/service.go:64-71`: the `parentHash` branch is skipped when mode is amend; `DeleteService.Delete` and `PatchService.Patch` do the same).

The `parent_hash` field in amend-mode transactions is always empty. The chain length is always one. There is no history to walk, no prior state to recover. `git log` on the repository still shows the sequence of commits — one per write — but the working tree at any point in time holds only the latest version of every document.

Inside `Store.PutTx`, the amend path takes an additional shortcut at line 401-403:

```go
parentRef := baseRef
if s.historyMode() == domain.HistoryModeAmend {
    parentRef = nil
}
```

The new commit is written with **no parent**, even though the previous commit still exists on disk. This means `git log` shows independent commits (no parent chain across writes), and `git gc` is free to prune the prior commits once they are unreferenced. The behaviour is intentional — amend mode is for users who want a working document store with a single most-recent revision per record, not a history-bearing audit trail. The git layer still gives you bit-for-bit reproducibility of any committed state, but it does not retain prior states.

The patch path in amend mode also rewrites the patch as a snapshot. `PatchService.Patch` at `internal/app/doc/patch_service.go:108-119` distinguishes the two modes:

```go
if s.historyMode == domain.HistoryModeAmend {
    snapshot, err := s.canonicalizer.Canonicalize(ctx, updatedDoc)
    if err != nil { return PutResult{}, err }
    tx.Op = domain.TxOpMerge
    tx.Snapshot = snapshot
} else {
    tx.Op = domain.TxOpPatch
    tx.Patch = canonicalPatch
    tx.ParentHash = headHash
}
```

In amend mode the patch is computed (the new document body is materialised) but only the resulting snapshot is persisted as a `MERGE` op. The patch instruction itself is discarded. This is consistent with the rest of amend mode — there is no surviving history that a downstream consumer could replay the patch against, so storing the patch would be storing garbage.

## Choosing a mode

The trade is audit fidelity versus storage cost and operational simplicity.

`append` is the right choice when the repository's value is the audit trail. Use cases: regulated event sourcing, configuration change logs, agreement records that must remain provably unmodified, scientific datasets with full provenance. In these the size growth is the point — every prior version stays addressable by hash.

`amend` is the right choice when the repository's value is the current state with no need to recover earlier revisions. Use cases: small document databases that just happen to want git-native deployment, intermediate caches, ephemeral or rebuild-anytime data. The integrity verifier still runs and the `db.yaml` manifest still records the mode; what you lose is `doc log` (returns one entry), `doc revert` (no prior tx to revert to), and `ledgerdb integrity verify --deep` (deep-mode rehydration is trivially correct because every chain has one element).

Switching modes after init is not supported by the CLI as a single command. The closest path is `ledgerdb backup` to capture the current state, `ledgerdb truncate` (`internal/app/dr/truncate.go`) to drop history, and a re-init with the new mode. There is no in-place migration because changing the mode changes the meaning of every tx file under `tx/`; the safer story is "fresh repo, replay your most recent state".

## The mode is repository-wide

Worth flagging explicitly because the surface looks per-call: every put/patch/delete service constructor takes a `historyMode` argument (visible in the CLI wireup at `internal/cli/commands.go:178-187` for put, similar for patch and delete). The CLI plumbs it through from `RootOptions.HistoryMode`, which the persistent pre-run sets from the manifest (`internal/cli/root.go:53-58`). The SDK does the same: `pkg/ledgerdbsdk/client.go:138-182` resolves the mode from the manifest and rejects an explicit `Config.HistoryMode` that disagrees. There is no per-call override; if you set the wrong mode at init, the way out is a full re-init.

## Mode interactions with replication

A clone inherits the source repository's manifest, so the mode propagates automatically. Two repositories on different modes cannot share a remote sanely — push and fetch would each interpret the other's commits as their own kind of history, and the head check would either fire spuriously (a clone in append mode looking at amend-mode commits with no parent chain) or be silently bypassed (an amend-mode push to an append-mode origin). The expected discipline is "one repository, one mode, one set of consumers". See [Replication](Concepts-Replication) for the protocol layer.

## See also

- [Transactions and TxV3](Concepts-Transactions-And-TxV3) for the on-wire format
- [Storage Layout](Concepts-Storage-Layout) for the on-disk effect of each mode
- [Conflict Resolution](Concepts-Conflict-Resolution) for how the head check interacts with CAS retries
- [Documents and Collections](Concepts-Documents-And-Collections) for the put/patch/delete service surface
