# Integrity and Verification

A LedgerDB repository is tamper-evident because every transaction's hash is a function of its bytes, every transaction's `parent_hash` field points at the SHA-256 of the predecessor's bytes, and every transaction is stored in git's content-addressed object database. Changing a single byte of a historical transaction would change its hash, which would invalidate every descendant in the per-document chain and (separately) invalidate the git object's address. The system catches both classes of damage with the `ledgerdb integrity verify` command, backed by the verification service at `internal/app/integrity/verify_service.go`.

This page explains what `integrity verify` does, what `--deep` adds, how the verifier walks from genesis to head per stream, and how the optional commit signing makes commits non-repudiable. It does not cover the on-wire transaction format — that is [Transactions and TxV3](Concepts-Transactions-And-TxV3) — nor the chain semantics — that is [Versioning and Causality](Concepts-Versioning-And-Causality).

## What this page does not cover

Operational guidance on when to schedule verification, how to surface its output to a monitoring system, or how to integrate signing keys with corporate PKI. Those belong to the operator surface. This page is the conceptual model of what the verifier checks and why those checks are sufficient.

## The verification surface

The CLI command (`internal/cli/commands.go:712-742`) is:

```
ledgerdb integrity verify [--deep]
```

It enumerates every document stream in the repository and reports any issue it finds, with each issue labelled by one of nine codes (`internal/app/integrity/verify_service.go:13-22`):

- `head_read` — the `HEAD` file is unreadable
- `head_missing` — the stream directory exists but has no `HEAD`
- `tx_read` — a tx file cannot be read
- `tx_missing` — the tx directory is empty
- `tx_decode` — a tx file is not valid TxV3 protobuf
- `tx_invalid` — a tx decoded successfully but failed `domain.Transaction.Validate`
- `chain_invalid` — the parent-hash chain is broken (cycle, duplicate hash, etc.)
- `orphan_tx` — there are tx files that the chain does not reference
- `rehydrate_failed` — `--deep` mode could not replay the chain into a valid document

The CLI emits the issues either as human-readable lines or as JSON when `--json` is set, with one entry per issue. The exit code is non-zero whenever any issue is reported. Operators wire it into their backup and replication workflows as the canary signal — a single run that returns clean over the full repo means everything is hash-consistent, no commit has been edited in place, no blob is missing, no chain has been broken.

## The verification walk

For each stream listed by `StreamLister.ListDocStreams` (`internal/infra/gitrepo/stream_list.go:17-61`), the verifier runs `verifyStream` (`verify_service.go:76-126`). The walk is:

1. **Load the head**. `store.LoadStreamHead` returns the hash that the stream's `HEAD` file points at (after dereferencing the path and SHA-256-ing the resolved blob). If the load fails or returns empty, emit `head_read` / `head_missing` and stop.
2. **Load all tx blobs**. `store.LoadStreamTxs` returns every `*.txpb` file under `tx/`. If the directory is empty, emit `tx_missing` and stop.
3. **Build an index**. Decode each blob, validate it via `domain.Transaction.Validate`, rehash the bytes, key the entry by the hash. Bail on the first decode or validate failure with `tx_decode` or `tx_invalid`. Bail on duplicate hashes with `chain_invalid` (`fmt.Errorf("duplicate tx hash %s", hash)`) — a duplicate hash means two distinct blob copies are present, which should not be possible because git deduplicates content-addressed.
4. **Walk the chain from head**. `buildTxChain` (`verify_service.go:133-151`) follows `parent_hash` pointers backwards, tracking visited hashes. Cycle → `chain_invalid`. Missing predecessor → `chain_invalid` ("missing tx <hash>"). Otherwise produce an ordered list of entries from head to genesis.
5. **Check for orphan transactions**. If the chain has fewer entries than the index (some blob exists in `tx/` but is not reachable from head via parent-hash), emit `orphan_tx` with a count. Orphans are not corruption in the chain sense — the document's current state is intact — but they do indicate something wrote a transaction that never became part of the linear history, which is a signal worth surfacing.
6. **If `--deep`, replay the chain**. `verifyRehydrate` (`verify_service.go:153-202`) walks the chain in reverse (oldest first), applies each transaction to an in-memory document, and verifies that the replay succeeds. PUT seeds a snapshot. PATCH applies via the JSON Patch executor. DELETE clears the document. MERGE either replaces (snapshot variant) or applies a patch. Any error — patch path failure, delete without base, merge without base — yields `rehydrate_failed`.

The shallow check is cheap: one blob read per tx, one SHA-256 per tx, plus the chain walk. The deep check is expensive in proportion to chain length and document size, because every patch must actually run through the patcher. On a repository with millions of small documents and short chains, shallow scales fine; deep scales linearly with total history.

## Why the rehydration check matters

A chain can be hash-consistent without being applicable. Concretely: a PATCH transaction's `patch` payload might reference a path that does not exist in the document the chain has produced so far. The encoding would still validate, the SHA-256 would still chain correctly, the parent-hash check would pass. Only when you actually try to apply the patch do you discover the inconsistency.

This is exactly the gap `--deep` covers. The rehydration is a faithful replay; if it fails, the chain may be byte-consistent but it is semantically broken. The likely causes are operator error (manually editing a tx file, then recomputing parent_hash by hand without re-running the actual patch logic — except that recomputing parent_hash without re-encoding is impossible, so this is mostly hypothetical) or a bug in an older LedgerDB release that produced a syntactically valid but semantically broken sequence.

For production use, run `--deep` periodically (nightly is typical) and `--shallow` (the default) more frequently. The shallow check is the fast canary; the deep check is the thorough audit.

## The verifier's tradeoffs

The verifier walks every stream and every tx, sequentially. There is no parallelism today — `Verify` iterates `streams` in `verify_service.go:60-72`, doing one stream at a time. For a repository with millions of documents this is slow but bounded; the workload is read-only and respects `ctx.Done()` so a long-running verify can be cancelled cleanly.

The verifier reads from the bare repository's tree (the committed working state at the current `refs/heads/main`), not from a checked-out worktree. The tree is loaded via `loadMainTree` (`internal/infra/gitrepo/tx_read.go:113-138`), and each tx file is read out of the git object database. This means the verifier always sees a consistent snapshot — the one named by the current ref — even if a concurrent writer is producing new commits underneath. Each new commit is a new ref value; the verifier's tree is the one it loaded at the start of each stream's check.

The verifier does not currently re-verify the SHA-256 of the git blob against the git object hash. Git already does that on object retrieval — a corrupted blob fails to load with a checksum mismatch — so the redundant check would not add coverage. The verifier's contribution is the parent-hash chain check and the optional rehydration, both of which git does not know about.

## Commit signing

Commit signing is opt-in via the `--sign` flag (or `LEDGERDB_GIT_SIGN=true`) and an optional `--sign-key` (or `LEDGERDB_GIT_SIGN_KEY`). The CLI persistent flags at `internal/cli/root.go:30-31` and `67-68` set the store option `StoreOptions{SignCommits, SignKey}`. When signing is enabled, `Store.writeCommit` (`internal/infra/gitrepo/tx_store.go:399-408`) dispatches to `writeSignedCommit` instead of `writeUnsignedCommit`. The signed variant shells out to the system `git commit-tree -S` (`tx_store.go:440-486`) because go-git does not expose the GPG/SSH signing path internally.

The signed commit carries a detached signature in the commit object, validated by `git verify-commit` or any compatible tool. The verifier does not currently check signatures itself — `ledgerdb integrity verify` does not run `git verify-commit` — but the signature is preserved in the object database and propagates via `git push`/`fetch`. Verifying signatures is a separate operator workflow, typically a wrapper script that runs `git log --pretty=format:'%H %G?'` and filters for non-good results.

Signing gives non-repudiation: a signed commit is provably from the holder of the signing key. Combined with the hash chain, this means a downstream auditor can verify both that a sequence of transactions is hash-consistent and that each commit in the sequence was signed by a known operator. The combination is what makes LedgerDB suitable for regulated workloads — the chain prevents silent tampering, the signature prevents anonymous tampering.

The performance cost of signing is one fork-and-exec of `git commit-tree` per write (plus GPG/SSH agent round-trips). For interactive CLI use this is unmeasurable. For batch ingestion it adds milliseconds per commit. The choice to shell out rather than implement signing inline is a deliberate scope decision — duplicating GPG/SSH signing in Go is non-trivial and would have to track upstream protocol changes.

## What gets caught vs what does not

The verifier catches:

- Byte-level corruption of any tx blob (rehash check fails, chain walk fails)
- Manual deletion of a tx blob the chain references (`missing tx` error)
- A tx blob whose parent_hash points at a hash not present in the stream (`missing tx`)
- A chain that loops back on itself (`cycle detected`)
- A tx blob that decodes but violates the domain validation rules (e.g. wrong payload shape for the op)
- Under `--deep`, a chain that cannot be replayed coherently

It does not catch:

- A commit whose signature has been stripped (signing verification is operator-side)
- A push to a divergent ref that landed via `--force` (the verifier verifies the current state; it cannot detect that the history was rewritten)
- A consumer that misinterprets a transaction (the verifier checks structure, not meaning)
- Bit-rot on the SQLite sidecar (the sidecar is not part of integrity; it can be rebuilt)
- Replay attacks where a transaction is re-encoded by an attacker who controls the writer (the chain only proves who knew the previous hash; if the attacker is the writer, they know all previous hashes)

The last point is worth flagging. Hash chains prove that history has not been altered after the fact; they do not prove that history was never under attacker control to begin with. Commit signing is the answer to that — a signed chain proves both integrity and origin.

## Periodic verification as policy

A LedgerDB-shaped deployment typically runs verification on three schedules:

1. **Pre-backup**: before `ledgerdb backup` runs, verify the source repo so the backup is not a snapshot of an already-corrupted state.
2. **Post-restore**: after `ledgerdb restore` recovers from a bundle, verify the restored repo before bringing it back into the read path.
3. **Periodic full-deep**: scheduled nightly or weekly across the production cluster's primary repository. Catches gradual disk degradation, accidental tampering, and any class of bug that produces a hash-consistent but semantically broken chain.

The verify command's output is structured (JSON via `--json`) and small, so feeding it into a monitoring or alerting pipeline is a one-script job. The expected steady state is zero issues across all streams; any issue at all is a "wake an operator" event.

## See also

- [Transactions and TxV3](Concepts-Transactions-And-TxV3) for what the SHA-256 chain is computed over
- [Versioning and Causality](Concepts-Versioning-And-Causality) for the parent-hash semantics the verifier walks
- [Storage Layout](Concepts-Storage-Layout) for where the verifier finds streams to walk
- [Replication](Concepts-Replication) for why backups should be preceded by a verify
- [Architecture Overview](Concepts-Architecture-Overview) for the verifier's place in the call stack
