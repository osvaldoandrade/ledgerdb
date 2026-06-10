# Concepts Overview

LedgerDB is a document store that uses a bare git repository as its only persistent backend. Every write produces a TxV3 protobuf blob, every blob is stored as a git object, and every committed transaction becomes a commit on `refs/heads/main`. There is no daemon, no embedded server, no replication protocol of its own. The CLI is the database; the repository on disk is the data. Everything else in this section is a refinement of that sentence.

It helps to start by saying what LedgerDB is not. A document database like MongoDB or CouchDB keeps the latest version of each document in a B-tree or LSM, and reads return that version directly. History, when it exists at all, is an operational concern bolted on with change streams or oplogs. An event store like EventStoreDB or Kafka treats the log as the source of truth, but throws away document identity — events are append-only and consumers materialise their own state per topic. LedgerDB sits between the two. The transaction log is the durable record. The latest document state is a projection of that log onto a parallel `state/` tree, refreshed every write. Both representations live in the same git commit, so a clone of the repository carries both.

This shape changes what the system has to do. There is no in-process write pipeline, no commit coordinator, no leader election, no replication factor. Every state change is a `git commit-tree` plus a CAS update of `refs/heads/main`. The CAS is the only concurrency primitive; if two writers race, the loser sees `ErrHeadChanged` (declared at `internal/domain/errors.go:5`) and retries with exponential backoff (`internal/infra/gitrepo/tx_store.go:139-200`). Replication is `git push` and `git fetch`. Backup is `git bundle create`. Integrity verification is rehashing every blob and walking the parent-hash chain. The operational vocabulary is git's; the only LedgerDB-specific surface is the commands the CLI builds on top.

## The three layers

A LedgerDB repository has three logical layers, all materialised inside one bare git repo:

1. **The immutable tx stream**, under `documents/<collection>/<shard>/<shard>/DOC_<hash>/tx/`. Each file is one TxV3 protobuf blob (`internal/infra/txv3/tx.proto`). A per-document `HEAD` file points at the newest tx file. The chain from `HEAD` backwards via `parent_hash` is the document's full history. See [Transactions and TxV3](Concepts-Transactions-And-TxV3) for the encoding and [Versioning and Causality](Concepts-Versioning-And-Causality) for the chain semantics.
2. **The materialized state tree**, under `state/<collection>/<shard>/<shard>/DOC_<hash>/tx/current.txpb`. This holds a single snapshot TxV3 blob for the current state of each document, written in the same commit as the history-tree update (`internal/infra/gitrepo/tx_store.go:122-178`). A `doc get` that hits the state tree returns in one tree walk; a get that has to rebuild from the history walks the parent chain and replays patches. The state layout is covered in [Storage Layout](Concepts-Storage-Layout).
3. **The SQLite sidecar index**, a separate file outside the bare repo (`internal/infra/sqliteindex/store.go`). It is rebuilt from the git log by a long-running `ledgerdb index watch` process or refreshed on demand by `ledgerdb index sync`. It holds one table per collection (`collection_<name>`) with `json_extract` indexes on declared fields, so ad-hoc queries do not have to deserialise every document. See [Indexing](Concepts-Indexing).

The first two layers travel together inside the same git commit. The third is per-replica and never replicates — every clone rebuilds its own SQLite from the tx stream.

## What this section covers

The pages that follow build the concept stack from the inside out. They are not a tutorial; they explain why the system is shaped the way it is. If you want to run LedgerDB first and read the rationale later, start with the [Get Started](Get-Started-Overview) page and come back here.

[Documents and Collections](Concepts-Documents-And-Collections) covers the data model: how a collection declares its JSON schema and index specs, what a document ID is, what gets written when you call `doc put`. [Transactions and TxV3](Concepts-Transactions-And-TxV3) walks the protobuf format and the deterministic encoding that makes blob hashing stable. [History Modes](Concepts-History-Modes) explains the choice between `append` (full audit, every tx kept) and `amend` (compacted, only the current state retained).

[Storage Layout](Concepts-Storage-Layout) is the long page on disk structure — the SHA-256-based directory sharding from `internal/domain/hds.go`, the relationship between `documents/`, `state/`, `collections/`, and `db.yaml`, and how git's content-addressed object store maps to logical entities. [Versioning and Causality](Concepts-Versioning-And-Causality) covers the parent-hash DAG per document; [Conflict Resolution](Concepts-Conflict-Resolution) covers the CAS retry loop and the JSON Patch semantics on top.

[Indexing](Concepts-Indexing) describes the SQLite sidecar and the watch loop. [Integrity and Verification](Concepts-Integrity-And-Verification) covers `ledgerdb integrity verify` and how the rehash-and-walk path detects corruption. [Replication](Concepts-Replication) covers `git push`/`fetch`, the bundle path, and the eventual-consistency story.

The final page, [Architecture Overview](Concepts-Architecture-Overview), ties everything together: the CLI command surface, the application-services layer under `internal/app/`, the infrastructure adapters under `internal/infra/`, and the SDK under `pkg/ledgerdbsdk`.

## Why git as the storage engine

The choice of git as a backend is not a marketing label; it changes what the database has to do and what it gets for free. Three properties follow from it.

The first is **content-addressed durability**. A git object's name is its SHA-1 hash; a blob with the same bytes always lands at the same address, no matter who wrote it. Concurrent writers cannot corrupt the object store — they may write duplicate blobs, but never inconsistent ones. The integrity verifier (`internal/app/integrity/verify_service.go`) leans on this directly: rehashing a blob and comparing to its expected hash detects any byte-level damage.

The second is **replication via the git wire protocol**. Push and fetch are someone else's problem; the LedgerDB CLI shells out to `go-git` (and occasionally to the system `git` binary for `bundle` and signed commits). Hosting is whatever speaks git — GitHub, GitLab, Gitea, a bare repo over SSH, a USB stick. The auth surface is the one git already knows: HTTPS tokens (`LEDGERDB_GIT_TOKEN`, `GITHUB_TOKEN`, `GH_TOKEN`) and SSH keys, resolved in `internal/infra/gitrepo/auth.go`.

The third is **time travel for free**. Every commit is a complete snapshot. `git log` over the bare repo gives you the full mutation history; checking out an older commit shows the world as it was at that moment. The doc-log API (`internal/app/doc/log_service.go`) is a thin projection of this — it walks the tx chain for a single document and emits each step.

The cost is that git's object model was designed for source code, not for high-throughput document writes. Every transaction is one commit. Tree updates copy every directory entry along the path from root to the changed file (`internal/infra/gitrepo/tx_store.go:343-389`). There is no in-process batching of writes across documents; one `doc put` is one commit. LedgerDB does not pretend this scales to OLTP workloads. The benchmarks (`internal/infra/gitrepo/tx_store_bench_test.go`) measure dozens to a few hundred writes per second per process on local disk, depending on the size of the working tree. The system targets workloads where auditability, replayability, and bit-for-bit reproducibility matter more than write throughput.

## A note on "sharding"

The word "sharding" appears in LedgerDB's docs but it does not mean what it means in MongoDB or Cassandra. There is no keyspace partitioning across nodes, no router, no consistent hash ring. "Sharded" is the name of the **directory layout** under `documents/` and `state/` — given the SHA-256 of `<collection>/<doc_id>`, the first two hex chars and the next two hex chars become two directory levels (`internal/domain/hds.go:24-29`). The point is to avoid putting a million sibling entries in one git tree, which would make tree-update cost linear in the collection size. There is no horizontal scale-out story; one repository is one address space, served by one process at a time. See [Storage Layout](Concepts-Storage-Layout) for the directory layout and [Architecture Overview](Concepts-Architecture-Overview) for why the single-writer model is a deliberate constraint.

## What this section deliberately omits

Nothing in this section is about how to install LedgerDB, what flags `doc put` accepts, what the SQLite schema looks like in DBeaver, or how to wire `index watch` to systemd. The CLI reference, the operator how-tos, the SDK reference, and the troubleshooting page each have their own home. This section is the conceptual ground those other sections stand on. Read it once if you intend to use LedgerDB for anything beyond a toy project; refer back to it when a specific behaviour surprises you.

## See also

- [Architecture Overview](Concepts-Architecture-Overview) for the full call stack
- [Storage Layout](Concepts-Storage-Layout) for what lives where on disk
- [Transactions and TxV3](Concepts-Transactions-And-TxV3) for the on-wire encoding
- [Indexing](Concepts-Indexing) for the SQLite sidecar
