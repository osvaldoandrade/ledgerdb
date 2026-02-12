# Overview

LedgerDB exists for a specific class of systems: systems where historical truth is not optional. In many databases, the current row is the product and history is an afterthought. In LedgerDB, history is first-class. Every change is a durable, immutable fact stored as a Git object, and the current state is a projection built from those facts.

This architectural choice is not cosmetic. It changes what the database can guarantee. Instead of asking whether a row was updated, LedgerDB can answer how it evolved, in which causal order, from which parent state, and by which actor path. That gives engineering teams a strong foundation for auditability, reproducibility, incident analysis, and offline-first operation.

## The Core Mental Model

The easiest way to understand LedgerDB is to separate three layers that are usually collapsed in traditional databases.

The first layer is the immutable transaction stream. Every write generates a deterministic transaction payload and a new Git object. This stream is append-only in `append` history mode, so no write destroys prior evidence.

The second layer is the materialized state. This is the "current answer" for each document, derived from transaction history and optionally mirrored into the `state/` tree for efficient indexing.

The third layer is query acceleration. SQLite sidecar indexes convert state projections into low-latency SQL reads without changing ledger semantics.

When you keep these layers explicit, the behavior of the system becomes predictable: writes optimize for correctness and verifiability; reads optimize for practical query ergonomics; replication optimizes for standard Git transport and eventual convergence.

## Concrete Example: Task Lifecycle

Consider a team tracking deployment tasks for multiple services.

They initialize a repository, apply schema and indexes, then create and evolve a task:

```bash
ledgerdb init --name "Ops Ledger" --repo ./ledgerdb.git --layout sharded --history-mode append
ledgerdb collection apply tasks --schema ./schemas/task.json --indexes "status,service"

ledgerdb doc put tasks "task_0001" --payload '{"service":"billing","title":"deploy v1.8","status":"todo"}'
ledgerdb doc patch tasks "task_0001" --ops '[{"op":"replace","path":"/status","value":"running"}]'
ledgerdb doc patch tasks "task_0001" --ops '[{"op":"replace","path":"/status","value":"done"}]'
```

What happened semantically is more important than the CLI output. The first command established a baseline snapshot of the document. The next two commands appended causal deltas. At no point did the system overwrite history. A verifier can later recompute lineage from genesis to head and confirm that the final `status="done"` state is cryptographically consistent with all prior transitions.

If the team enables SQLite indexing:

```bash
ledgerdb index watch --db ./index.db --mode state --interval 1s --fast --batch-commits 200
```

then a query such as `SELECT * FROM collection_tasks WHERE status='done'` becomes near real-time while still being derived from immutable ledger state.

## Why This Model Works in Distributed Environments

Distributed systems fail in ways centralized systems often hide: intermittent network partitions, concurrent updates from independent clients, out-of-order synchronization, and inconsistent clocks. LedgerDB avoids relying on global coordination for the common write path. It uses optimistic concurrency (CAS on head refs), deterministic serialization, and explicit parent lineage.

That means conflicts are not silently overwritten. If two writers diverge, one CAS wins and the other must reconcile on top of the new head. This is a safer failure mode because divergence is explicit, observable, and resolvable through policy.

Replication also benefits from this design. Because persistence is Git-native, nodes synchronize with standard fetch/push semantics. A node can continue operating locally and converge later. The database behavior remains understandable because sync is not a hidden side channel; it is an explicit operation with visible branch state.

## Performance and Tradeoff Profile

LedgerDB is not a universal replacement for low-latency in-memory key-value stores. Its value proposition is different. It offers strong auditability and deterministic history at the cost of more structured write mechanics and projection-oriented read optimization.

In practice, teams adopt LedgerDB when they need one or more of the following properties: tamper-evident history, offline-first writes, reproducible state evolution, and operationally simple replication using standard Git tooling. Teams usually avoid LedgerDB when they require microsecond-level hot-path reads with no historical semantics or when globally coordinated write throughput is the primary goal.

## Reading Path Through This Wiki

Start with [Architecture](Architecture) for the runtime data flow. Then read [Storage Engine and Interface](Storage-Engine-and-Interface) and [Execution Model and Consistency](Execution-Model-and-Consistency) to understand write guarantees. Finish with [Use Cases](Use-Cases) to map design to real workloads.
