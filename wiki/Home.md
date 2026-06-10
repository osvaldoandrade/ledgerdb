# LedgerDB

LedgerDB is a git-native immutable document database. The data plane is a bare git repository on local disk; every document write is serialized into a TxV3 protobuf blob and committed against `refs/heads/main` under optimistic concurrency control. The `ledgerdb` CLI is the primary surface, with a Go SDK (`pkg/ledgerdbsdk`) and a TypeScript SDK (`@osvaldoandrade/ledgerdb`) on top of it. A SQLite sidecar, maintained by `ledgerdb index watch`, projects the immutable log into per-collection tables for SQL queries. There is no server, no gRPC plane, and no built-in consensus protocol: replication is whatever git already does — `fetch` and `push` over any transport git understands.

## Read by section

The wiki is organized into six sections. Each section has an overview page that explains what the section covers, followed by topical pages that go deep on one idea at a time.

The [Get Started](Get-Started-Overview) section walks through installing the CLI, running a single-writer workstation, attaching the SQLite sidecar, layering a distributed topology over git remotes, and putting the whole thing inside a container. Start here if you have never used LedgerDB before.

The [Concepts and Architecture](Concepts-Overview) section explains the document and collection model, the TxV3 transaction format, append vs. amend history modes, the on-disk storage layout, causal versioning, conflict resolution, indexing, integrity verification, and how replication actually works given that the substrate is plain git.

The [SDKs and CLI](SDK-Overview) section is the reference for the `ledgerdb` binary, the Go SDK, the TypeScript SDK, and the interactive REPL with `query explain`.

The [LedgerDB IO](IO-Overview) section is the on-disk and on-wire reference: the TxV3 protobuf format, the git object layout, the state tree, the SQLite sidecar schema, the sync protocol, and the bundle format used for backups.

The [Observability](Observability-Overview) section covers structured logging, the opt-in metrics surface on `index watch`, and the audit log.

The [Performance](Performance-Overview) section reports measured throughput on the hot paths (CAS retries, TxV3 marshalling, sidecar batch flush), explains the tuning knobs that move those numbers, and documents the bench harness.

## Quick paths

| Intent | Start here |
| --- | --- |
| I want to install LedgerDB on my laptop | [Run Locally](Get-Started-Run-Locally) |
| I want SQL queries over my documents | [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index) |
| I want multiple writers via git remotes | [Run Distributed](Get-Started-Run-Distributed) |
| I want a container image | [Run In Docker](Get-Started-Run-In-Docker) |
| I want to understand the architecture | [Architecture Overview](Concepts-Architecture-Overview) |
| I want the full CLI reference | [SDK CLI Reference](SDK-CLI-Reference) |
| I want to embed LedgerDB in a Go service | [Go SDK](SDK-Go-SDK) |
| I want to call LedgerDB from Node | [TypeScript SDK](SDK-TypeScript-SDK) |
| I want to know what the TxV3 bytes look like | [TxV3 Format](IO-TxV3-Format) |
| I want to tune throughput | [Tuning Knobs](Performance-Tuning-Knobs) |
| Something broke and I need to debug | [Observability Overview](Observability-Overview) |

## Source code and issues

The LedgerDB source lives at [github.com/osvaldoandrade/ledgerdb](https://github.com/osvaldoandrade/ledgerdb). Issues, discussions, and pull requests are accepted on GitHub. The wiki is generated from `wiki/` in the main repository — edits go via PR there.
