# Get Started: Overview

This section is for readers who have never run LedgerDB before. The goal is concrete: by the end you have a bare repo on disk, a collection schema applied, a document written and read back, and a reasonable idea of which of the four supported runtime topologies — local single-repo, single-repo with SQLite sidecar, two-node sync over a git remote, or containerized — you actually want to keep. The pages that follow are written to be read in order on a first pass and as standalone references afterward.

LedgerDB is a CLI and a Go SDK; there is no server, no daemon, no embedded gRPC, no raft. The persistence engine is a bare git repository. Every write is a TxV3 protobuf blob (`internal/infra/txv3/`) committed to `refs/heads/main`. Replication is delegated to standard `git fetch` and `git push` against any remote you point the repo at. Querying past a key-value `get` happens by syncing a SQLite sidecar (`internal/infra/sqliteindex/`) and running SQL against it. The shape of the system does not change between topologies; what changes is how many copies of the bare repo exist and where the SQLite file lives.

## Who this section is for

If you are evaluating LedgerDB, the right entry point is [Run Locally](Get-Started-Run-Locally). You install once, you initialize a bare repo, you apply a collection, you write a document, you read it back, and you see the on-disk layout. The detour through the sidecar, multi-node sync, and Docker is unnecessary for evaluation — the local CLI is the same code path every other topology uses. There is no separate server binary to run.

If you are integrating LedgerDB into a Go service, read [Run Locally](Get-Started-Run-Locally) for the install and quickstart, then jump to [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index). The Go SDK lives at `pkg/ledgerdbsdk/`. It opens the bare repo directly through the same `internal/infra/gitrepo.Store` the CLI uses; there is no IPC or RPC. The SDK can also manage the SQLite sidecar — open the index, start a background watcher, run SQL queries — so a single process drives both the ledger and the query path.

If you need to share a database between two or more machines, read [Run Distributed](Get-Started-Run-Distributed). Synchronization is `git fetch` and `git push` against a shared remote, with optional auto-fetch before each write and auto-push after. The model is optimistic: a non-fast-forward push surfaces as `ErrSyncConflict` (`internal/domain/errors.go:6`), and the operator decides how to reconcile. There is no consensus protocol; the system is AP across nodes, and CP only at the single-writer level.

If you are running LedgerDB as a long-lived process in a container — typically the `ledgerdb index watch` loop next to an application — read [Run In Docker](Get-Started-Run-In-Docker). The container does not host a server; it runs a one-shot or long-poll CLI against a mounted bare repo. The Dockerfile pattern in that page is canonical because the repo does not ship a published image.

## What you will know after reading this section

Four concrete things, and they map onto the four runtime pages.

You will be able to install LedgerDB. Two install paths exist. The first is `curl -fsSL .../install.sh | sh`, which clones the repo at the configured ref and runs `go build ./cmd/ledgerdb` into the first writable directory on `$PATH`. The full rule set is in `install.sh`. The second is `npm install -g @osvaldoandrade/ledgerdb`, which downloads a prebuilt CLI binary as a `postinstall` step. A third path — `go install github.com/osvaldoandrade/ledgerdb/cmd/ledgerdb@latest` — works for developers who already have a Go toolchain. The binary itself is built from `cmd/ledgerdb/main.go` and calls into `pkg/ledgerdb.Execute()`.

You will know what the on-disk layout is. `ledgerdb init` creates a bare git repository (`HEAD`, `objects/`, `refs/`, `config`) plus a `manifest.json` at the root of the work tree describing the database name, the stream layout (`flat` or `sharded`), and the history mode (`append` or `amend`). The manifest is written by `internal/app/repo/init_service.go:31` and read back on every command by `internal/cli/root.go:53`. The committed tree carries one stream directory per collection (`collections/<name>/`) and the materialized state (`state/<collection>/<doc_id>`), one append-only stream of TxV3 blobs per document under `collections/<name>/.../tx-XXXXXX`, and the schema and index spec under `collections/<name>/schema.json` and `collections/<name>/indexes.json`.

You will know what writes a write. The minimum surface is `ledgerdb doc put <collection> <doc_id> --payload '{...}'`. Internally the put service (`internal/app/doc/put_service.go`) canonicalizes the JSON, encodes a TxV3, hashes it, stores it as a git blob, updates the state tree, and CASes `refs/heads/main` forward. Patches (`ledgerdb doc patch`) and deletes (`ledgerdb doc delete`) follow the same pipeline with different `Op` codes. Every write returns a `commit`, a `tx_hash`, and a `tx_id`; those three identifiers are the entire causal record of the change.

You will know how to read. The fast path is `ledgerdb doc get <collection> <doc_id>`, which reads the materialized state directly from the git tree without replaying history. The audit path is `ledgerdb doc log <collection> <doc_id>`, which walks the document's tx stream and returns one entry per change. The query path is the SQLite sidecar: `ledgerdb index sync --db ./index.db` materializes one table per collection (`collection_<name>`), and `ledgerdb query explain "<sql>"` plus the Go SDK's `Client.Query(ctx, sql, args...)` run SQL against it.

## How the pages connect

The four runtime pages are independently readable. Each starts from scratch and assumes nothing about the previous one. They share a common Quickstart workload: a `tasks` collection with a small JSON schema, a `task_0001` document, a couple of patches, and a read back. The point is to make the same exercise observable across topologies, so you can see exactly which behaviors change when persistence moves from a local directory to a mounted volume to a remote-shared bare repo, and which behaviors do not change at all.

When you have done the local run, the sidecar run will feel like the same exercise plus a SQLite file. When you have done the sidecar run, the distributed run will feel like the same exercise plus a second bare repo over the network. When you have done the distributed run, the Docker run will feel like one of the previous shapes wrapped in a container. That is intentional. The CLI, the on-disk layout, the TxV3 encoder, and the git store are identical across topologies. The only thing that changes is what surrounds the bare repo.

The reading order is also the complexity order. [Run Locally](Get-Started-Run-Locally) introduces install, init, collection apply, put/get/patch/delete, the doc log, and the Go SDK in that sequence. [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index) reintroduces the same workload and adds `ledgerdb index sync` and `ledgerdb index watch`, the `state` vs `history` modes, the polling knobs, the opt-in Prometheus exporter, and the JSON Lines audit log. [Run Distributed](Get-Started-Run-Distributed) adds a second node, the git remote, auto-fetch and auto-push, the CAS-on-`refs/heads/main` write path, and the conflict-resolution model. [Run In Docker](Get-Started-Run-In-Docker) wraps the watch loop in a container with a mounted volume.

## A word on conventions

The wiki uses `path/to/file.go:line` to cite source. Every assertion about a default value, a flag, an environment variable, or a CLI subcommand is traceable to either `internal/cli/commands.go`, `internal/cli/root.go`, the relevant `internal/app/<area>/` service, or `install.sh`. If you see a flag quoted without a citation, treat it as a typo and open an issue.

Diagrams use Mermaid. Code snippets are runnable as written when copy-pasted into a Unix shell. Where a snippet uses a placeholder, the placeholder is enclosed in `<angle-brackets>` so it is obvious you need to substitute. The standard placeholders are `./ledgerdb.git` for the bare repo path and `./index.db` for the SQLite sidecar.

## What this section does not cover

Anything beyond "first document written and read end to end on each topology" is somewhere else. For the conceptual model — what a TxV3 actually is, what the Merkle DAG buys you, what canonical JSON is, why `state/` exists alongside the tx log — read the architecture chapter. For the SQLite schema and the supported query shapes, see the querying chapter. For backup, restore, snapshot, and gc, see the operations chapter. For the integrity verifier and signing model, see the integrity chapter. None of those are this section's job.

If you finish this section and want a typed Go API rather than the CLI, the next click is the SDK reference inside the [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index) page; it walks the same `Open` / `Get` / `Put` / `Query` shape used by every Go integration. If you want offline-first or multi-machine setups, [Run Distributed](Get-Started-Run-Distributed) is the entry point.

## Prerequisites

The pages in this section assume some baseline. The exact baseline depends on which runtime page you intend to follow.

For [Run Locally](Get-Started-Run-Locally) you need a Unix-like environment (Linux, macOS, or Windows via Git Bash), `git` on `$PATH`, `jq` for parsing JSON output, and Go 1.22 or newer if you intend to build from source. The install script needs both `git` and `go`; it clones the repo at `LEDGERDB_REF` (default `main`) and runs `go build ./cmd/ledgerdb`. The page does not assume any prior LedgerDB knowledge.

For [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index) you need everything above plus a writable directory for the SQLite file. The sidecar uses `modernc.org/sqlite`, which is pure-Go — no system SQLite required.

For [Run Distributed](Get-Started-Run-Distributed) you need a second machine (or two clones on one machine) and a git remote both can reach. A bare repo hosted on any git server works: GitHub, Gitea, a plain `ssh://` URL into a server with a bare repo, even a shared filesystem path. The `internal/infra/gitrepo/auth.go` module reads `LEDGERDB_GIT_TOKEN`, `GITHUB_TOKEN`, or `GH_TOKEN` for HTTPS auth and falls back to system `git` for SSH.

For [Run In Docker](Get-Started-Run-In-Docker) you need a working Docker daemon. The repository does not publish an image today, so the page presents a small canonical Dockerfile that builds the CLI into a minimal base. The page assumes basic `docker run` familiarity.

## A common thread

Across all four runtime pages the same architectural truth holds: one CLI binary, one bare git repo on disk, optional SQLite for queries, optional git remote for replication. The shape of the system does not grow when you scale it; the shape of the surrounding orchestration does. There is no clustering layer to enable, no consensus quorum to size, no server fleet to manage. That is the design promise. By the end of this section you will have seen that promise hold under four different operational pressures and you will be ready to decide which topology to commit to.

## Where this leads

The cleanest path through LedgerDB for someone learning it for the first time is to install via `curl install.sh | sh`, run `ledgerdb --help` once to confirm the CLI is on `$PATH`, follow [Run Locally](Get-Started-Run-Locally) end to end, then read the architecture overview. At that point you have committed a transaction, you have read it back, and you have read why the system is shaped the way it is. Every other section becomes a reference you consult on demand.

## See also

- [Run Locally](Get-Started-Run-Locally)
- [Run With Sidecar Index](Get-Started-Run-With-Sidecar-Index)
- [Run Distributed](Get-Started-Run-Distributed)
- [Run In Docker](Get-Started-Run-In-Docker)
