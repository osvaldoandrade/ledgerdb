# LedgerDB Roadmap

This roadmap captures the active epics driving LedgerDB toward a stable
v1.0. It is intentionally a *summary* — each epic has its own GitHub issue
where detailed scope, child tasks, and acceptance criteria live. The
ordering here is by epic number, not strict priority; the maintainer
sequences work based on dependencies and contributor availability.

For the formal stability plan and what v1.0 will guarantee, see
[`docs/V1_STABILITY.md`](docs/V1_STABILITY.md). For our release schedule,
see the **Release cadence** section in [`README.md`](README.md).

---

## Active epics

### Epic [#3](https://github.com/osvaldoandrade/ledgerdb/issues/3) — Local Web Console (ephemeral, loopback-only)
An opt-in local web UI bound to `127.0.0.1` that lets operators browse
collections, inspect transactions, and run ad-hoc SQL against the sidecar
index. The console is ephemeral and never listens on a public interface.

### Epic [#4](https://github.com/osvaldoandrade/ledgerdb/issues/4) — Opt-in instrumentation for `ledgerdb index watch`
Structured metrics (OpenTelemetry or Prometheus exposition) for the index
watcher so operators can monitor sidecar freshness, batch latency, and
SQLite write throughput. Disabled by default; opt-in via flag/env.

### Epic [#5](https://github.com/osvaldoandrade/ledgerdb/issues/5) — Advanced Querying (FTS, aggregations, GraphQL)
Builds on the SQLite sidecar to expose full-text search, aggregation
helpers, and an experimental GraphQL surface. Keeps the storage engine
unchanged — purely a read-path projection.

### Epic [#6](https://github.com/osvaldoandrade/ledgerdb/issues/6) — Binary Blob Storage via git-lfs
First-class support for large binary payloads (images, PDFs, exports) by
integrating `git-lfs`, with content-addressed pointers in the regular
ledger so integrity guarantees still hold.

### Epic [#7](https://github.com/osvaldoandrade/ledgerdb/issues/7) — Backup, Archive & Disaster Recovery
Defines snapshot, archive, and restore primitives: cold-storage formats,
verified restores, and pruning strategies that preserve cryptographic
chain integrity for the retained range.

### Epic [#8](https://github.com/osvaldoandrade/ledgerdb/issues/8) — Performance, Benchmarks & Tuning
A reproducible bench harness, baseline measurements, and targeted
optimisations for the hot paths (CAS retries, TxV3 marshalling, sidecar
batch flush). Outputs published with each release.

### Epic [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59) — SDK Expansion (Python, Rust, native TS, Java)
Today only Go is first-class and the TypeScript SDK shells out to the CLI.
This epic expands to native Python, Rust, native TypeScript, and Java
SDKs sharing a common API contract.

### Epic [#73](https://github.com/osvaldoandrade/ledgerdb/issues/73) — Security & Compliance
Threat model documentation, signed-commit enforcement options, supply-chain
hardening (SLSA provenance, SBOM in releases), and the disclosure tooling
referenced by [`SECURITY.md`](SECURITY.md).

### Epic [#77](https://github.com/osvaldoandrade/ledgerdb/issues/77) — Community, Governance & Path to v1.0
Tracks community-facing work: governance documentation (this pack),
contributor onboarding, the v1.0 stability commitment, and the decisions
required to declare v1.0 ready to ship.

### Epic [#88](https://github.com/osvaldoandrade/ledgerdb/issues/88) — CRDT Modes & Conflict Resolution UX
Beyond the current JSON merge strategies: pluggable CRDT modes (LWW,
multi-value, OR-set) and a CLI/SDK UX for surfacing and resolving
conflicts when automatic merging is insufficient.

### Epic [#89](https://github.com/osvaldoandrade/ledgerdb/issues/89) — Developer Experience & Tooling
Quality-of-life improvements across the developer surface: shell
completion, better error messages, an `init`-style wizard, schema
scaffolding, and improved logs/diagnostics.

---

## v1.0 stability plan

The detailed scope of what v1.0 freezes (and what stays explicitly
unstable) lives in [`docs/V1_STABILITY.md`](docs/V1_STABILITY.md). In
short: the TxV3 wire format, the CLI command surface, the public Go SDK
types, and the manifest schema are the four pillars that v1.0 will
guarantee. Everything under `internal/` and the sidecar SQLite schema
remain mutable.

Until v1.0 ships, minor releases may include breaking changes. See the
release cadence section of the README for how those land in practice, and
`docs/DEPRECATION.md` for how we phase removals.
