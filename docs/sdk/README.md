# LedgerDB SDKs

LedgerDB is a Git-native immutable document database. The "server" is a Git
repository — all database logic lives in the client SDK (see
[`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md) for the architectural contract).

This section documents each official language binding, how to install it, and
its current maturity. Where a language is not yet shipped, you'll find a stub
that previews the planned API and links to the tracking issue.

## Status matrix

| Language       | Package                                  | Status                         | Install                                                                    | Issue / Tracking |
| -------------- | ---------------------------------------- | ------------------------------ | -------------------------------------------------------------------------- | ---------------- |
| **Go**         | `pkg/ledgerdbsdk`                        | Stable (first-class)           | `go get github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk`                | —                |
| **TypeScript** | `@osvaldoandrade/ledgerdb`               | Beta — CLI bridge              | `npm install @osvaldoandrade/ledgerdb` (GitHub Packages registry)          | Native: [#63](https://github.com/osvaldoandrade/ledgerdb/issues/63) |
| **Python**     | `ledgerdb` (planned)                     | Coming soon                    | n/a                                                                        | [#60](https://github.com/osvaldoandrade/ledgerdb/issues/60) |
| **Rust**       | `ledgerdb` (planned crate)               | Coming soon                    | n/a                                                                        | [#62](https://github.com/osvaldoandrade/ledgerdb/issues/62) |
| **Java**       | `io.ledgerdb:ledgerdb` (planned)         | Coming soon                    | n/a                                                                        | [#64](https://github.com/osvaldoandrade/ledgerdb/issues/64) |

The umbrella epic for future SDKs is
[#59](https://github.com/osvaldoandrade/ledgerdb/issues/59).

## Per-language documentation

- [Go SDK](./go.md) — direct, in-process access to core services.
- [TypeScript SDK](./typescript.md) — CLI bridge (`@osvaldoandrade/ledgerdb`).
- [Python SDK](./python.md) — planned.
- [Rust SDK](./rust.md) — planned.
- [Java SDK](./java.md) — planned.

## Capability summary

All SDKs target the same set of operations against a LedgerDB repository:

| Operation        | Description                                                          |
| ---------------- | -------------------------------------------------------------------- |
| `put`            | Write a full JSON snapshot for `(collection, docID)`.                |
| `get`            | Read the materialized document (latest version).                     |
| `patch`          | Apply RFC 6902 JSON Patch operations on top of the latest version.   |
| `delete`         | Append a tombstone — the document is gone but the history remains.   |
| `log`            | List the transaction history for a document.                         |
| `revert`         | Roll a document back to a previous transaction (by `TxID`/`TxHash`). |
| `index sync`     | Project Git transactions into the SQLite sidecar (`index.db`).       |
| `index watch`    | Long-running loop that keeps the SQLite sidecar fresh.               |

The Go SDK exposes all of these directly. The TypeScript SDK exposes them via
a CLI bridge today; a native binding is tracked in
[#63](https://github.com/osvaldoandrade/ledgerdb/issues/63). Future SDKs will
match the same surface.

## Choosing an SDK

- **Embedded / backend / CLI tooling (Go):** use the Go SDK. It is the
  reference implementation and runs in-process with no extra binary.
- **Node.js / Bun / Deno scripts and services:** use the TypeScript SDK. It
  requires the `ledgerdb` binary (downloaded automatically by `postinstall`).
- **Other languages:** follow the linked tracking issue, or use the
  `ledgerdb` CLI directly (`--json`) until a native binding is available.

See also:

- [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md) — the SDK conformance spec.
- [`docs/04_EXECUTION.md`](../04_EXECUTION.md) — write pipeline / CAS loop.
- [`docs/05_QUERYING.md`](../05_QUERYING.md) — SQLite sidecar / read path.
