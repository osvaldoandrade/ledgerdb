# TypeScript SDK (`@osvaldoandrade/ledgerdb`)

The TypeScript SDK is a **CLI bridge**: each call to the client spawns the
`ledgerdb` binary with appropriate arguments and parses its JSON output. This
keeps the Node side small (no native bindings, no FFI) at the cost of a
process spawn per call.

A native binding that talks to LedgerDB core in-process is tracked in
[#63](https://github.com/osvaldoandrade/ledgerdb/issues/63).

- Package: `@osvaldoandrade/ledgerdb`
- Registry: GitHub Packages (`https://npm.pkg.github.com`)
- Source: [`packages/sdk-typescript/`](../../packages/sdk-typescript/)
- Tier: Tier 1 (see [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)).

## Install

```bash
# One-time: point @osvaldoandrade scope at GitHub Packages.
npm config set @osvaldoandrade:registry https://npm.pkg.github.com

npm install @osvaldoandrade/ledgerdb
```

The `postinstall` script downloads a prebuilt `ledgerdb` binary that matches
your platform/arch and places it at `node_modules/@osvaldoandrade/ledgerdb/bin/`.

### Overriding the binary

| Env var                       | Effect                                                            |
| ----------------------------- | ----------------------------------------------------------------- |
| `LEDGERDB_BIN`                | Use a preinstalled binary; skip download entirely.                |
| `LEDGERDB_SKIP_DOWNLOAD=1`    | Skip the postinstall download (you must provide the binary).      |
| `LEDGERDB_RELEASE_TAG`        | Pin the GitHub release tag (defaults to `v<package-version>`).    |
| `LEDGERDB_DOWNLOAD_BASE`      | Override the download base URL (mirrors, offline installs).       |

Node 18+ is required.

## Quick start

```ts
import { LedgerDBClient } from "@osvaldoandrade/ledgerdb";

const client = new LedgerDBClient({
  repoPath: "/path/to/ledgerdb.git",
  autoSync: true,
});

await client.put("tasks", "task_0001", {
  title: "Ship v1",
  status: "todo",
});

const { doc } = await client.get("tasks", "task_0001");
console.log(doc);
```

## Document operations

```ts
// put — full snapshot
await client.put("tasks", "task_0001", { title: "Ship v1", status: "todo" });

// get — latest materialized version
const { doc, tx_hash, tx_id, op } = await client.get("tasks", "task_0001");

// patch — RFC 6902 operations
await client.patch("tasks", "task_0001", [
  { op: "replace", path: "/status", value: "done" },
]);

// delete — append tombstone
await client.delete("tasks", "task_0001");

// log — full history for a document
const entries = await client.log("tasks", "task_0001");

// revert — roll back to a specific transaction
await client.revert("tasks", "task_0001", { txId: "01HXX..." });
```

All write methods return `{ commit, tx_hash, tx_id }` (the IDs of the
transaction the CLI just wrote).

Payloads can be passed as a JS value (the client `JSON.stringify`s for you)
or as a pre-encoded string.

## Index sync

The SQLite sidecar (`index.db`) is updated by running the equivalent of
`ledgerdb index sync` / `ledgerdb index watch`. The client gives you both:

```ts
// One-shot:
const result = await client.indexSync();
console.log(result.commits, result.docs_upserted);

// Background watch (long-running child process):
const child = client.startIndexWatch({ stdio: "pipe" });
child.stdout?.on("data", (chunk) => process.stdout.write(chunk));
```

`startIndexWatch` returns the spawned `ChildProcess`. Stop it with
`child.kill()`. By default it inherits stdio; pass `stdio: "pipe"` to capture
the per-tick JSON summaries.

## Push to remote

```ts
await client.push();
```

When `autoSync` is `true` (default), each write already wraps a `fetch` +
`push` cycle inside the binary, so explicit `push()` is only needed if you
turned `autoSync` off.

## How it works under the hood

Every call shells out to the binary resolved via `resolveBinaryPath()`:

1. `LEDGERDB_BIN` if set.
2. Otherwise, `node_modules/@osvaldoandrade/ledgerdb/bin/ledgerdb` (or
   `ledgerdb.exe` on Windows).

Each command runs as:

```
<binary> --repo <repoPath> [--json] [--sync=false] <subcommand...>
```

This means:

- Every call has process-spawn overhead. For latency-sensitive workloads
  (loops, hot paths), batch into the CLI's `--json` interface or wait for
  the native binding ([#63](https://github.com/osvaldoandrade/ledgerdb/issues/63)).
- All errors thrown by the client wrap the binary's `stderr`. Exit codes
  follow the same taxonomy as the CLI
  ([`internal/cli/errors.go`](../../internal/cli/errors.go)):
  `1` internal, `2` validation, `3` not found, `4` conflict.

## See also

- [`packages/sdk-typescript/README.md`](../../packages/sdk-typescript/README.md)
  — package-level README and binary download details.
- [`packages/sdk-typescript/src/client.ts`](../../packages/sdk-typescript/src/client.ts)
  — full TypeScript surface.
- Native TS binding: [#63](https://github.com/osvaldoandrade/ledgerdb/issues/63).
