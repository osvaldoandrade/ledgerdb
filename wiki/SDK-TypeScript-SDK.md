# SDK TypeScript SDK

The TypeScript SDK lives at [`packages/sdk-typescript/`](../tree/main/packages/sdk-typescript) and is published as `@osvaldoandrade/ledgerdb` on GitHub Packages. It is a CLI bridge, not a native binding. Every method on `LedgerDBClient` is implemented by spawning the `ledgerdb` binary with the right flags, capturing stdout, parsing it as JSON, and resolving a Promise. There is no Go runtime in the Node process, no FFI, no native module to compile. The cost of that decision is one process spawn per call (and one fork per `index watch`); the benefit is portability — the same package works under Node, Deno-with-node-compat, Bun, and inside slim containers where adding a native module would mean adding a toolchain.

The sister package `@osvaldoandrade/ledgerdb` at [`npm/ledgerdb/`](../tree/main/npm/ledgerdb) is a thinner wrapper: it has no TypeScript surface, just a `bin/ledgerdb` shim that spawns the downloaded binary. It exists so that callers who only want the CLI on `PATH` (for shell scripts, for CI) can `npm i -g @osvaldoandrade/ledgerdb` without pulling in the TS API. The two packages currently share the package name but are published at different versions from different directories; future releases may rename one to disambiguate.

## How the bridge is wired

`LedgerDBClient` in [`packages/sdk-typescript/src/client.ts`](../tree/main/packages/sdk-typescript/src/client.ts) takes a `ClientConfig` and stores three things: the resolved binary path, the merged environment, and the index defaults. Every call shape is identical:

```ts
private async execJson<T>(args: string[], writeOperation: boolean): Promise<T> {
    const fullArgs = [...this.baseArgs(writeOperation, true), ...args];
    const result = await execFileAsync(this.binaryPath, fullArgs, {
      cwd: this.repoPath,
      env: this.env,
      maxBuffer: 10 * 1024 * 1024,
    });
    return JSON.parse(result.stdout) as T;
}
```

`baseArgs` prepends `--repo <repoPath>` and, when the call expects JSON, `--json`. For write operations it additionally appends `--sync=false` when the client was constructed with `autoSync: false`. The `maxBuffer` cap of 10 MiB is the implicit ceiling on a single response; callers retrieving very large documents or wide log pages should be aware that `execFile` rejects with `ERR_CHILD_PROCESS_STDIO_MAXBUFFER` if it is exceeded.

`resolveBinaryPath()` checks `LEDGERDB_BIN` first; if set and non-empty, it is used unchanged. Otherwise it resolves to `<package_dir>/bin/ledgerdb` (`.exe` on Windows). That path is the one the postinstall script populates.

## The API surface

The public methods on `LedgerDBClient` mirror the CLI subset that the TS SDK chose to expose. Each call delegates to one CLI subcommand:

| Method | CLI command | Notes |
|---|---|---|
| `get(collection, docId)` | `doc get` | Returns `GetResult = { doc, tx_hash?, tx_id?, op? }`. |
| `put(collection, docId, payload)` | `doc put --payload <json>` | Returns `PutResult = { commit, tx_hash, tx_id }`. The payload is `JSON.stringify`'d unless it is already a string. |
| `patch(collection, docId, ops)` | `doc patch --ops <json>` | Same `PutResult`. |
| `delete(collection, docId)` | `doc delete` | Same `PutResult`. |
| `log(collection, docId)` | `doc log` | Returns `LogEntry[]`; the `next_cursor` field is currently dropped — callers needing pagination should shell out directly or move to the Go SDK. |
| `revert(collection, docId, { txId, txHash })` | `doc revert --tx-id ... --tx-hash ...` | At least one of `txId` or `txHash` is required. |
| `indexSync(overrides?)` | `index sync` | Returns `IndexSyncResult`. |
| `startIndexWatch(options?)` | `index watch` | Spawns a child process and returns the `ChildProcess` handle; see below. |
| `push()` | `push` | Useful with `autoSync: false`. |

The `IndexConfig` shape on the constructor maps directly onto `index` subcommand flags. `dbPath` becomes `--db`, `mode` becomes `--mode`, `intervalMs` becomes `--interval <N>ms`, `jitterMs` becomes `--jitter <N>ms`, `batchCommits` becomes `--batch-commits`, `fast` becomes `--fast`, `fetch === false` becomes `--fetch=false`, `onlyChanges` becomes `--only-changes`. `buildIndexArgs` enforces the same `interval > 0` guard the CLI does and throws `"index watch interval must be > 0"` early.

`startIndexWatch` is the one method that does not use `execFile`. It uses `spawn` and returns the live `ChildProcess` so the caller can attach `data`, `error`, and `exit` listeners. The default `stdio` is `"inherit"`, which forwards the watcher's stdout/stderr to the Node process; pass `stdio: "pipe"` to capture the JSON Lines stream (`--json` is on by default through `baseArgs(false, options?.json ?? false)`, but the watch path does not include it — set `options.json = true` to get structured per-iteration output and parse it line by line).

The error model is one class. Any non-zero exit from the spawned process is wrapped in a Node `Error` whose message is the original `error.message` plus a trimmed copy of stderr. There are no typed error subclasses. Callers needing structured errors should parse stderr — the CLI emits a JSON envelope (`{ code, kind, message }`) when `--json` is set, which is automatic for the `exec*` methods through `baseArgs`.

## Postinstall and binary resolution

[`packages/sdk-typescript/scripts/postinstall.js`](../tree/main/packages/sdk-typescript/scripts/postinstall.js) is the half of the package that nobody usually reads but everybody depends on. It runs on `npm install` and downloads a prebuilt `ledgerdb` binary from GitHub Releases. The asset name is derived from the host platform and arch through the `platformMap` and `archMap` tables — `darwin`, `linux`, `windows` and `amd64`, `arm64` — and assembled into `ledgerdb-<platform>-<arch>[.exe]`. The release tag is `v<package.version>` by default and can be overridden with `LEDGERDB_RELEASE_TAG`. The base URL is `https://github.com/osvaldoandrade/ledgerdb/releases/download/<tag>` and can be overridden with `LEDGERDB_DOWNLOAD_BASE`, which is the right knob for testing against a forked or staged release.

The script honours two skip switches. `LEDGERDB_SKIP_DOWNLOAD=1` (or `=true`) exits cleanly without downloading; `LEDGERDB_BIN=<path>` does the same and signals that the SDK should use that binary at runtime. Both are the right thing in CI where the binary is already provisioned and an HTTPS round trip would be wasted, or in offline development. If the download fails for any reason, the script removes the partial file and exits non-zero with a hint pointing at the two skip switches.

On runtime, `resolveBinaryPath()` in `client.ts` mirrors the same logic: prefer `LEDGERDB_BIN`, then fall back to the bundled binary. The two ends of the pipeline therefore agree without sharing state.

The `npm/ledgerdb/` package uses the same postinstall pattern (`npm/ledgerdb/scripts/postinstall.js`, which is structurally identical) and ships only the `bin/ledgerdb` shim, no TypeScript. It is the right install for a developer who wants `ledgerdb` on `PATH` and nothing else.

## Tradeoffs

The honest comparison with the Go SDK is dominated by the spawn cost. A `doc put` from `LedgerDBClient.put` is one `fork`/`execve` to start the binary, one full git operation, one JSON encode, and one stdout drain. On a modern macOS or Linux host the per-call floor is in the 10-30 ms range before the underlying git operation runs. For a workload doing a handful of writes per request that is invisible; for a workload doing thousands of small writes per second it dominates. The right pattern in that case is one of three:

The first is to move to the Go SDK described on [SDK Go SDK](SDK-Go-SDK). It removes the spawn cost entirely and is the right answer if the host application is already Go or can host a Go sidecar.

The second is to keep a `ledgerdb repl` child process resident and pipe commands to it. The REPL — documented on [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain) — dispatches each line through the same command surface but reuses one process. The TS SDK does not currently wrap this pattern; a caller has to use `child_process.spawn` directly. It is on the SDK roadmap.

The third is to batch on the caller side. If the workload is "apply N transactions and then read", the right shape is one `migrate apply` or a script-driven REPL invocation rather than N independent `put` calls. The TS SDK exposes no batching primitive of its own.

The corresponding upside is that the TS SDK has no native dependencies. It works in any Node 18+ environment that can reach GitHub Releases at install time (or that has `LEDGERDB_BIN` set), and there is no `node-gyp`, no prebuilt `.node` artefact to keep current per Node version. For most service-side workloads — a few writes per request, the occasional `query explain` driven from an admin endpoint — that is the right tradeoff.

## When to pick the TS SDK versus a raw child process

If the application already spawns the CLI from JavaScript and only needs a few methods, importing the TS SDK pays for itself: it ships the Promise wrappers, the `--repo` / `--json` plumbing, the index-argument builder, the binary-resolution logic. If the application needs commands that are not in the TS SDK surface (`migrate apply`, `backup`, `truncate`, `query explain`, `repl --script`), spawning the binary directly through `node:child_process` is the right move; the SDK is not load-bearing in that case.

The SDK does not currently expose `collection apply`, `schema scaffold`, `integrity verify`, `maintenance gc`, or any of the DR commands. They are reachable through `child_process` and through the resident-CLI pattern, but they are not first-class on the TS surface.

## Worked example

```ts
import { LedgerDBClient } from "@osvaldoandrade/ledgerdb";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);

async function main() {
  // collection apply is not in the SDK; shell out.
  await exec("ledgerdb", [
    "--repo", "./data.git",
    "collection", "apply", "tasks",
    "--schema", "./schemas/task.json",
    "--indexes", "status",
  ]);

  const client = new LedgerDBClient({
    repoPath: "./data.git",
    index: { mode: "state", intervalMs: 1000, batchCommits: 200, fast: true },
  });

  await client.put("tasks", "t1", { title: "Ship v1", status: "open" });
  await client.put("tasks", "t2", { title: "Write docs", status: "open" });

  const watch = client.startIndexWatch({ stdio: "pipe", json: true });
  watch.stdout?.on("data", (chunk) => {
    for (const line of chunk.toString().split("\n").filter(Boolean)) {
      const result = JSON.parse(line);
      if (result.txs_applied > 0) {
        console.log("indexed", result.txs_applied, "tx(s) up to", result.last_commit);
      }
    }
  });

  // Direct shell-out for the bits the SDK does not wrap.
  const explain = await exec("ledgerdb", [
    "--repo", "./data.git", "--json",
    "query", "explain",
    "SELECT doc_id FROM collection_tasks WHERE json_extract(payload,'$.status') = 'open' ORDER BY doc_id",
  ]);
  console.log("plan:", JSON.parse(explain.stdout));

  // Stop the watcher cleanly before exit.
  watch.kill("SIGTERM");
}

main().catch((err) => { console.error(err); process.exit(1); });
```

The example highlights the two patterns side by side: the high-level `client.put` / `client.startIndexWatch` for the things the SDK wraps, and direct `execFile` for the things it does not. Both paths cross the same CLI boundary.

## See also

- [SDK Overview](SDK-Overview) — frames the three surfaces and when each fits.
- [SDK CLI Reference](SDK-CLI-Reference) — the canonical list of commands the SDK shells out to.
- [SDK Go SDK](SDK-Go-SDK) — the in-process surface to compare against.
- [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain) — the resident-CLI pattern that mitigates spawn cost.
