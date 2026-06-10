# SDK Language Roadmap

The first-class SDKs today are Go (in-process, see [Go SDK](SDK-Go-SDK)) and TypeScript (CLI bridge, see [TypeScript SDK](SDK-TypeScript-SDK)). A small umbrella epic — [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59) — tracks the work to add native Python, Rust, and Java bindings. This page is the catalogue of those planned SDKs: the rationale for each, the shared API contract they target, the initial implementation strategy, and the tracking issues to watch.

The general principle is consistent across languages. Every native SDK targets the same operation surface as the Go SDK, so applications can move between languages without re-learning the model. The first implementation in any new language is usually a CLI bridge (mirroring the TypeScript approach) because it ships in days rather than months; the native binding lands later, behind the same public API, as a transparent performance upgrade.

## Status matrix

| Language       | Package                                  | Status                         | Issue / Tracking |
| -------------- | ---------------------------------------- | ------------------------------ | ---------------- |
| **Go**         | `pkg/ledgerdbsdk`                        | Stable (Tier 1, in-process)    | shipped          |
| **TypeScript** | `@osvaldoandrade/ledgerdb`               | Beta (Tier 1, CLI bridge)      | Native binding: [#63](https://github.com/osvaldoandrade/ledgerdb/issues/63) |
| **Python**     | `ledgerdb` (planned)                     | Coming soon                    | [#60](https://github.com/osvaldoandrade/ledgerdb/issues/60) |
| **Rust**       | `ledgerdb` (planned crate)               | Coming soon (Tier 2)           | [#62](https://github.com/osvaldoandrade/ledgerdb/issues/62) |
| **Java**       | `io.ledgerdb:ledgerdb` (planned)         | Coming soon                    | [#64](https://github.com/osvaldoandrade/ledgerdb/issues/64) |

The umbrella epic for future SDKs is [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59).

## Shared API contract

All SDKs target the same set of operations against a LedgerDB repository:

| Operation        | Description                                                          |
| ---------------- | -------------------------------------------------------------------- |
| `put`            | Write a full JSON snapshot for `(collection, docID)`.                |
| `get`            | Read the materialised document (latest version).                     |
| `patch`          | Apply RFC 6902 JSON Patch operations on top of the latest version.   |
| `delete`         | Append a tombstone — the document is gone but the history remains.   |
| `log`            | List the transaction history for a document.                         |
| `revert`         | Roll a document back to a previous transaction (by `TxID`/`TxHash`). |
| `index sync`     | Project Git transactions into the SQLite sidecar (`index.db`).       |
| `index watch`    | Long-running loop that keeps the SQLite sidecar fresh.               |

The Go SDK exposes all of these directly. The TypeScript SDK exposes them via a CLI bridge today. Every future SDK matches the same surface.

The SDK conformance contract — what each binding must implement and what it may extend — is intended to live in this wiki under the SDKs section as each language ships; until then, the Go SDK at [Go SDK](SDK-Go-SDK) and the CLI surface at [CLI Reference](SDK-CLI-Reference) are the canonical references.

## Python SDK

**Tracking:** [#60](https://github.com/osvaldoandrade/ledgerdb/issues/60).

A native Python SDK is planned. The initial implementation will likely use a CLI bridge (mirroring the TypeScript approach) before moving to a native binding. The package name will be `ledgerdb` on PyPI.

### Planned API shape

```python
from ledgerdb import Client

c = Client.open(".")  # path to ledgerdb.git

c.put("users", "u1", {"name": "Ada", "role": "admin"})

doc = c.get("users", "u1")
print(doc.payload, doc.tx_id, doc.tx_hash)

c.patch("users", "u1", [
    {"op": "replace", "path": "/role", "value": "owner"},
])

c.delete("users", "u1")

for entry in c.log("users", "u1"):
    print(entry.timestamp, entry.op, entry.tx_id)

c.revert("users", "u1", tx_id="01HXX...")
```

The sidecar SQLite database will be exposed through a context manager that yields a DB-API 2.0 connection, so Python users can run ad-hoc SQL with their preferred tools (`sqlite3`, `sqlalchemy`, `pandas`):

```python
with c.index() as conn:
    rows = conn.execute(
        'SELECT doc_id FROM "collection_users" WHERE deleted = 0',
    ).fetchall()
```

### In the meantime

You can drive LedgerDB from Python today by shelling out to the `ledgerdb` CLI with `--json`:

```python
import json, subprocess

def put(repo, collection, doc_id, payload):
    out = subprocess.check_output([
        "ledgerdb", "--repo", repo, "--json",
        "doc", "put", collection, doc_id,
        "--payload", json.dumps(payload),
    ])
    return json.loads(out)
```

This is exactly the strategy the TypeScript SDK uses today — see [TypeScript SDK](SDK-TypeScript-SDK) for the operational pattern.

## Rust SDK

**Tracking:** [#62](https://github.com/osvaldoandrade/ledgerdb/issues/62).

A Rust crate is planned as a Tier 2 SDK. The crate will target `git2-rs` (libgit2) plus a `rusqlite` sidecar, mirroring the Go SDK's in-process model. The initial scaffolding may use a CLI bridge before moving to a fully native implementation.

### Planned API shape

```rust
use ledgerdb::{Client, Config, RevertOptions};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Task {
    title: String,
    status: String,
}

fn main() -> anyhow::Result<()> {
    let client = Client::open(Config::new(".").auto_sync(true))?;

    client.put_json("tasks", "task_0001", &Task {
        title: "Ship v1".into(),
        status: "todo".into(),
    })?;

    let task: Task = client.get_into("tasks", "task_0001")?;
    println!("{} / {}", task.title, task.status);

    client.patch_json("tasks", "task_0001", &serde_json::json!([
        { "op": "replace", "path": "/status", "value": "done" }
    ]))?;

    for entry in client.log("tasks", "task_0001")? {
        println!("{} {} {}", entry.timestamp, entry.op, entry.tx_id);
    }

    client.revert("tasks", "task_0001", RevertOptions { tx_id: Some("01HXX...".into()), tx_hash: None })?;
    Ok(())
}
```

Index sidecar access is expected to look like:

```rust
let conn = client.index_connection()?;
let mut stmt = conn.prepare(
    r#"SELECT doc_id, payload FROM "collection_tasks" WHERE deleted = 0"#,
)?;
```

### In the meantime

You can integrate from Rust today by invoking the `ledgerdb` CLI through `std::process::Command` with the `--json` flag — the JSON contract is stable and is what the TypeScript SDK already relies on. See [TypeScript SDK](SDK-TypeScript-SDK) for the call pattern (it transfers one-to-one to any language that can spawn a child process).

## Java SDK

**Tracking:** [#64](https://github.com/osvaldoandrade/ledgerdb/issues/64).

A Java SDK is planned to extend LedgerDB's reach into JVM ecosystems (Spring, Quarkus, Android tooling). The initial implementation will most likely use a CLI bridge before moving to JGit + a JDBC SQLite driver for full in-process access. The Maven coordinate will be `io.ledgerdb:ledgerdb`.

### Planned API shape

```java
import io.ledgerdb.Client;
import io.ledgerdb.Config;
import io.ledgerdb.RevertOptions;

import java.util.List;
import java.util.Map;

public class Example {
    public static void main(String[] args) throws Exception {
        try (Client client = Client.open(Config.builder()
                .repoPath(".")
                .autoSync(true)
                .build())) {

            client.put("tasks", "task_0001", Map.of(
                    "title", "Ship v1",
                    "status", "todo"));

            Map<String, Object> task = client.get("tasks", "task_0001");
            System.out.println(task);

            client.patch("tasks", "task_0001", List.of(
                    Map.of("op", "replace", "path", "/status", "value", "done")));

            client.log("tasks", "task_0001")
                  .forEach(e -> System.out.println(e.timestamp() + " " + e.op()));

            client.revert("tasks", "task_0001",
                    RevertOptions.byTxId("01HXX..."));
        }
    }
}
```

The SQLite sidecar is expected to be exposed through standard JDBC:

```java
try (var conn = client.indexConnection();
     var stmt = conn.prepareStatement(
         "SELECT doc_id FROM \"collection_tasks\" WHERE deleted = 0")) {
    try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getString("doc_id"));
        }
    }
}
```

### In the meantime

You can drive LedgerDB from Java today by invoking the `ledgerdb` CLI via `ProcessBuilder` with `--json`. The JSON contract is stable and matches what the TypeScript SDK already uses.

## Native TypeScript binding

**Tracking:** [#63](https://github.com/osvaldoandrade/ledgerdb/issues/63).

The TypeScript SDK at [TypeScript SDK](SDK-TypeScript-SDK) is a CLI bridge today: every method spawns the `ledgerdb` binary, parses JSON from stdout, and resolves a Promise. That ships immediately and works without a native module, but it pays a `fork`/`execve` per call (10-30ms floor before the underlying Git work runs), which dominates for workloads doing many small writes per second.

A native binding that talks to LedgerDB core in-process is on the roadmap. The expected architecture is the Go `pkg/ledgerdb` C-shared library (`make build-core-shared`) loaded into Node through `node-addon-api` or a similar bridge. The same `LedgerDBClient` interface stays in place; only the implementation underneath changes. Existing TS callers should not have to update their code.

## Why CLI bridges first

The first implementation in any new language is almost always a CLI bridge. Two reasons:

The first is shipping cost. A CLI bridge is one wrapper class per language, plus a postinstall hook that downloads the right `ledgerdb` binary for the platform. The TypeScript SDK is roughly 500 lines of TypeScript plus a Node-side postinstall script; that surface ships in a week. A native binding requires either a CGO/JNI/FFI bridge into the Go core or a from-scratch reimplementation of the storage layer; both are months of work plus a per-language test matrix.

The second is correctness. The CLI binary is the canonical implementation. A bridge that shells out to the same binary the project tests in CI cannot drift in behaviour. A native reimplementation introduces a second code path that has to be kept in sync — every TxV3 change, every CAS retry-policy tweak, every schema-validation rule has to land in both places. Going through the CLI keeps "one source of truth" honest until the native binding is genuinely worth the duplication.

The cost is spawn latency. For a workload doing a few writes per request, the per-call floor of 10-30ms is invisible. For a workload doing thousands of small writes per second, it dominates. The right pattern in that case is either to move to the in-process Go SDK (if the host is Go or can host a Go sidecar), or to keep a `ledgerdb repl` child process resident and pipe commands to it (see [REPL And Query Explain](SDK-REPL-And-Query-Explain) for the resident-CLI pattern), or to batch on the caller side. None of these are SDK-shaped APIs today; the resident-REPL bridge is a likely future addition to several SDKs at once.

## Tier definitions

The SDK epic distinguishes Tier 1 and Tier 2 bindings:

- **Tier 1** ships with each LedgerDB release, has CI coverage on every supported platform, and is treated as a public surface for stability purposes. Today: Go and TypeScript.
- **Tier 2** is community-maintained or experimental, may lag a release behind the core, and does not block a LedgerDB release if it has not been updated. The planned Rust crate starts as Tier 2.

The tier of a binding can change as it matures; the Python and Java SDKs are expected to start at Tier 2 and graduate to Tier 1 as adoption justifies it.

## See also

- [SDK Overview](SDK-Overview) — frames the three surfaces (Go in-process, TS bridge, raw CLI) and when each fits.
- [Go SDK](SDK-Go-SDK) — the canonical SDK every other binding mirrors.
- [TypeScript SDK](SDK-TypeScript-SDK) — the operational pattern for the CLI-bridge approach.
- [CLI Reference](SDK-CLI-Reference) — the JSON contract every CLI-bridge SDK speaks.
- [REPL And Query Explain](SDK-REPL-And-Query-Explain) — the resident-CLI pattern that mitigates spawn-cost.
- [v1.0 Contract](Stability-V1) — what is frozen in the public SDK surfaces.
- Epic [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59) — the umbrella tracker for SDK expansion.
