# Rust SDK

> **Status: Coming soon.** Tracked in
> [#62](https://github.com/osvaldoandrade/ledgerdb/issues/62) (part of the
> SDK epic [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)).

A Rust crate is planned as a Tier 2 SDK (see
[`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)). The crate will target
`git2-rs` (libgit2) plus a `rusqlite` sidecar, mirroring the Go SDK's
in-process model. The initial scaffolding may use a CLI bridge before moving
to a fully native implementation.

## Planned API shape

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

## In the meantime

You can integrate from Rust today by invoking the `ledgerdb` CLI through
`std::process::Command` with the `--json` flag — the JSON contract is stable
and is what the TypeScript SDK already relies on. See
[TypeScript SDK](./typescript.md) for the call pattern (it transfers
one-to-one to any language that can spawn a child process).

## Follow progress

- Tracking issue: [#62](https://github.com/osvaldoandrade/ledgerdb/issues/62)
- Epic: [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)
- Conformance spec: [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)
