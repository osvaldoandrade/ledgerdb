# Python SDK

> **Status: Coming soon.** Tracked in
> [#60](https://github.com/osvaldoandrade/ledgerdb/issues/60) (part of the
> SDK epic [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)).

A native Python SDK is planned. It will follow the same conformance contract
as the Go and TypeScript bindings (see
[`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)). Initial implementation will
likely use a CLI bridge (mirroring the TypeScript approach) before moving to
a native binding.

## Planned API shape

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

The sidecar SQLite database (`index.db`) will be exposed through a context
manager that yields a DB-API 2.0 connection, so Python users can run ad-hoc
SQL with their preferred tools (`sqlite3`, `sqlalchemy`, `pandas`).

```python
with c.index() as conn:
    rows = conn.execute(
        'SELECT doc_id FROM "collection_users" WHERE deleted = 0',
    ).fetchall()
```

## In the meantime

You can drive LedgerDB from Python today by shelling out to the `ledgerdb`
CLI with `--json`:

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

This is exactly the strategy the TypeScript SDK uses today — see
[TypeScript SDK](./typescript.md) for the operational pattern.

## Follow progress

- Tracking issue: [#60](https://github.com/osvaldoandrade/ledgerdb/issues/60)
- Epic: [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)
- Conformance spec: [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)
