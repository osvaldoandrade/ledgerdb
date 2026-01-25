# LedgerDB TypeScript SDK (CLI Bridge)

This package wraps the `ledgerdb` CLI to avoid FFI in Node/TypeScript.
It downloads a prebuilt `ledgerdb` binary on install.

## Install

```bash
npm config set @osvaldoandrade:registry https://npm.pkg.github.com

npm install @osvaldoandrade/ledgerdb
```

## Usage

```ts
import { LedgerDBClient } from "@osvaldoandrade/ledgerdb";

const client = new LedgerDBClient({
  repoPath: "/path/to/ledgerdb.git",
});

await client.put("tasks", "task_0001", {
  title: "Ship v1",
  status: "todo",
});

const doc = await client.get("tasks", "task_0001");
console.log(doc);

await client.indexSync();
client.startIndexWatch();
```

## Binary download

The postinstall script downloads a release asset named:

```
ledgerdb-<platform>-<arch>
```

Supported values:
- platform: `darwin`, `linux`, `windows`
- arch: `amd64`, `arm64`

### Overrides

- `LEDGERDB_BIN`: use a preinstalled binary and skip download.
- `LEDGERDB_SKIP_DOWNLOAD=1`: skip download.
- `LEDGERDB_RELEASE_TAG`: override the GitHub release tag (defaults to `v<version>`).
- `LEDGERDB_DOWNLOAD_BASE`: override the download base URL.
