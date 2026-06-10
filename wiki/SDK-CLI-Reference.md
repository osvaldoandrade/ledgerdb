# SDK CLI Reference

This page is the comprehensive reference for the `ledgerdb` command-line interface. Every command is implemented in `internal/cli/`, wired into the root command in [`internal/cli/root.go`](../tree/main/internal/cli/root.go), and dispatched through `internal/cli/execute.go`. The cobra command tree is constructed in `newRootCmd`; the bulk of the command bodies live in `internal/cli/commands.go`, with newer or larger commands split into `cmd_*.go` files. The page below groups commands the way the root constructor groups them, describes each one in the same shape (synopsis, what it does, key flags, tradeoffs), and points at the implementing file. The CLI is the canonical surface for LedgerDB; the Go and TypeScript SDKs described on the sibling pages compose the same commands.

## Global flags and environment

Persistent flags are declared on the root command in `internal/cli/root.go` and apply to every subcommand. They are loaded from environment variables first, then overridden by explicit flags.

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--repo` | — | `.` | Path to the repository directory; every command resolves files relative to it. |
| `--json` | — | `false` | Emit structured JSON on stdout instead of the human-readable renderer. Disables ANSI colour. |
| `--log-level` | `LEDGERDB_LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`; configured by `platform.ConfigureLogger` in `PersistentPreRunE`. |
| `--log-format` | `LEDGERDB_LOG_FORMAT` | `text` | `text` or `json`; chooses the slog handler. |
| `--sign` | `LEDGERDB_GIT_SIGN` | `false` | Sign every git commit produced by write commands. Requires a configured gpg or ssh signer. |
| `--sign-key` | `LEDGERDB_GIT_SIGN_KEY` | `""` | Key id passed to git for signing. Setting it implicitly enables `--sign`. |
| `--sync` | `LEDGERDB_AUTO_SYNC` | `true` | Run `git fetch` before each write and `git push` after; the auto-sync behaviour is implemented by `runWithAutoSync` in `commands.go`. |

The `--json` flag also suppresses the spinner (`spinnerEnabled` in `internal/cli/ui.go`) and the colour renderer. Errors are normalised through `NormalizeError` in `internal/cli/errors.go` and mapped to exit codes: `1` internal, `2` validation, `3` not found, `4` conflict.

The `PersistentPreRunE` hook in `newRootCmd` reads `db.yaml` from `--repo` and resolves `StreamLayout` and `HistoryMode` from the manifest so that subcommands inherit the correct mode without having to be told. `init`, `clone`, and `restore` skip that read because they run before a manifest exists.

## Repository commands

These three commands create, copy, and inspect the repository itself. Their implementations are in `internal/cli/commands.go` (`newInitCmd`, `newCloneCmd`, `newStatusCmd`, `newPushCmd`).

### `ledgerdb init`

```
ledgerdb init [--name <db>] [--layout flat|sharded] [--history-mode append|amend] [--remote <url>]
```

Initialises a new ledger at `--repo`. Calls `repoapp.NewInitService(...).Init(...)` which writes `db.yaml`, creates the initial commit, and (if `--remote` is set) configures `origin`. The `--layout` choice fixes how transaction streams are arranged on disk — `flat` keeps one ref per collection, `sharded` splits by document hash and is the better default for collections that grow past a few thousand documents. `--history-mode` chooses between `append` (every transaction is a new commit; history is immutable) and `amend` (writes can rewrite the tip; needed for index `--mode state` to allow resets). The choice is permanent for the repo.

### `ledgerdb clone <url> [path]`

Wraps `git clone` through `repoapp.NewCloneService`. The remote must point at a LedgerDB repository (i.e. one with a `db.yaml` at the root). The post-clone state is identical to a freshly initialised repo of the same layout and history mode.

### `ledgerdb status`

Prints the resolved repo path, the current HEAD hash, and the manifest summary (`Name`, `Version`, `StreamLayout`, `HistoryMode`, `CreatedAt`). The JSON form, defined by `statusOutput` in `commands.go`, is stable and is the form used by tooling.

### `ledgerdb push`

Runs `git push` against `origin`. Useful when `--sync=false` has been used to defer pushes; the implementation simply calls `autoPush` from `commands.go`.

## Collection and schema commands

A collection is a typed bucket of documents. Its schema is a JSON Schema file and its index list is a comma-separated set of field names or a JSON array of composite specs.

### `ledgerdb collection apply <name> --schema <path> [--indexes <spec>]`

Implemented by `newCollectionApplyCmd` in `commands.go`. Reads the schema from the local filesystem (`filesystem.SchemaSource{}`), validates it with `schema.JSONSchemaValidator{}`, parses `--indexes` with `collectionapp.ParseIndexSpecs` — which accepts either a comma list (`"status,assignee"`) or a JSON array of `{name,fields[],unique?}` objects for composite indexes (the format added in commit `cdc4a85`) — and commits the result. Auto-sync wraps the call: fetch before, push after, unless `--sync=false`.

A `collection` group also exists as a parent command (`newCollectionCmd`), but it currently has exactly one subcommand. There is no `collection list` in the binary today.

### `ledgerdb schema scaffold <name> [--output <path>] [--minimal] [--force]`

Implemented in `internal/cli/cmd_schema_scaffold.go`. Writes a starter JSON Schema for a collection, either to `schemas/<name>.json` (the default), to a chosen path, or to stdout (`--output -`). The `--minimal` form emits `{ type: "object" }`; the default form includes `id`, `created_at`, `updated_at` with their RFC and pattern constraints. The collection name is validated against `domain.IsValidCollectionName`. Returns `ErrScaffoldFileExists` if the target already exists and `--force` was not supplied.

## Document commands

The `doc` subtree is the per-document CRUD surface. Every write goes through `runWithAutoSync`; every read goes straight to the git store. Constructors are in `commands.go` (`newDocPutCmd`, `newDocGetCmd`, `newDocPatchCmd`, `newDocDeleteCmd`, `newDocRevertCmd`, `newDocLogCmd`).

### `ledgerdb doc put <collection> <doc_id> --payload <json> | --file <path>`

Writes a full snapshot of the document. The payload is either an inline JSON string or a path read with `readJSONInput`. Internally builds a `docapp.NewPutService` with the canonicaliser, `txv3` encoder, SHA-256 hasher, ULID generator, and the layout and history mode loaded from the manifest. On success it emits `{ commit, tx_hash, tx_id }` (the `putOutput` struct).

### `ledgerdb doc get <collection> <doc_id>`

Reads the current state of the document by walking the per-doc stream and replaying snapshots and patches. Emits raw JSON on stdout by default; with `--json` it emits a `getOutput` wrapper that also includes `tx_hash`, `tx_id`, and `op`.

### `ledgerdb doc patch <collection> <doc_id> --ops <json> | --file <path>`

Applies a JSON Patch (RFC 6902). The patcher (`jsonpatch.Patcher{}`) is applied against the current snapshot inside `docapp.NewPatchService`. The resulting commit references the previous `tx_hash` so the chain is verifiable.

### `ledgerdb doc delete <collection> <doc_id>`

Writes a tombstone. The document is not removed from history; the latest tx is a `delete` op and `doc get` returns `ErrDocDeleted` for it.

### `ledgerdb doc revert <collection> <doc_id> --tx-id <ulid> | --tx-hash <sha>`

Rewinds the document to a previous transaction by writing a new snapshot whose contents match the target. The original history is preserved; this is a forward-only operation regardless of history mode.

### `ledgerdb doc log <collection> <doc_id> [--limit N] [--cursor <token>]`

Lists the per-document transaction history. Cursor-paginated since commit `ec88eb6`; `--cursor` is an opaque base64 token returned as `next_cursor` on the previous page. The default page size is `docapp.DefaultLogPageLimit`.

## Index commands

The SQLite sidecar is built and refreshed by the `index` subtree. The index is rebuildable from the ledger and is therefore not part of the durable state of the database.

### `ledgerdb index sync --db <path> [--fetch] [--batch-commits N] [--fast] [--mode history|state]`

Implemented by `newIndexSyncCmd` in `commands.go`. Opens the SQLite database (via `sqliteindex.OpenWithOptions` with `Fast: <flag>` to relax WAL durability), constructs `indexapp.NewSyncService`, and runs a single sync. `--mode state` materialises the current state per document; `--mode history` materialises the full transaction log. `--batch-commits` controls how many git commits are grouped into a single SQLite transaction (>=1). The `--fast` flag is the typical choice when rebuilding from scratch and `--mode state` is the typical choice when the SQLite file is meant to be a queryable mirror of the current state.

### `ledgerdb index watch --db <path> [--interval 5s] [--jitter 0] [--only-changes] [--once] [--quiet] [--metrics-addr ...] [--audit-log ...]`

The single long-lived command in the system. Polls in a loop, calling `service.Sync` on every iteration and applying `--jitter` to the interval to avoid thundering-herd polls when several watchers run against the same upstream. `--once` runs a single sync and exits; combined with cron, it is the right way to drive periodic indexing without leaving a process resident. `--only-changes` suppresses the per-iteration summary when no commits applied. The opt-in observability flags added in commit `7179d14` are `--metrics-addr` (binds a Prometheus `/metrics` endpoint; defaults to loopback unless `--metrics-allow-public` is set) and `--audit-log` (JSON Lines file, `-` for stdout, with `--audit-flush-interval` controlling buffer cadence). The metrics and audit observers are wired through `indexapp.NewCombinedObserver` and avoid the typed-nil-interface footgun documented in the source.

## Integrity and inspection commands

### `ledgerdb integrity verify [--deep]`

Constructs `integrityapp.NewVerifyService` and walks every per-doc stream verifying parent hashes match. `--deep` additionally rebuilds documents by replaying patches, which catches latent JSON-Patch failures that a hash-only walk would miss. Returns exit code `4` (`ExitConflict`) on integrity failure.

### `ledgerdb inspect blob <hash>`

Decodes a transaction blob by its git object hash. Useful when staring at a git log entry and needing to know which document and op it corresponds to. Emits the decoded `txv3` record either pretty-printed or as JSON.

## Maintenance commands

### `ledgerdb maintenance gc [--prune <when>]`

Runs `git gc --prune=<when>` against the repository. The default `--prune=now` is aggressive and the right choice immediately after a `truncate`; pass `--prune=2.weeks.ago` to keep dangling objects for a recovery window.

### `ledgerdb maintenance snapshot [--threshold 50] [--max N] [--dry-run]`

Walks per-doc streams whose patch chain length exceeds `--threshold` and writes a new full snapshot transaction at the tip. This bounds the cost of `doc get` and of index rebuilds. `--max` caps the number of snapshots written in one pass; `--dry-run` reports candidates without writing.

## Disaster-recovery commands

The DR triad is implemented in `cmd_backup.go`, `cmd_restore.go`, and `cmd_truncate.go`. The shapes here were added in commit `8842571`.

### `ledgerdb backup [--output <path>] [--include-sidecar] [--sidecar-path <path>]`

Creates a sealed `tar.gz` of the repository through `drapp.NewBackupService`. The default output path is `ledgerdb-backup-<utc>.tar.gz`. The SQLite sidecar is excluded by default because it is derivable; `--include-sidecar` embeds it for restore-time speed.

### `ledgerdb restore --input <path> [--target <dir>] [--skip-verify]`

Extracts a backup tarball into `--target` (or `./restored-<utc>`) and, by default, runs `integrity verify` against the result. `--skip-verify` is not recommended; the warning is printed unconditionally when set.

### `ledgerdb truncate --before <ts|ulid|tx_id> [--collection <name>] [--dry-run] [--yes]`

Destructively rewrites history so that transactions older than the threshold are dropped from per-doc streams, keeping the latest snapshot per document. `--before` accepts an ISO-8601 timestamp, unix nanoseconds, or a `tx_id`. `--dry-run` produces the plan only; running without `--dry-run` requires `--yes`. The implementation creates a backup branch before rewriting (see `truncateExecutionOutput`).

## Migration commands

### `ledgerdb migrate apply [--target N] [--dry-run]` and `ledgerdb migrate status`

Implemented in `cmd_migrate.go`. Migrations are a directory of ordered `collection apply` operations that the service replays. `--target` stops at a specific migration number; the default applies all pending. The `manifestStoreAdapter` bridges between `gitrepo`'s package-level helpers and the migrate service's port.

## Diagnostic and reporting commands

### `ledgerdb stats [--collection <name>]`

Implemented in `cmd_stats.go`. Reports repo size, git object counts, per-collection document counts, average and maximum patch-chain depth (sampled across `statsSampleStreams = 256` streams), and replication lag against `origin/main` if a remote is configured. The collection-level depth metrics are the right signal for deciding when to run `maintenance snapshot`.

### `ledgerdb diff <refA> <refB> [--collection <name>] [--limit 50]`

Implemented in `cmd_diff.go`. Compares per-collection document state between two refs (commit hashes, branch names, or tags) and prints added/changed/removed sets. `--limit` caps the per-category, per-collection list; counts are always full.

## Query and REPL

### `ledgerdb query explain <sql> [--db <path>]`

Implemented in `cmd_query_explain.go`. Opens the SQLite sidecar read-only and runs `EXPLAIN QUERY PLAN <sql>`, returning the plan as a nested tree (driven by parent links in the explain rows). The default `--db` is `<repo>/index.db`. See [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain) for a worked example of reading the output.

### `ledgerdb repl [--script <path>]`

Implemented in `cmd_repl.go`. An interactive command loop that dispatches each line back through `newRootCmd` with `--repo` and `--json` prepended where the user did not already supply them. Without `--script` it reads from stdin and prints `ledgerdb> ` (or `<repo>> `) as the prompt. With `--script` it reads from a file non-interactively and exits at EOF. Tokenisation supports double-quoted, single-quoted, and backslash-escaped tokens (`splitReplLine`). The full UX is described on [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain).

## Output and error model

Every command supports `--json`. The JSON shapes are stable Go structs defined alongside their commands — `putOutput`, `getOutput`, `logOutput`, `indexSyncOutput`, `statusOutput`, `backupOutput`, `restoreOutput`, `truncateOutput`, `statsOutput`, `diffOutput`, `migrateApplyOutput` — and tools should script against them rather than parsing the human-readable form.

Errors are normalised through `NormalizeError` in `internal/cli/errors.go`. Validation errors (`ErrCollectionRequired`, `ErrInvalidCollection`, `ErrInvalidCursor`, and the rest) become exit code `2`. Not-found errors (`ErrDocNotFound`, `ErrTxNotFound`, `ErrBlobNotFound`) become `3`. Conflict errors (`ErrHeadChanged`, `ErrSyncConflict`, `ErrIntegrityFailed`) become `4`. Anything else is `1`. The mapping is the single place to add new error classifications; if a new sentinel is not listed there it will end up as a generic internal error.

## See also

- [SDK Go SDK](SDK-Go-SDK) — the in-process equivalent of these commands.
- [SDK TypeScript SDK](SDK-TypeScript-SDK) — the Node bridge to these commands.
- [SDK REPL And Query Explain](SDK-REPL-And-Query-Explain) — interactive use of `ledgerdb repl` and `query explain`.
- [CLI Reference](SDK-CLI-Reference) — the older single-page summary; this page supersedes it.
- [Operations and CLI Strategy](SDK-CLI-Reference) — design rationale for the CLI-first surface.
