# Documents and Collections

A LedgerDB repository organises data into collections. A collection has a name, a JSON Schema, an optional set of declared secondary indexes, and a set of documents identified by application-chosen string IDs. None of these concepts produce a new abstraction at the storage layer — they are conventions enforced by the application services in `internal/app/collection/` and `internal/app/doc/`, then projected onto the file tree under `documents/` and `state/`. The collection itself is, on disk, two files under `collections/<name>/`: `schema.json` and (optionally) `indexes.json`. Documents are blobs under deeper paths. There is no "collection table" because there is no database engine to hold it.

## What this page covers

This page walks the data model from the outside in: what a collection looks like, what `collection apply` does, what `doc put` does, and how the schema and index declarations feed the downstream pipelines. It does not cover the wire format of a transaction — that is [Transactions and TxV3](Concepts-Transactions-And-TxV3) — nor the directory layout — that is [Storage Layout](Concepts-Storage-Layout) — nor the SQLite materialisation — that is [Indexing](Concepts-Indexing).

## Collections

A collection is created or updated by `ledgerdb collection apply <name> --schema <path> [--indexes <list>]`. The CLI parses the flags and calls `collection.Service.Apply` (`internal/app/collection/service.go:29-75`). The service validates the collection name against `domain.IsValidCollectionName` (which rejects `/`, `\`, and `..` in the name; `internal/domain/names.go:5-13`), reads the schema file via the `SchemaSource`, runs it through the configured `SchemaValidator` (`internal/infra/schema/validator.go` compiles it with `santhosh-tekuri/jsonschema/v5` to catch malformed schemas), normalises the index specs, then writes the two files to disk under `collections/<name>/`:

```
collections/
  <name>/
    schema.json    # the raw JSON Schema bytes
    indexes.json   # optional; declared index specs
```

These files are not under `documents/` or `state/`. They live directly under the repo root as ordinary working-tree files (the bare repo uses an absent worktree, but the manifest, schema, and index files are written as flat-file content alongside the `objects/` and `refs/` directories — see `internal/infra/gitrepo/collection.go:17-49`). The current implementation writes them outside the git object database, which means schema changes are not themselves recorded as commits. This is a deliberate simplification; schema evolution is operator-driven and tracked out of band.

The schema is a standard JSON Schema document. LedgerDB does not currently re-run the schema against incoming `doc put` payloads — the schema is recorded for clients and downstream consumers to interpret, but the put path does not validate (search `internal/app/doc/service.go` and the patch service for `validator` references; none exist). The cost of this trade-off is that a malformed document can be written if the producer is not careful; the benefit is that the write path stays simple and the schema can be amended without forcing a re-validation of historical data. Tighter enforcement, if needed, is currently a consumer concern.

The index specs in `indexes.json` are a list of `domain.IndexSpec` (`internal/domain/index.go`):

```go
type IndexSpec struct {
    Name   string   `json:"name"`
    Fields []string `json:"fields"`
    Unique bool     `json:"unique,omitempty"`
}
```

Single-column indexes can be declared as bare JSON strings (`"email"`); composite or unique indexes need the object form (`{"name": "user_email", "fields": ["org_id", "email"], "unique": true}`). `collection.DecodeIndexesJSON` and `ParseIndexSpecs` (`internal/app/collection/indexes.go`) handle both. The specs are normalised — fields are trimmed, default names are derived from the field list, duplicates by name are dropped, the list is sorted — by `NormalizeIndexSpecs` so the on-disk file is canonical. These specs are consumed by the SQLite indexer (`internal/infra/sqliteindex/store.go:239-291`), which materialises them as `CREATE INDEX` statements with `json_extract(payload, '$.field')` expressions. See [Indexing](Concepts-Indexing) for the full pipeline.

## Documents

A document is identified by `(collection, doc_id)`. The doc ID is a string chosen by the application — there is no server-generated key. The pairing is hashed via `domain.HDSHash` (`internal/domain/hds.go:15-19`):

```go
func HDSHash(collection, key string) string {
    payload := collection + "/" + key
    sum := sha256.Sum256([]byte(payload))
    return hex.EncodeToString(sum[:])
}
```

The hex digest becomes the document's identity on disk. The actual stream path depends on the repository's stream layout (declared in the manifest at init time and stored under `db.yaml`):

- **flat**: `documents/<collection>/DOC_<hash>/`
- **sharded** (the default): `documents/<collection>/<hash[0:2]>/<hash[2:4]>/DOC_<hash>/`

The path is computed by `domain.StreamPath` (`internal/domain/hds.go:21-30`); the corresponding state-tree path is computed by `domain.StatePath`. Both paths exist inside the same commit; reads can either walk the history or hit the state copy directly. [Storage Layout](Concepts-Storage-Layout) covers the layout decision and its consequences.

Under each `DOC_<hash>/` directory live two files:

```
DOC_<hash>/
  HEAD            # one-line text: "tx/<timestamp>_<op>.txpb"
  tx/             # directory of tx blobs
    <ts>_put.txpb
    <ts>_patch.txpb
    ...
```

`HEAD` points to the newest tx file by relative path. The newest tx file's parent-hash field points to the previous one, and so on back to the first put (which has `parent_hash == ""`). That chain is the document's history. The set of files under `tx/` is the same set as the chain when history mode is `append`; when mode is `amend`, the directory holds a single `current.txpb` file and the chain has length one. See [History Modes](Concepts-History-Modes).

## The write path: trace a `doc put`

When a user runs `ledgerdb doc put users alice --payload '{"name":"Alice"}'`, the chain is:

1. The CLI handler in `internal/cli/commands.go:163-202` reads the payload, constructs a `gitrepo.Store` with the configured history mode, and builds a `doc.PutService` wired with: `canonicaljson.Canonicalizer`, `txv3.Encoder`, `hash.SHA256`, `platform.RealClock`, an `ident.ULIDGenerator`, the manifest's stream layout, and the manifest's history mode.
2. `PutService.Put` (`internal/app/doc/service.go:40-135`) validates the collection name and doc ID, normalises the repo path, computes the stream path via `domain.StreamPath(layout, collection, docID)`, and (in `append` mode) loads the current `HEAD` of that stream as the parent hash.
3. The payload is canonicalised — `canonicaljson.Canonicalizer.Canonicalize` (`internal/infra/canonicaljson/canonicalizer.go`) uses `jsontext.Value.Canonicalize` to produce RFC 8785-style sorted-keys output. This is what makes two semantically identical payloads produce identical hashes regardless of input formatting. [Transactions and TxV3](Concepts-Transactions-And-TxV3) explains why this matters.
4. A new ULID is generated for the `TxID`. The service builds a `domain.Transaction` (`internal/domain/tx.go:26-36`) with `Op = TxOpPut`, the canonical payload as `Snapshot`, and the loaded `ParentHash`.
5. The encoder marshals the transaction to deterministic protobuf bytes via `proto.MarshalOptions{Deterministic: true}.Marshal(pb)` (`internal/infra/txv3/codec.go:22-33`). The hasher computes `txHash = sha256_hex(encoded)`.
6. A parallel `stateTx` is built with `ParentHash` cleared — it is the standalone snapshot for the state tree. If the document has any history, the two encodings differ in just that one field; if it is the first write, they are identical and the same bytes are reused.
7. The whole bundle is handed to `gitrepo.Store.PutTx` (`internal/infra/gitrepo/tx_store.go:95-210`). The store opens the bare repo, writes the tx blob into the git object database (`writeBlob`), writes the `HEAD` blob (`writeBlob` on a single-line string), and the state-tree blobs if applicable. It then enters the CAS loop: load the current `refs/heads/main`, walk the tree, verify the head hash still matches the expected parent, compose the new tree by inserting/replacing the tx file and the `HEAD` pointer in both `documents/...` and `state/...`, write the new commit, and attempt `CheckAndSetReference`. If the ref moved under us, retry up to five times with jittered exponential backoff (`casBackoffBase = 25ms`, full jitter). On success, the result returns the new commit hash, the tx hash, and the tx ID. [Conflict Resolution](Concepts-Conflict-Resolution) covers the CAS loop in detail.

The CLI prints these values and, if `--sync` is on (the default; controlled by `LEDGERDB_AUTO_SYNC`), pushes the new commit to `origin` via `runWithAutoSync` (`internal/cli/commands.go:1267-1294`).

The patch path (`internal/app/doc/patch_service.go`) follows the same skeleton but with two differences: it loads the current document either from the state tree (`LoadHeadTx` on the `state/` path) or by rehydrating from the history chain via `buildTxIndex` + `buildTxChain` + `rehydrateChain` (`internal/app/doc/chain.go`), applies the JSON Patch, and writes both a history tx (`Op = TxOpPatch` carrying the patch ops) and a state tx (`Op = TxOpMerge` carrying the resulting snapshot). The split lets a `doc get` against the state tree return in one blob read while the history tree still holds the minimal-diff form. The delete path follows the same pattern with `Op = TxOpDelete` and no payload.

## The manifest

Every repository has a `db.yaml` at its root with the manifest fields (`internal/domain/manifest.go`):

```go
type Manifest struct {
    Version           int
    Name              string
    CreatedAt         time.Time
    StreamLayout      StreamLayout    // "flat" or "sharded"
    HistoryMode       HistoryMode     // "append" or "amend"
    AppliedMigrations []string
}
```

It is written by `repo.InitService.Init` (`internal/app/repo/init_service.go`) when `ledgerdb init` runs, and is loaded on every CLI invocation by the persistent pre-run in `internal/cli/root.go:41-60`. The layout and history mode propagate from the manifest into every subsequent service constructor so that, for example, a put against a repository initialised with `--layout sharded` always uses the sharded path resolver. The SDK does the same resolution in `pkg/ledgerdbsdk/client.go:138-182` and rejects any explicit `Config.StreamLayout` that disagrees with the on-disk manifest. The manifest is read on every command rather than cached, so an operator who deletes `db.yaml` will see the CLI fall back to a `Version: 1` zero-value with default flat layout — a footgun the [Storage Layout](Concepts-Storage-Layout) page calls out.

## See also

- [Storage Layout](Concepts-Storage-Layout) for the on-disk directory tree
- [Transactions and TxV3](Concepts-Transactions-And-TxV3) for the protobuf encoding
- [History Modes](Concepts-History-Modes) for append vs amend
- [Indexing](Concepts-Indexing) for what `indexes.json` becomes in SQLite
- [Architecture Overview](Concepts-Architecture-Overview) for the call stack from CLI to git
