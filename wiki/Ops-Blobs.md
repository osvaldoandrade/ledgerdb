# Operations: Binary Blobs

> **Status:** Design proposal. Tracked under Epic [#6](https://github.com/osvaldoandrade/ledgerdb/issues/6). The CLI verbs and the `$blob` sentinel described below are the planned shape; no implementation has shipped in v0.2.x.

LedgerDB stores small JSON documents well. It does not store multi-megabyte binary payloads well: stuffing a base64-encoded PDF into a JSON string explodes TxV3 blob size, defeats Git delta compression, and pushes parsing cost onto every reader. This page documents the planned design for first-class binary blob support: an out-of-band content-addressed store layered on `git-lfs`, surfaced through a `$blob` sentinel embedded in TxV3 documents.

The design intentionally piggybacks on the maturity of `git-lfs` rather than building a native object store. The trade-offs — operational dependency on the `git-lfs` binary, bandwidth amplification on clone and fetch, lack of fine-grained ACLs — are discussed at the end. A native blobstore backend remains an open question for v0.4+; the sentinel scheme is forward compatible with any future backend.

## Problem statement

Users in audit-heavy domains (legal evidence, scientific notebooks, regulated workflows) increasingly want to associate large binary artifacts with their documents:

- A contract document referencing the PDF that was signed.
- An incident record carrying packet captures or core dumps.
- A scientific record referencing raw instrument output.
- A user record carrying an avatar image.

Today the only options are:

1. **Inline base64**: stuff the binary into a JSON string. This explodes TxV3 blob size, defeats Git delta compression (because base64 of changed binary content has no useful delta), and pushes parsing cost onto every reader.
2. **Out-of-band storage**: keep the binary in S3/GCS and store a URL in the document. This breaks the LedgerDB integrity contract (the URL is mutable; the bucket is mutable; nothing in the TxV3 hash chain detects swap-in/swap-out attacks), and it splits the operational story across two systems.

The blob substrate needs to:

- Keep the TxV3 transaction blob small and Git-friendly.
- Preserve a content-addressed integrity guarantee.
- Distribute alongside the repository (clone/push/fetch story is unified).
- Avoid requiring a separate operational system in the simple case.

Non-goals: native S3/GCS backends (use `git-lfs` against those backends instead); CDN integration; transcoding or media processing; streaming append to a blob; encryption-at-rest (tracked separately under Epic [#73](https://github.com/osvaldoandrade/ledgerdb/issues/73)); inline base64 fallback.

## Storage model

### Three locations of a blob

For any single blob, three distinct on-disk artifacts exist:

1. **The content**, byte-identical to what the caller uploaded, living in the configured LFS storage backend (a local `.git/lfs/objects/` directory or a remote LFS server). This is what `ledgerdb blob get` ultimately streams back.
2. **The LFS pointer**, a small text file checked into the Git repository under a path determined by the LedgerDB blob layout. The pointer contains the OID and size; `git-lfs` replaces the pointer with the content on checkout when configured.
3. **The sentinel reference**, a JSON object of shape `{"$blob":"sha256:<hex>"}` embedded somewhere in a TxV3 document. This is the only mechanism by which a blob becomes "live" from LedgerDB's perspective.

The crucial invariant is that (1) and (3) share the SHA-256. The pointer file's OID is the same SHA-256 the sentinel carries. This permits two independent integrity paths:

- `git-lfs` verifies the pointer↔content pairing on download.
- LedgerDB verifies the sentinel↔content pairing on `blob get` by re-hashing the bytes it receives.

A blob is therefore content-addressed end to end: a malicious or buggy LFS server cannot serve different bytes under the same digest without detection.

### Why not embed the bytes in TxV3

The TxV3 protocol (see [TxV3 Format](IO-TxV3-Format)) treats the snapshot and patch payloads as the canonical document state. Embedding multi-megabyte binary payloads there would:

- Inflate every TxV3 blob, defeating loose-object packing and forcing Git to delta-compress over binary content (which usually fails to compress).
- Break canonicalization assumptions: RFC 8785 (JCS) canonicalization is defined over JSON. Mixing in raw bytes requires escaping that is verbose and error-prone.
- Couple replication of a small metadata change to the cost of moving every binary attached to that document.

By keeping the bytes in LFS and the reference in TxV3, the TxV3 blob remains small, deterministic, and JCS-canonicalizable. The blob is fetched only when the reader chooses to dereference it.

### On-disk layout

LedgerDB uses a content-addressed layout for the LFS pointer files, mirroring the sharding philosophy of `documents/` documented on [Storage Layout](Concepts-Storage-Layout):

```text
blobs/
├── sh/                          # First 2 hex chars of SHA-256
│   ├── a1/                      # Next 2 hex chars
│   │   └── sha256_a1b2c3.../    # The LFS pointer file (text)
│   └── ...
└── _meta/
    ├── sha256_a1b2c3.../meta.json   # size, created-at, content-type hint, refcount
    └── ...
```

The `.gitattributes` file at the repository root contains:

```text
blobs/**/sha256_* filter=lfs diff=lfs merge=lfs -text
```

This tells `git-lfs` to intercept every blob pointer file during checkout. The `_meta/` subtree is plain JSON checked into Git directly — it is small, benefits from Git delta compression, and is queryable without contacting the LFS server.

### The blob sentinel

A blob reference inside a document is a JSON object with exactly one key, `$blob`, whose value is a string of the form `sha256:<64 lowercase hex characters>`:

```json
{
  "name": "Q3 board minutes",
  "signed_pdf": { "$blob": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" },
  "exhibits": [
    { "$blob": "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae" },
    { "$blob": "sha256:fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9" }
  ]
}
```

The sentinel is:

- **Recursively recognized** at any depth — root, nested object, array element, deeply nested array of objects, etc.
- **Strict in shape**: an object with exactly one key, `$blob`, mapping to a string matching `^sha256:[0-9a-f]{64}$`. Any deviation (extra keys, wrong digest length, mixed case in hex, unknown algorithm prefix) is rejected at write time.
- **Reserved**: the key `$blob` is reserved at any position inside any LedgerDB document. Documents containing `$blob` keys that do not conform to the sentinel shape are rejected.

This single-key, prefix-tagged convention follows the same spirit as Postgres' `$$ ... $$` dollar-quoting and MongoDB's `$type` operators: a syntactically minimal, easy-to-detect marker.

### Why SHA-256

SHA-256 is already the canonical hash inside LedgerDB (TxV3 `parent_hash`, document sharding, Git's own SHA-256 mode). Using the same primitive for blob identity avoids a second cryptographic dependency, allows the LFS layer (which natively understands SHA-256 OIDs) to be used unchanged, and aligns with the broader content-addressed-storage convention.

The `sha256:` prefix exists for future-proofing (e.g., a `blake3:` migration), but parsers must reject any non-`sha256:` prefix today.

## CLI surface

The CLI verbs mirror the existing `ledgerdb doc` family and are grouped under `ledgerdb blob`. All commands take the usual repository-discovery flags.

### `ledgerdb blob put <file>`

Uploads a local file as a blob and prints its digest.

```text
$ ledgerdb blob put ./minutes_2026_q3.pdf
sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

Behaviour: streams the file through a SHA-256 hasher without loading it fully into memory. If the digest already exists, the command is a no-op (deduplication is free with content-addressed storage). If the digest is new, writes the LFS pointer under `blobs/<shard>/sha256_<hex>`, writes `_meta/sha256_<hex>/meta.json`, stages a Git commit recording the new pointer, and triggers `git lfs push` against the configured remote.

Flags: `--chunk-size <bytes>` (default 4 MiB for the resumable upload protocol), `--resume`, `--content-type <mime>`, `--no-push`.

### `ledgerdb blob get <sha256:hex> [-o out]`

Streams the blob content from LFS storage. Resolves the digest to the pointer file; if the LFS object is not present locally, performs a `git lfs fetch` for that specific OID; streams bytes to the destination while re-hashing; on EOF compares the computed SHA-256 to the requested one. Mismatch raises a fatal `BlobIntegrityError` and the partial output is removed if writing to a file.

Flags: `-o, --output <path>` (default stdout), `--no-verify` (debugging only), `--peek <bytes>` (read only the first N bytes; integrity check is skipped and stderr emits a clear warning).

### `ledgerdb blob rm <sha256:hex>`

Marks a blob as removable. Does not physically delete content. Refuses to mark a digest still referenced by any TxV3 document in the current state tree (append mode includes every revision; amend mode counts only the latest). Otherwise sets `removed_at` in `_meta/sha256_<hex>/meta.json` and commits.

Physical removal happens during `ledgerdb maintenance gc`. Rationale: an interactive `rm` that immediately deleted bytes would conflict with the principle that Git history is immutable. Mark, then sweep.

Flags: `--force` (bypass live-reference check; still refuses if referenced by the latest snapshot, to prevent foot-shooting in amend mode).

### `ledgerdb blob stat <sha256:hex>`

Prints metadata.

```text
$ ledgerdb blob stat sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
digest:        sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
size:          1843267
content_type:  application/pdf
created_at:    2026-05-08T14:21:09Z
refcount:      3
referenced_by:
  - contracts/2026-board-pkg @ tx ULID 01HZK4...
  - contracts/2026-board-pkg @ tx ULID 01HZN8...
  - audit/board-attachments  @ tx ULID 01HZN9...
removed_at:    (live)
```

`refcount` is computed by scanning the current state tree (or every revision under append mode). `referenced_by` lists at most the first 20 references; `--all-refs` dumps them all.

### `ledgerdb blob ls`

Lists all blobs known to the repository, optionally filtered by liveness, size range, or content-type hint. Reads only `_meta/` and the in-repo state tree; never contacts the LFS server.

## Embedding in documents

### Write path

When `ledgerdb doc put` or `ledgerdb doc patch` (or their SDK equivalents) receives a document, the engine performs a recursive scan for `$blob` sentinels. For every sentinel encountered:

1. Validate the shape (single key, value matches `sha256:[0-9a-f]{64}$`).
2. Verify the blob is known to the repository: a pointer exists under `blobs/<shard>/sha256_<hex>` and the corresponding `_meta/sha256_<hex>/meta.json` is present and not marked `removed_at`.
3. If quotas are configured on the collection, accumulate the referenced blob's size into the per-document and per-collection budget and fail fast if either is exceeded.
4. Increment the conceptual refcount for the blob (the actual refcount is recomputed lazily by `stat` and `gc`; this step is logical).

If any sentinel fails validation, the entire write is rejected with `BlobReferenceError` naming the offending JSON path. **Writes are atomic with respect to blob references**: a `put` either succeeds with all referenced blobs verified or fails without committing.

Importantly, the engine does **not** download blob content during validation — it only checks for the pointer's existence. This keeps writes fast and avoids surprising bandwidth costs on `doc put`.

### Patch semantics

JSON Patch (RFC 6902) operations may add, remove, or replace blob sentinels exactly like any other JSON value. The validation above runs against the post-patch document state, not against the patch ops themselves. A patch may move references around freely, but the final state must reference only existing blobs.

### Read path

`ledgerdb doc get` returns the document with blob sentinels as-is. The engine does not auto-dereference. Callers who want the bytes invoke `ledgerdb blob get` (or the SDK equivalent) with the digest. Rationale:

- Predictable I/O: a `doc get` should not surprise the caller with a 2 GB download.
- Lazy fetching: many use cases inspect document metadata without ever touching the blob payload.
- Separation of concerns: blob streaming has different error and retry semantics than document fetching.

SDKs may offer a convenience method `getWithBlobs()` or `materialize()` that walks the document and pre-fetches all referenced blobs, but this is opt-in.

### Canonicalization

The blob sentinel is, syntactically, a normal JSON object. RFC 8785 (JCS) canonicalization rules apply without modification: key `$blob` is lexicographically the only key (ordering is moot); the digest string contains only `0-9a-f` plus the literal `sha256:` prefix (safely UTF-8); no special escaping beyond standard JCS rules.

Therefore the `parent_hash` Merkle chain in TxV3 covers the sentinel byte-for-byte. A blob substitution attack (swap the LFS bytes under the same OID, then serve a tampered document) is detected on `blob get` integrity recheck even though the TxV3 chain alone cannot see it: the TxV3 hash chain attests to the sentinel, not to the bytes, but the sentinel cryptographically commits to the bytes.

## Chunked upload with resume

Large blobs (multi-GB binaries, video files, scientific datasets) cannot rely on single-shot HTTP PUT. LedgerDB layers a simple chunked-upload protocol on top of `git-lfs`'s batch API.

### Protocol sketch

1. `blob put <file>`:
   - Stream the file, computing SHA-256.
   - Split into chunks of `--chunk-size` bytes (default 4 MiB).
   - For each chunk: PUT to the LFS server's chunk endpoint with the chunk index and the running digest.
   - On chunk success: append `{index, sha256, size, uploaded_at}` to `.ledgerdb/blob-uploads/sha256_<hex>.json`.
   - On final chunk: send a `commit` request; the LFS server stitches chunks, recomputes the SHA-256, and confirms.
2. If the upload fails (network, signal, server 5xx) the partial state is left in `.ledgerdb/blob-uploads/sha256_<hex>.json`.
3. `blob put --resume <file>`:
   - Recompute the SHA-256 of the full file; refuse if it differs from the digest stored in the partial state file (the file was modified mid-upload).
   - Read the partial state; skip already-uploaded chunks; resume from the next index.

The partial state file lives in `.ledgerdb/blob-uploads/` (untracked by Git; users should add this directory to `.gitignore`, and `ledgerdb` will ensure that the gitignore entry exists on first use). `chunk_size` is fixed for the lifetime of a single upload; changing `--chunk-size` mid-upload aborts and restarts.

Successful uploads remove the partial state file. Stale partials are reaped by `ledgerdb maintenance gc` after a configurable TTL (default 7 days) and on demand by `ledgerdb blob cleanup-uploads`.

The LFS server must support either the `git-lfs` Locks API plus a custom chunked-upload extension, or multipart-resumable upload semantics (S3-style multipart) exposed through an LFS-compatible gateway. For LFS servers that lack chunked-upload support, LedgerDB falls back to single-shot upload with no resume; `--resume` becomes a no-op and emits a warning.

## Garbage collection

### Defining live and orphan

A blob digest `D` is **live** if there exists at least one TxV3 transaction whose post-canonicalization snapshot contains a sentinel `{"$blob":"sha256:D"}` at any JSON path. Otherwise `D` is **orphan**.

The exact set of TxV3 transactions considered depends on the manifest's history mode (see [History Modes](Concepts-History-Modes) and `internal/domain/manifest.go`):

- **append mode** (default): every transaction in every `tx/` stream counts. A blob referenced by an old PUT but no longer by the current HEAD is still live.
- **amend mode**: only the current state matters. A blob no longer referenced by `tx/current.txpb` of any document is eligible for collection immediately.

### The GC pass

The existing `internal/app/maintenance/` package gains a blob-aware sub-pass. `ledgerdb maintenance gc` performs:

1. **Mark phase**: walk every TxV3 transaction (filtered by history mode), JCS-canonicalize the snapshot, recursively extract `$blob` sentinels, build the live set `L`.
2. **List phase**: enumerate `blobs/` to obtain the candidate set `C`.
3. **Filter phase**: `O = C \ L`. Drop from `O` any blob whose `_meta/.../meta.json` does not carry a `removed_at` timestamp older than the configured grace period (default 24 hours). The grace period prevents racing GC against in-flight writes.
4. **Sweep phase**: for each digest in `O`, delete the LFS pointer file, delete `_meta/sha256_<hex>/`, and call `git lfs prune` for the OID.
5. **Commit phase**: a single Git commit records the removed pointer files (commit message: `gc: prune N orphan blobs`).
6. **Push phase** (optional, gated by `--push`): push the GC commit and force the LFS server to drop the deleted OIDs.

### Safety properties

- GC is **single-writer**: it acquires the existing maintenance lock. Concurrent `blob put` will block briefly during the sweep phase.
- GC is **idempotent**: rerunning after a successful run is a no-op.
- GC is **interruptible**: the sweep phase processes one blob at a time and records progress to `.ledgerdb/gc-progress.json` so an interrupted GC can be resumed.
- GC **never removes a live blob**: the mark phase is exhaustive within the configured history mode. The grace period covers the window between sentinel write and commit visibility.

Dry run is supported via `ledgerdb maintenance gc --dry-run --blobs-only`, which lists the orphans without committing or pushing.

## Quotas

Per-collection quotas are configured via `ledgerdb collection apply`:

```text
$ ledgerdb collection apply --name contracts \
    --max-blob-size  10MB  \
    --max-blob-total 1GB
```

- `--max-blob-size` is the upper bound for any single blob referenced by a document in this collection.
- `--max-blob-total` is the cumulative size of all live blobs referenced by all documents in this collection. The cumulative size is recomputed lazily during write and cached in the manifest.

Enforcement happens in `internal/app/doc/` at write time. Quotas are advisory: existing data that predates a quota tightening is not retroactively rejected, but new writes that would push the collection further over the limit are denied. An explicit `--rebalance` flag may force a recompute of cumulative size if the cached value drifts (e.g., after manual repo surgery).

If neither flag is set, the collection has no blob quotas. This is deliberately the default: surprise-rejecting writes on a freshly-upgraded repo would be a worse experience than relying on operators to apply quotas explicitly.

## Security implications

### Integrity (recap)

Blob content is content-addressed by SHA-256. The TxV3 hash chain (`parent_hash`) attests to the canonical bytes of every transaction blob, and those bytes include the `$blob` sentinel verbatim. Therefore:

- Tampering with the TxV3 chain is detected by the standard `parent_hash` walk (see [Integrity and Verification](Concepts-Integrity-And-Verification)).
- Swapping the bytes in LFS storage without updating the OID is detected by SHA-256 mismatch on `blob get`.
- Swapping the bytes *and* the OID requires forging a SHA-256 collision (computationally infeasible) AND modifying every TxV3 transaction that references the digest (which breaks the `parent_hash` chain).

This gives transitive integrity without expanding the TxV3 schema.

### What is not covered

- **Confidentiality**: LFS content is, by default, stored in cleartext on whichever backend the LFS server is configured against. Encryption-at-rest is tracked under Epic [#73](https://github.com/osvaldoandrade/ledgerdb/issues/73); the LFS server may use server-side encryption (e.g., S3 SSE-KMS) but client-visible cleartext is still observable by anyone with credentials to the server.
- **Authorization**: LedgerDB itself does not enforce who may `blob get`. Access control sits at the LFS server (HTTP auth, mTLS, signed URLs). Repositories that need fine-grained ACLs on blobs must front the LFS server with an authentication proxy.
- **Audit log of `blob get`**: dereferencing a blob is a read against the LFS server; it does not generate a TxV3 transaction. Operators who need read auditing must configure their LFS server's access logs.

### Encryption-at-rest (forward reference)

When Epic [#73](https://github.com/osvaldoandrade/ledgerdb/issues/73) lands, the proposed model is per-collection (or per-repo) data encryption keys (DEKs) wrapped by a KMS-managed KEK, client-side envelope encryption of the blob content before upload, and the `$blob` sentinel optionally extending to `{"$blob":"sha256:<hex>","$wrap":"<key-id>"}` to record the wrapping key. This is forward compatible with the present design: today's sentinels remain valid (no `$wrap` means cleartext); tomorrow's sentinels carry the wrap key id.

### Denial of service via quota exhaustion

A malicious or buggy client could attempt to exhaust the LFS server by uploading many large blobs without referencing them, expecting GC to clean up. Mitigations: per-collection quotas cap the worst case; LFS servers should enforce per-user upload quotas at the auth layer; GC's grace period prevents the worst case of "upload, never reference, immediate sweep" — orphans must age before they are collected, but the storage cost is bounded by `max_blob_total`.

## Replication

### Two-channel model

A LedgerDB repository with blobs replicates over two distinct channels:

- **Git protocol** (smart HTTP, SSH, or local): synchronises the TxV3 transaction history, document tree, blob pointers, and `_meta/`. Cheap; pointer files are tiny.
- **git-lfs protocol**: synchronises the blob content. Expensive; bandwidth is dominated by the cumulative size of all live blobs.

`git push` and `git pull` automatically trigger LFS transfer for objects referenced by checked-out commits when `git-lfs` is installed and the smudge filter is enabled. The user-facing commands are:

```text
git lfs fetch [--all]    # download blob content for current branch (or all branches)
git lfs push origin main # push blob content for commits being pushed
git lfs prune            # remove local LFS content not referenced by recent commits
```

### Bandwidth tradeoff

JSON-only LedgerDB repositories enjoy very small clone footprints — a million-document repo with median doc size 4 KiB clones in a few hundred MiB after Git delta compression. Adding blobs changes the calculus dramatically:

| Repo profile        | Git transfer | LFS transfer | Total       |
| :------------------ | :----------- | :----------- | :---------- |
| 1M docs, JSON only  | ~200 MiB     | 0            | ~200 MiB    |
| 1M docs + 10k blobs of 1 MiB avg | ~210 MiB | ~10 GiB | ~10.2 GiB |
| 1M docs + 1k blobs of 100 MiB avg | ~210 MiB | ~100 GiB | ~100 GiB |

Mitigations:

- **Partial clone**: `git lfs clone --skip-smudge` and selectively `lfs fetch` only the blobs the client needs.
- **Lazy fetch on `blob get`**: `blob get` triggers a per-OID fetch if the local LFS store does not have the content. Read-heavy clients with selective access patterns pay only for what they consume.
- **Sparse checkout**: collections that are blob-heavy can be in distinct subtrees that thin clients exclude from checkout.

### Mirroring and conflict-free property

Mirrors (read-only replicas) are first-class Git mirrors plus first-class LFS mirrors. The LFS server should be configured for replication independently of the Git server.

Because blobs are content-addressed and immutable, there is no merge conflict on blob content. Two clients writing the same logical document with different blob references produce a standard TxV3 merge conflict on the document, not on the blobs. Two clients writing the same blob (same bytes → same SHA-256) deduplicate automatically.

## Operational prerequisites

### Host requirements

- `git` ≥ 2.39 (for stable LFS pointer behaviour).
- `git-lfs` ≥ 3.4 installed on the host and discoverable in `$PATH`.
- For chunked uploads with resume, the LFS server must support the chunked extension; otherwise downgrade is automatic.

LedgerDB detects `git-lfs` at startup and emits a clear error if it is missing.

### Repository bootstrap

On a repository that has never used blobs, the first `blob put` performs an idempotent bootstrap:

1. `git lfs install --local` (registers smudge/clean filters in `.git/config`).
2. Ensure `.gitattributes` at the repository root contains the `blobs/**/sha256_* filter=lfs ...` line; create or edit as needed.
3. Ensure `.gitignore` contains `.ledgerdb/blob-uploads/`.
4. Create the `blobs/` directory and a placeholder `blobs/.keep` if the directory is empty.
5. Commit the bootstrap as `chore(blobs): enable git-lfs blob storage`.

Subsequent `blob put` calls skip the bootstrap. The bootstrap is also exposed as `ledgerdb blob init` for users who want to enable blobs ahead of their first upload.

### LFS server configuration

The LFS server URL is read from `git config lfs.url` (or `git config remote.<name>.lfsurl`). LedgerDB does not parse its own LFS config; it defers entirely to `git-lfs`'s normal resolution rules. Common configurations:

- **Default (same host as Git remote)**: no extra config needed; LFS will use `<remote>/info/lfs`.
- **Dedicated LFS server**: set `git config -f .lfsconfig lfs.url https://lfs.example.com/`.
- **Standalone (local-only) LFS**: leave `lfs.url` unset; blobs live in `.git/lfs/objects/` and are not pushed anywhere.

Auth is delegated to `git-lfs`'s standard credential helpers (basic auth via `~/.netrc`, SSH key inheritance from the Git remote, custom helpers configured via `git config credential.helper`). LedgerDB never sees or stores LFS credentials.

## Open questions

### Native blobstore backend

If `git-lfs` proves operationally restrictive — particularly around ACLs, per-blob TTLs, and audit-grade access logging — a native LedgerDB blobstore is a candidate for v0.4+. Candidate designs: direct S3/GCS/Azure SDK integration with a pluggable `BlobBackend` interface; a small purpose-built HTTP service that LedgerDB clients address with a custom protocol.

The decision criterion is operational pain: if more than (say) 30% of production users report a blocker rooted in `git-lfs`, the project revisits. The blob sentinel design is forward compatible with any future backend because it commits to content via SHA-256 alone.

### Inline base64 blobs for tiny content

For blobs in the hundreds-of-bytes range (a 200-byte signature blob, a 64-byte symmetric IV), the overhead of LFS pointer + LFS round-trip is silly. An alternative sentinel was considered:

```json
{ "$blob": "sha256:abc...", "$inline": "base64-encoded-bytes" }
```

Not implemented in v0.3 because it complicates JSON parsing on every `doc get` (the parser must walk all `$blob` sentinels to extract inline content even if the caller does not want it); blurs the integrity story (is `$inline` covered by the TxV3 hash chain? technically yes, but mixed semantics are confusing); and the use case is not strong enough today — users with sub-KB binary metadata can encode it inline as a normal JSON string field. The door is left open by reserving the `$inline` key alongside `$blob`.

### Content-type as first-class

`meta.json` carries an optional `content_type` hint but LedgerDB never enforces or validates it. Open: should the sentinel optionally carry the MIME type, e.g. `{"$blob":"sha256:...","$type":"application/pdf"}`? This would let queries and indexers filter by type without dereferencing. The argument against is purity: the SHA-256 already determines the bytes; MIME is metadata that lives more naturally in `_meta/`.

### Per-blob replication policies

For a multi-region deployment, some blobs (e.g., personal data subject to data-residency rules) must not replicate to certain regions. The current design treats the LFS server as a single replication boundary. Per-blob policies would require either multiple LFS servers with routing rules (operator concern, out of scope here) or a metadata extension to the sentinel.

### Streaming read API

`blob get` streams to stdout, but there is no SDK-level streaming reader yet. SDKs currently materialise blob content fully before returning. A streaming `Reader` interface is straightforward to add and is tracked separately.

### Garbage collection on append mode at scale

Append-mode repositories accumulate blob references indefinitely. For a 10-year-old repo with weekly snapshots and rich attachments, the live set may grow without bound. Compaction strategies — squashing very old history to drop unreferenced-since-N-years blobs — are a topic for a separate proposal, likely an amendment to retention policy.

## Compatibility and migration

Repositories that predate v0.3 have no `blobs/` tree and no LFS configuration. They are unaffected: the engine does not require LFS to be installed when no blob sentinels are present in any document. The first `blob put` triggers the bootstrap.

Documents written before v0.3 contain no `$blob` sentinels. The engine treats `$blob` strictly: the key is reserved going forward. Any pre-v0.3 document that happens to contain a `$blob` key (extremely unlikely; the prefix was undocumented but never reserved) will now be rejected by writes that attempt to round-trip it.

Pre-v0.3 engines do not understand the sentinel. They will return the document including the `{"$blob":"..."}` JSON object as opaque data. They cannot dereference; they will not corrupt; they will not enforce quotas. This is a graceful read-only degradation.

The Go, TypeScript, and Python SDKs will gain a `BlobClient` in v0.3:

- `BlobClient.Put(reader, opts) -> Digest`
- `BlobClient.Get(digest, writer, opts) -> error`
- `BlobClient.Stat(digest) -> Metadata`
- `BlobClient.Remove(digest, opts) -> error`

## Summary

LedgerDB v0.3 plans to add binary blob storage via `git-lfs` without modifying the TxV3 protocol. Blobs are content-addressed SHA-256 objects stored in LFS; LedgerDB documents reference them via a single-key sentinel `{"$blob":"sha256:<hex>"}`. The CLI gains a `blob` subcommand family (`put`, `get`, `rm`, `stat`, `ls`); writes validate blob existence; reads return sentinels as-is and require an explicit dereference. GC piggybacks on `ledgerdb maintenance gc` with a mark-and-sweep over the TxV3 transaction set, respecting history mode (append vs amend). Quotas live on the collection. Replication composes the Git protocol with the `git-lfs` protocol; bandwidth amplifies for blob-heavy repos and is mitigated by lazy fetch.

The central virtue is conservatism: it adds the smallest possible surface area to TxV3 (zero — the sentinel is just JSON), reuses Git's own large-file ecosystem, and preserves the verifiability invariants the rest of LedgerDB depends on. The price is an operational dependency on `git-lfs`, judged acceptable for v0.3 and revisited in v0.4 if production feedback demands a native backend.

## See also

- [TxV3 Format](IO-TxV3-Format) — the wire format the sentinel embeds into.
- [Storage Layout](Concepts-Storage-Layout) — the sharding philosophy the `blobs/` layout mirrors.
- [Integrity and Verification](Concepts-Integrity-And-Verification) — the chain that covers the sentinel byte-for-byte.
- [History Modes](Concepts-History-Modes) — append vs amend; the choice changes which blobs are live.
- [Replication and HA](Ops-Replication-HA) — how the two-channel model fits multi-region deployments.
- Epic [#6](https://github.com/osvaldoandrade/ledgerdb/issues/6) — the implementation tracker.
