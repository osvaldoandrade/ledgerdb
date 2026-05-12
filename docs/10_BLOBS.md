# Binary Blob Storage via git-lfs


## 1. Abstract

This document specifies the design for binary blob storage in LedgerDB, extending the engine beyond its current JSON-only payload to accommodate large opaque artifacts (images, audio, video, archives, signed binaries, model weights, etc.) while preserving the immutability, verifiability, and Git-native properties described in `docs/01_STORAGE_INTERFACE.md`. The chosen substrate is **git-lfs** (Git Large File Storage): binary content is pushed to an LFS server, while LedgerDB tracks references to that content inside TxV3 documents using a sentinel object `{"$blob":"sha256:<hex>"}`. Blobs are out-of-band with respect to the TxV3 transaction blob itself, but their content-addressed identifiers participate in the document state and therefore inherit a transitive integrity guarantee: tampering with a blob is detectable on read because the SHA-256 in the sentinel will mismatch.

This design intentionally piggybacks on the maturity of git-lfs rather than building a native object store. The trade-offs (operational dependency on the git-lfs binary, bandwidth amplification on clone/fetch, lack of fine-grained ACLs) are discussed in section 11. A native blobstore backend remains an open question (section 12) but is out of scope for v0.3.

## 2. Problem & Non-Goals

### 2.1 Problem Statement

LedgerDB v0.2.x stores small JSON documents. Users in audit-heavy domains (legal evidence, scientific notebooks, regulated workflows) increasingly want to associate large binary artifacts with their documents:

* A contract document referencing the PDF that was signed.
* An incident record carrying packet captures or core dumps.
* A scientific record referencing raw instrument output.
* A user record carrying an avatar image.

Today the only options are:

1. **Inline base64**: stuff the binary into a JSON string. This explodes TxV3 blob size, defeats Git delta compression (because base64 of changed binary content has no useful delta), and pushes parsing cost onto every reader.
2. **Out-of-band storage**: keep the binary in S3/GCS and store a URL in the document. This breaks the LedgerDB integrity contract (the URL is mutable; the bucket is mutable; nothing in the TxV3 hash chain detects swap-in/swap-out attacks), and it splits the operational story across two systems.

We need a first-class binary path that:

* Keeps the TxV3 transaction blob small and Git-friendly.
* Preserves a content-addressed integrity guarantee.
* Distributes alongside the repository (clone/push/fetch story is unified).
* Does not require a separate operational system in the simple case.

### 2.2 Non-Goals

* **Native S3/GCS backends.** Users who want object-storage durability can configure a git-lfs server backed by S3/GCS/Azure Blob. LedgerDB does not implement those adapters directly.
* **CDN integration.** Edge caching of blobs is delegated to the LFS server / fronting infrastructure.
* **Transcoding, thumbnailing, or media processing.** Blobs are opaque byte sequences. Any derived artifact (thumbnail, transcript, OCR text) is the caller's responsibility and should be stored as a separate blob with its own digest.
* **Streaming append to a blob.** Blobs are immutable. Mutation requires writing a new blob with a new digest.
* **Encryption-at-rest (this doc).** Encryption is tracked under epic #73; this doc only documents how that future capability layers on top of the LFS substrate.
* **Inline base64 fallback.** Even for tiny blobs (a few hundred bytes), inline base64 is not supported. Section 12 discusses why we currently reject this.

## 3. Storage Model

### 3.1 The Three Locations of a Blob

For any single blob, three distinct on-disk artifacts exist:

1.  **The content**, byte-identical to what the caller uploaded, living in the configured LFS storage backend (a local `.git/lfs/objects/` directory or a remote LFS server). This is what `ledgerdb blob get` ultimately streams back.
2.  **The LFS pointer**, a small text file checked into the Git repository under a path determined by the LedgerDB blob layout (see 3.3). The pointer contains the OID and size; git-lfs replaces the pointer with the content on checkout when configured.
3.  **The sentinel reference**, a JSON object of shape `{"$blob":"sha256:<hex>"}` embedded somewhere in a TxV3 document. This is the only mechanism by which a blob becomes "live" from LedgerDB's perspective.

The crucial invariant is that **(1) and (3) share the SHA-256**. The pointer file's OID is the same SHA-256 the sentinel carries. This permits two independent integrity paths:

* Git-lfs verifies the pointer↔content pairing on download.
* LedgerDB verifies the sentinel↔content pairing on `blob get` by re-hashing the bytes it receives.

A blob is therefore content-addressed end to end: a malicious or buggy LFS server cannot serve different bytes under the same digest without detection.

### 3.2 Why Not Embed the Bytes in TxV3?

The TxV3 protocol (see `docs/01_STORAGE_INTERFACE.md` section 3) treats the `snapshot` and `patch` payloads as the canonical document state. Embedding multi-megabyte binary payloads there would:

* Inflate every TxV3 blob, defeating loose-object packing and forcing Git to delta-compress over binary content (which usually fails to compress).
* Break canonicalization assumptions: RFC 8785 (JCS) canonicalization is defined over JSON. Mixing in raw bytes requires escaping that is verbose and error-prone.
* Couple replication of a small metadata change to the cost of moving every binary attached to that document.

By keeping the bytes in LFS and the reference in TxV3, the TxV3 blob remains small, deterministic, and JCS-canonicalizable. The blob is fetched only when the reader chooses to dereference it.

### 3.3 On-Disk Layout

LedgerDB uses a content-addressed layout for the LFS pointer files, mirroring the sharding philosophy of `documents/`:

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

This tells git-lfs to intercept every blob pointer file during checkout. The `_meta/` subtree is plain JSON checked into Git directly — it is small, benefits from Git delta compression, and is queryable without contacting the LFS server.

### 3.4 The Blob Sentinel

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

* **Recursively recognized** at any depth — root, nested object, array element, deeply nested array of objects, etc.
* **Strict in shape**: an object with exactly one key, `$blob`, mapping to a string matching `^sha256:[0-9a-f]{64}$`. Any deviation (extra keys, wrong digest length, mixed case in hex, unknown algorithm prefix) is rejected at write time.
* **Reserved**: the key `$blob` is reserved at any position inside any LedgerDB document. Documents containing `$blob` keys that do not conform to the sentinel shape are rejected.

This single-key, prefix-tagged convention follows the same spirit as Postgres' `$$ ... $$` dollar-quoting and MongoDB's `$type` operators: a syntactically minimal, easy-to-detect marker.

### 3.5 Why SHA-256?

SHA-256 is already the canonical hash inside LedgerDB (TxV3 `parent_hash`, document sharding, Git's own SHA-256 mode). Using the same primitive for blob identity:

* Avoids a second cryptographic dependency.
* Allows the LFS layer (which natively understands SHA-256 OIDs) to be used unchanged.
* Aligns with the broader content-addressed-storage industry convention.

We do not currently support other digest algorithms in the sentinel. The `sha256:` prefix exists for future-proofing (e.g., a `blake3:` migration), but parsers MUST reject any non-`sha256:` prefix today.

## 4. CLI Surface

The CLI verbs mirror the existing `ledgerdb doc` family and are grouped under `ledgerdb blob`. All commands take the usual repository-discovery flags (`--repo`, `--collection` where relevant).

### 4.1 `ledgerdb blob put <file>`

Uploads a local file as a blob and prints its digest.

```text
$ ledgerdb blob put ./minutes_2026_q3.pdf
sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

Behavior:

* Streams the file through a SHA-256 hasher without loading it fully into memory.
* If the digest already exists in `blobs/`, the command is a no-op and prints the existing digest (deduplication is free, since storage is content-addressed).
* If the digest is new, writes the LFS pointer under `blobs/<shard>/sha256_<hex>`, writes `_meta/sha256_<hex>/meta.json`, stages a Git commit recording the new pointer, and triggers `git lfs push` against the configured remote.
* Returns nonzero on any of: hash mismatch (pathological hardware bug), LFS server rejection, repository write failure, exceeded per-collection quota (see section 8).

Flags:

* `--chunk-size <bytes>` — chunk size for the resumable upload protocol (default 4 MiB, see section 6).
* `--resume` — resume a partially-uploaded blob (section 6).
* `--content-type <mime>` — hint stored in `meta.json`. Not interpreted by LedgerDB but useful for downstream tools.
* `--no-push` — write the local pointer and metadata but skip the `git lfs push`. The user must push later with `git lfs push --all`.

### 4.2 `ledgerdb blob get <sha256:hex> [-o out]`

Streams the blob content from LFS storage to stdout or to `out`.

```text
$ ledgerdb blob get sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 -o minutes.pdf
$ ledgerdb blob get sha256:e3b0... | sha256sum
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  -
```

Behavior:

* Resolves the digest to the pointer file under `blobs/`.
* If the LFS object is not present locally, performs a `git lfs fetch` for that specific OID.
* Streams bytes to the destination while re-hashing.
* On EOF, compares the computed SHA-256 to the requested one. Mismatch raises a fatal `BlobIntegrityError` and the partial output is removed if writing to a file.

Flags:

* `-o, --output <path>` — destination file. Default is stdout.
* `--no-verify` — skip the integrity recheck (NOT recommended; intended for debugging only).
* `--peek <bytes>` — read only the first N bytes (e.g., for sniffing magic numbers); integrity check is skipped in this mode and stderr emits a clear warning.

### 4.3 `ledgerdb blob rm <sha256:hex>`

Marks a blob as removable. Does not physically delete content.

```text
$ ledgerdb blob rm sha256:fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9
marked for GC: sha256:fcde2b2edba56bf...
```

Behavior:

* Refuses to mark a digest that is still referenced by any TxV3 document in the current state tree (append mode includes every revision; amend mode counts only the latest). Returns nonzero with a clear error listing one example document that still references the digest.
* Otherwise sets a `removed_at` field in `_meta/sha256_<hex>/meta.json` and commits.
* Physical removal happens during `ledgerdb maintenance gc` (section 7).

Rationale for two-phase delete: an interactive `rm` that immediately deleted bytes would conflict with the principle that Git history is immutable — historical commits in append mode may still reference the blob. We mark, then sweep.

Flags:

* `--force` — bypass the live-reference check. Will refuse if the blob is referenced by the very latest snapshot of any document in any collection; this is to prevent foot-shooting in amend mode.

### 4.4 `ledgerdb blob stat <sha256:hex>`

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

The `refcount` is computed by scanning the current state tree (or every revision under append mode). `referenced_by` lists at most the first 20 references for readability. Use `--all-refs` to dump them all to stdout (potentially many for popular blobs).

### 4.5 `ledgerdb blob ls`

Convenience command for listing all blobs known to the repository, optionally filtered by liveness, size range, or content-type hint.

```text
$ ledgerdb blob ls --live --min-size 1MB
sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  1.8 MiB  application/pdf      live
sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae  4.3 MiB  application/pdf      live
sha256:fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9  982 KiB  image/jpeg           removed
```

This command reads only `_meta/` and the in-repo state tree; it never contacts the LFS server.

## 5. Embedding in Documents

### 5.1 Write Path

When `ledgerdb doc put` or `ledgerdb doc patch` (or their SDK equivalents) receives a document, the engine performs a recursive scan for `$blob` sentinels. For every sentinel encountered:

1. Validate the shape (single key, value matches `sha256:[0-9a-f]{64}$`).
2. Verify the blob is known to the repository: a pointer exists under `blobs/<shard>/sha256_<hex>` and the corresponding `_meta/sha256_<hex>/meta.json` is present and not marked `removed_at`.
3. If quotas are configured on the collection (section 8), accumulate the referenced blob's size into the per-document and per-collection budget and fail fast if either is exceeded.
4. Increment the conceptual refcount for the blob (the actual refcount is recomputed lazily by `stat` and `gc`; this step is logical).

If any sentinel fails validation, the entire write is rejected with `BlobReferenceError` naming the offending JSON path. **Writes are atomic with respect to blob references**: a `put` either succeeds with all referenced blobs verified or fails without committing.

Importantly, the engine does **not** download blob content during validation — it only checks for the pointer's existence. This keeps writes fast and avoids surprising bandwidth costs on `doc put`.

### 5.2 Patch Semantics

JSON Patch (RFC 6902) operations may add, remove, or replace blob sentinels exactly like any other JSON value:

```json
[
  { "op": "add",     "path": "/attachments/-", "value": { "$blob": "sha256:abc..." } },
  { "op": "replace", "path": "/cover_image",   "value": { "$blob": "sha256:def..." } },
  { "op": "remove",  "path": "/old_attachment" }
]
```

The validation in 5.1 runs against the post-patch document state, not against the patch ops themselves. This means a patch may move references around freely, but the final state must reference only existing blobs.

### 5.3 Read Path

`ledgerdb doc get` returns the document with blob sentinels **as-is**:

```json
{
  "name": "Q3 board minutes",
  "signed_pdf": { "$blob": "sha256:e3b0..." }
}
```

The engine does not auto-dereference. Callers who want the bytes invoke `ledgerdb blob get` (or the SDK equivalent) with the digest. Rationale:

* Predictable I/O: a `doc get` should not surprise the caller with a 2 GB download.
* Lazy fetching: many use cases inspect document metadata without ever touching the blob payload.
* Separation of concerns: blob streaming has different error and retry semantics than document fetching.

SDKs MAY offer a convenience method `getWithBlobs()` or `materialize()` that walks the document and pre-fetches all referenced blobs, but this is opt-in.

### 5.4 Canonicalization

The blob sentinel is, syntactically, a normal JSON object. RFC 8785 (JCS) canonicalization rules apply to it without modification:

* Key `$blob` is lexicographically the only key, so ordering is moot.
* The digest string contains only `0-9a-f` plus the literal `sha256:` prefix, all safely UTF-8.
* No special escaping or normalization beyond standard JCS rules.

Therefore the `parent_hash` Merkle chain in TxV3 covers the sentinel byte-for-byte. A blob substitution attack (swap the LFS bytes under the same OID, then serve a tampered document) is detected on `blob get` integrity recheck even though the TxV3 chain alone cannot see it (the TxV3 hash chain attests to the *sentinel*, not to the bytes — but the sentinel cryptographically commits to the bytes).

## 6. Chunked Upload with Resume

Large blobs (multi-GB binaries, video files, scientific datasets) cannot rely on single-shot HTTP PUT. LedgerDB layers a simple chunked-upload protocol on top of git-lfs's batch API.

### 6.1 Protocol Sketch

1. `blob put <file>`:
   * Stream the file, computing SHA-256.
   * Split into chunks of `--chunk-size` bytes (default 4 MiB).
   * For each chunk: PUT to the LFS server's chunk endpoint with the chunk index and the running digest.
   * On chunk success: append `{index, sha256, size, uploaded_at}` to `.ledgerdb/blob-uploads/sha256_<hex>.json`.
   * On final chunk: send a `commit` request; LFS server stitches chunks, recomputes the SHA-256, and confirms.
2. If the upload fails (network, signal, server 5xx) the partial state is left in `.ledgerdb/blob-uploads/sha256_<hex>.json`.
3. `blob put --resume <file>`:
   * Recompute the SHA-256 of the full file; refuse if it differs from the digest stored in the partial state file (the file was modified mid-upload).
   * Read the partial state; skip already-uploaded chunks; resume from the next index.

### 6.2 Partial State File Schema

```json
{
  "digest":      "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "size":        4831838208,
  "chunk_size":  4194304,
  "started_at":  "2026-05-12T09:00:00Z",
  "chunks": [
    { "index": 0, "sha256": "aa...", "size": 4194304, "uploaded_at": "2026-05-12T09:00:11Z" },
    { "index": 1, "sha256": "bb...", "size": 4194304, "uploaded_at": "2026-05-12T09:00:22Z" }
  ],
  "last_uploaded_index": 1
}
```

* The file lives in `.ledgerdb/blob-uploads/` (untracked by Git; users SHOULD add this directory to `.gitignore`, and `ledgerdb` will ensure that the gitignore entry exists on first use).
* `chunk_size` is fixed for the lifetime of a single upload; changing `--chunk-size` mid-upload aborts and restarts.

### 6.3 Cleanup

Successful uploads remove the partial state file. Stale partials are reaped by `ledgerdb maintenance gc` after a configurable TTL (default 7 days) and on demand by `ledgerdb blob cleanup-uploads`.

### 6.4 Server-Side Assumptions

The LFS server must support either:

* The git-lfs **Locks API** plus a custom chunked-upload extension, or
* Multipart-resumable upload semantics (S3-style multipart) exposed through an LFS-compatible gateway.

For LFS servers that lack chunked-upload support, LedgerDB falls back to single-shot upload with no resume; `--resume` becomes a no-op and emits a warning. This downgrade is logged at info level.

## 7. Garbage Collection

### 7.1 Defining "Live" and "Orphan"

A blob digest D is **live** if there exists at least one TxV3 transaction whose post-canonicalization snapshot contains a sentinel `{"$blob":"sha256:D"}` at any JSON path. Otherwise D is **orphan**.

The exact set of TxV3 transactions considered depends on the manifest's history mode (see `docs/01_STORAGE_INTERFACE.md` section 4.3 and `internal/domain/manifest.go`):

* **append mode** (default): every transaction in every `tx/` stream counts. A blob referenced by an old PUT but no longer by the current HEAD is still live.
* **amend mode**: only the current state matters. A blob no longer referenced by `tx/current.txpb` of any document is eligible for collection immediately.

### 7.2 The GC Pass

The existing `internal/app/maintenance/` package gains a blob-aware sub-pass. `ledgerdb maintenance gc` performs:

1. **Mark phase**: walk every TxV3 transaction (filtered by history mode), JCS-canonicalize the snapshot, recursively extract `$blob` sentinels, build the live set L.
2. **List phase**: enumerate `blobs/` to obtain the candidate set C.
3. **Filter phase**: O = C \ L. Drop from O any blob whose `_meta/.../meta.json` does not carry a `removed_at` timestamp older than the configured grace period (default 24 hours). The grace period prevents racing GC against in-flight writes.
4. **Sweep phase**: for each digest in O, delete the LFS pointer file, delete `_meta/sha256_<hex>/`, and call `git lfs prune` for the OID.
5. **Commit phase**: a single Git commit records the removed pointer files (commit message: `gc: prune N orphan blobs`).
6. **Push phase** (optional, gated by `--push`): push the GC commit and force the LFS server to drop the deleted OIDs.

### 7.3 Safety Properties

* GC is **single-writer**: it acquires the existing maintenance lock. Concurrent `blob put` will block briefly during the sweep phase.
* GC is **idempotent**: rerunning after a successful run is a no-op.
* GC is **interruptible**: the sweep phase processes one blob at a time and records progress to `.ledgerdb/gc-progress.json` so an interrupted GC can be resumed.
* GC **never removes a live blob**: the mark phase is exhaustive within the configured history mode. The grace period covers the window between sentinel write and commit visibility.

### 7.4 Dry Run

```text
$ ledgerdb maintenance gc --dry-run --blobs-only
would remove 47 orphan blobs, freeing ~1.2 GiB:
  sha256:fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9  982 KiB
  sha256:7d865e95...                                                        16 MiB
  ...
```

Dry run does not commit or push.

## 8. Quotas

Per-collection quotas are configured via `ledgerdb collection apply`:

```text
$ ledgerdb collection apply --name contracts \
    --max-blob-size  10MB  \
    --max-blob-total 1GB
```

* `--max-blob-size` is the upper bound for any single blob referenced by a document in this collection. A `doc put` whose document references a blob larger than this limit is rejected.
* `--max-blob-total` is the cumulative size of all live blobs referenced by all documents in this collection. The cumulative size is recomputed lazily during write and cached in the manifest.

Enforcement happens in `internal/app/doc/` at write time (the same layer that already validates schema, OCC context, etc.). Quotas are advisory: existing data that predates a quota tightening is not retroactively rejected, but new writes that would push the collection further over the limit are denied. An explicit `--rebalance` flag may force a recompute of cumulative size if the cached value drifts (e.g., after manual repo surgery).

### 8.1 Quota Errors

```text
$ ledgerdb doc put contracts/2026-09 ./pkg.json
Error: blob quota exceeded for collection "contracts"
  blob:      sha256:e3b0c4...
  blob size: 18.4 MiB
  limit:     10 MiB (--max-blob-size)
```

```text
$ ledgerdb doc put contracts/2026-09 ./pkg.json
Error: collection blob total quota exceeded
  current total: 1.04 GiB
  this write:    +24 MiB (1 new ref)
  limit:         1 GiB (--max-blob-total)
```

### 8.2 Unbounded Defaults

If neither flag is set, the collection has no blob quotas. This is deliberately the default: surprise-rejecting writes on a freshly-upgraded repo would be a worse experience than relying on operators to apply quotas explicitly.

## 9. Security Implications

### 9.1 Integrity (Recap)

Blob content is content-addressed by SHA-256. The TxV3 hash chain (`parent_hash`) attests to the canonical bytes of every transaction blob, and those bytes include the `$blob` sentinel verbatim. Therefore:

* Tampering with the TxV3 chain is detected by the standard `parent_hash` walk (see `docs/04_EXECUTION.md`).
* Swapping the bytes in LFS storage without updating the OID is detected by SHA-256 mismatch on `blob get`.
* Swapping the bytes *and* the OID requires forging a SHA-256 collision (computationally infeasible) AND modifying every TxV3 transaction that references the digest (which breaks the `parent_hash` chain).

This gives transitive integrity without expanding the TxV3 schema.

### 9.2 What Is NOT Covered

* **Confidentiality**: LFS content is, by default, stored in cleartext on whichever backend the LFS server is configured against. Encryption-at-rest is tracked under epic #73; the LFS server may use server-side encryption (e.g., S3 SSE-KMS) but client-visible cleartext is still observable by anyone with credentials to the server.
* **Authorization**: LedgerDB itself does not enforce who may `blob get`. Access control sits at the LFS server (HTTP auth, mTLS, signed URLs). Repositories that need fine-grained ACLs on blobs must front the LFS server with an authentication proxy.
* **Audit log of `blob get`**: dereferencing a blob is a read against the LFS server; it does not generate a TxV3 transaction. Operators who need read auditing must configure their LFS server's access logs.

### 9.3 Encryption-at-Rest (Forward Reference)

When epic #73 lands, the proposed model is:

* Per-collection (or per-repo) data encryption keys (DEKs) wrapped by a KMS-managed KEK.
* Client-side envelope encryption of the blob content before upload; only the ciphertext touches LFS.
* The `$blob` sentinel optionally extends to `{"$blob":"sha256:<hex>","$wrap":"<key-id>"}` to record the wrapping key.

This is forward compatible with the present design: today's sentinels remain valid (no `$wrap` means cleartext); tomorrow's sentinels carry the wrap key id.

### 9.4 Denial of Service via Quota Exhaustion

A malicious or buggy client could attempt to exhaust the LFS server by uploading many large blobs without referencing them from any document, expecting GC to clean up. Mitigations:

* `--max-blob-size` and `--max-blob-total` (section 8) cap the worst case per collection.
* LFS servers SHOULD enforce per-user upload quotas at the auth layer.
* GC's grace period prevents the worst case of "upload, never reference, immediate sweep" — orphans must age before they're collected, but the storage cost is bounded by `max_blob_total`.

## 10. Replication

### 10.1 Two-Channel Model

A LedgerDB repository with blobs replicates over two distinct channels:

* **Git protocol** (smart HTTP, SSH, or local): synchronizes the TxV3 transaction history, document tree, blob pointers, and `_meta/`. This is cheap; pointer files are tiny.
* **Git-LFS protocol**: synchronizes the blob content. This is expensive; bandwidth is dominated by the cumulative size of all live blobs.

`git push` and `git pull` automatically trigger LFS transfer for objects referenced by checked-out commits when `git-lfs` is installed and the smudge filter is enabled. The user-facing commands are:

```text
git lfs fetch [--all]    # download blob content for current branch (or all branches)
git lfs push origin main # push blob content for commits being pushed
git lfs prune            # remove local LFS content not referenced by recent commits
```

LedgerDB-level wrappers exist (`ledgerdb pull`, `ledgerdb push`) but these compose the two channels and are documented in `docs/06_REPLICATION.md`.

### 10.2 Bandwidth Tradeoff

JSON-only LedgerDB repositories enjoy very small clone footprints — a million-document repo with median doc size 4 KiB clones in a few hundred MiB after Git delta compression. Adding blobs changes the calculus dramatically:

| Repo profile        | Git transfer | LFS transfer | Total       |
| :------------------ | :----------- | :----------- | :---------- |
| 1M docs, JSON only  | ~200 MiB     | 0            | ~200 MiB    |
| 1M docs + 10k blobs of 1 MiB avg | ~210 MiB | ~10 GiB | ~10.2 GiB |
| 1M docs + 1k blobs of 100 MiB avg | ~210 MiB | ~100 GiB | ~100 GiB |

Mitigations:

* **Partial clone**: clones may use `git lfs clone --skip-smudge` and selectively `lfs fetch` only the blobs the client needs.
* **Lazy fetch on `blob get`**: as documented in section 4.2, `blob get` triggers a per-OID fetch if the local LFS store does not have the content. Read-heavy clients with selective access patterns pay only for what they consume.
* **Sparse checkout**: collections that are blob-heavy can be in distinct subtrees that thin clients exclude from checkout.

### 10.3 Mirroring

Mirrors (read-only replicas) are first-class Git mirrors plus first-class LFS mirrors. The LFS server SHOULD be configured for replication independently of the Git server. Documenting that two-way story is the responsibility of the deployment guide; this design doc only notes that the LedgerDB layer is agnostic to the LFS replication topology.

### 10.4 Conflict-Free Property

Because blobs are content-addressed and immutable, there is no merge conflict on blob content. Two clients writing the same logical document with different blob references produce a standard TxV3 merge conflict on the document, not on the blobs. Two clients writing the same blob (same bytes → same SHA-256) deduplicate automatically.

## 11. Operational Prerequisites

### 11.1 Host Requirements

* `git` ≥ 2.39 (for stable LFS pointer behavior).
* `git-lfs` ≥ 3.4 installed on the host and discoverable in `$PATH`.
* For chunked uploads with resume, the LFS server must support the chunked extension (section 6.4); otherwise downgrade is automatic.

LedgerDB detects `git-lfs` at startup and emits a clear error if it is missing:

```text
$ ledgerdb blob put ./foo.pdf
Error: git-lfs is not installed or not on PATH.
       Install from https://git-lfs.com/ and rerun `git lfs install` in this repo.
```

### 11.2 Repository Bootstrap

On a repository that has never used blobs, the first `blob put` performs an idempotent bootstrap:

1. `git lfs install --local` (registers smudge/clean filters in `.git/config`).
2. Ensure `.gitattributes` at the repository root contains the `blobs/**/sha256_* filter=lfs ...` line; create or edit as needed.
3. Ensure `.gitignore` contains `.ledgerdb/blob-uploads/`.
4. Create the `blobs/` directory and a placeholder `blobs/.keep` if the directory is empty.
5. Commit the bootstrap as `chore(blobs): enable git-lfs blob storage`.

Subsequent `blob put` calls skip the bootstrap. The bootstrap is also exposed as `ledgerdb blob init` for users who want to enable blobs ahead of their first upload.

### 11.3 LFS Server Configuration

The LFS server URL is read from `git config lfs.url` (or `git config remote.<name>.lfsurl`). LedgerDB does not parse its own LFS config; it defers entirely to git-lfs's normal resolution rules. Common configurations:

* **Default (same host as Git remote)**: no extra config needed; LFS will use `<remote>/info/lfs`.
* **Dedicated LFS server**: set `git config -f .lfsconfig lfs.url https://lfs.example.com/`.
* **Standalone (local-only) LFS**: leave `lfs.url` unset; blobs live in `.git/lfs/objects/` and are not pushed anywhere.

Documenting which LFS servers are supported in production is out of scope; the matrix is in `docs/06_REPLICATION.md`.

### 11.4 Auth

Auth is delegated to git-lfs's standard credential helpers (basic auth via `~/.netrc`, SSH key inheritance from the Git remote, custom helpers configured via `git config credential.helper`). LedgerDB never sees or stores LFS credentials.

## 12. Open Questions

### 12.1 Native Blobstore Backend

If git-lfs proves operationally restrictive — particularly around ACLs, per-blob TTLs, and audit-grade access logging — a native LedgerDB blobstore is a candidate for v0.4+. Candidate designs:

* Direct S3/GCS/Azure SDK integration with a pluggable `BlobBackend` interface.
* A small purpose-built HTTP service (think "MinIO but ledger-aware") that LedgerDB clients address with a custom protocol.

The decision criterion is operational pain: if more than (say) 30% of production users report a blocker rooted in git-lfs, we revisit. The blob sentinel design is forward compatible with any future backend because it commits to content via SHA-256 alone.

### 12.2 Inline Base64 Blobs for Tiny Content

For blobs in the hundreds-of-bytes range (a 200-byte signature blob, a 64-byte symmetric IV), the overhead of LFS pointer + LFS round-trip is silly. We considered an alternative sentinel:

```json
{ "$blob": "sha256:abc...", "$inline": "base64-encoded-bytes" }
```

We are not implementing this in v0.3 because:

* It complicates JSON parsing on every `doc get` (the parser must walk all `$blob` sentinels to extract inline content even if the caller does not want it).
* It blurs the integrity story (is `$inline` covered by the TxV3 hash chain? technically yes, but mixed semantics are confusing).
* The use case is not strong enough today; users with sub-KB binary metadata can encode it inline as a normal JSON string field.

We leave the door open by reserving the `$inline` key alongside `$blob`. A future RFC may revisit.

### 12.3 Content-Type as First-Class

`meta.json` carries an optional `content_type` hint but LedgerDB never enforces or validates it. Open question: should the sentinel optionally carry the MIME type, e.g. `{"$blob":"sha256:...","$type":"application/pdf"}`? This would let queries and indexers filter by type without dereferencing. The argument against is purity: the SHA-256 already determines the bytes; MIME is metadata that lives more naturally in `_meta/`.

### 12.4 Per-Blob Replication Policies

For a multi-region deployment, some blobs (e.g., personal data subject to data-residency rules) must not replicate to certain regions. The current design treats the LFS server as a single replication boundary. Per-blob policies would require either multiple LFS servers with routing rules (operator concern, out of scope here) or a metadata extension to the sentinel.

### 12.5 Streaming Read API

`blob get` streams to stdout, but there is no SDK-level streaming reader yet. SDKs currently materialize blob content fully before returning. A streaming `Reader` interface is straightforward to add and is tracked separately.

### 12.6 Garbage Collection on Append Mode at Scale

Append-mode repositories accumulate blob references indefinitely. For a 10-year-old repo with weekly snapshots and rich attachments, the live set may grow without bound. Compaction strategies — squashing very old history to drop unreferenced-since-N-years blobs — are a topic for a separate doc, likely a future amendment to `docs/05_RETENTION.md`.

## 13. Compatibility & Migration

### 13.1 Existing Repositories

Repositories that predate v0.3 have no `blobs/` tree and no LFS configuration. They are unaffected: the engine does not require LFS to be installed when no blob sentinels are present in any document. The first `blob put` triggers the bootstrap in section 11.2.

### 13.2 Reading Old Documents on a New Engine

Documents written before v0.3 contain no `$blob` sentinels. The engine treats `$blob` strictly: the key is reserved going forward. Any pre-v0.3 document that happens to contain a `$blob` key (extremely unlikely; the prefix was undocumented but never reserved) will now be rejected by writes that attempt to round-trip it. Such repos can either rename the offending key or set the manifest flag `legacy_dollar_blob_passthrough: true` for a deprecation window.

### 13.3 Reading New Documents on an Old Engine

Pre-v0.3 engines do not understand the sentinel. They will return the document including the `{"$blob":"..."}` JSON object as opaque data. They cannot dereference; they will not corrupt; they will not enforce quotas. This is a graceful read-only degradation.

### 13.4 SDK Versioning

The Go, TypeScript, and Python SDKs gain a `BlobClient` in v0.3:

* `BlobClient.Put(reader, opts) -> Digest`
* `BlobClient.Get(digest, writer, opts) -> error`
* `BlobClient.Stat(digest) -> Metadata`
* `BlobClient.Remove(digest, opts) -> error`

Schema and protocol details live in `docs/09_SDK_SPECS.md` and are updated in lockstep with this design.

## 14. Summary

LedgerDB v0.3 adds binary blob storage via git-lfs without modifying the TxV3 protocol. Blobs are content-addressed SHA-256 objects stored in LFS; LedgerDB documents reference them via a single-key sentinel `{"$blob":"sha256:<hex>"}`. The CLI gains a `blob` subcommand family (`put`, `get`, `rm`, `stat`, `ls`); writes validate blob existence; reads return sentinels as-is and require an explicit dereference. GC piggybacks on `ledgerdb maintenance gc` with a mark-and-sweep over the TxV3 transaction set, respecting history mode (append vs amend). Quotas live on the collection. Replication composes the Git protocol with the git-lfs protocol; bandwidth amplifies for blob-heavy repos and is mitigated by lazy fetch.

The design's central virtue is conservatism: it adds the smallest possible surface area to TxV3 (zero — the sentinel is just JSON), reuses Git's own large-file ecosystem, and preserves the verifiability invariants the rest of LedgerDB depends on. The price is an operational dependency on git-lfs, which we judge acceptable for v0.3 and revisit in v0.4 if production feedback demands a native backend.
