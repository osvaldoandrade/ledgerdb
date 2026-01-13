# Storage Engine & Interface


## 1. Abstract

This section defines the low-level storage architecture of LedgerDB. It details the interface contracts for data manipulation, the hierarchical directory sharding algorithm used to map logical keys to physical Git paths, and the strictly deterministic binary format (TxV3) used for persistence. The system implements a **Log-Structured Merge (LSM)** approach adapted for the Git Object Model, where every database write is an append-only operation resulting in a new immutable blob and a new commit tree.

## 2. The Mapping Layer: Hierarchical Directory Sharding

Git performance degrades linearly with the number of files in a single tree object (directory listing). To store millions of documents efficiently without overloading the file system or the Git index, LedgerDB employs a deterministic **Hierarchical Directory Sharding** strategy.

### 2.1 The Addressing Algorithm

We define a mapping function $M(k)$ where $k$ is the logical document key and $C$ is the collection name. We use the concatenation operator $\parallel$ to join the segments before hashing:

```math
H = \text{SHA-256}(C \parallel \text{separator} \parallel k)
```

The physical storage path $P$ is constructed as a hierarchy. We represent the directory containment structure using fractional notation, where the numerator acts as the parent directory for the denominator:

```math
P = \frac{\text{documents}}{\frac{C}{\frac{H_{0:2}}{\frac{H_{2:4}}{\text{DOC\_}\langle H \rangle}}}}
```

* **Sharded Default:** `documents/<collection>/<H[0:2]>/<H[2:4]>/DOC_<H>`
* **Legacy Flat Layout:** `documents/<collection>/DOC_<H>` (`stream_layout: flat`)

### 2.2 Structure Rationale

* **Namespace Isolation:** `documents/<collection>/` acts as the first partition level.
* **Uniform Distribution:** SHA-256 ensures documents are evenly distributed across the directory structure, preventing "hot spots" in the file system tree.
* **Collision Resistance:** The probability of hash collision with SHA-256 is negligible ($< 10^{-60}$), allowing us to assume uniqueness.
* **O(1) Resolution:** The path is computable purely from the key without disk I/O or index lookups.

### 2.3 Stream Layout

Inside the physical directory $P$, the system maintains the **Document Stream**:

```text
documents/users/a1/b2/DOC_a1b2c3.../
├── HEAD                     # Mutable pointer file (Blob)
└── tx/                      # Immutable Transaction Log
    ├── 1709392800_put.txpb
    ├── 1709392900_patch.txpb
    └── 1709393000_patch.txpb
```

* **HEAD:** A text file containing the relative path to the latest `.txpb` blob. This acts as the "Pointer" to the current state.
* **tx/:** A directory containing all historical transaction blobs.

## 3. TxV3 Binary Protocol

To ensure efficient storage and, crucially, cryptographic integrity, LedgerDB uses a custom Protobuf schema (**TxV3**). We strictly avoid storing raw JSON as the transaction wrapper to guarantee **Deterministic Serialization**, which is required for the Merkle Chain verification.

### 3.1 Schema Definition

```protobuf
syntax = "proto3";

package ledgerdb.v3;

message Transaction {
  // Identity & Ordering
  string tx_id = 1;          // ULID (Universally Unique Lexicographically Sortable Identifier)
  int64 timestamp = 2;       // Unix Nanoseconds
  
  // Routing
  string collection = 3;
  string doc_id = 4;
  
  // Operation Type
  enum Op {
    UNKNOWN = 0;
    PUT = 1;      // Snapshot
    PATCH = 2;    // Delta
    DELETE = 3;   // Tombstone
    MERGE = 4;    // Branch Resolution
  }
  Op op = 5;

  // The Data
  oneof payload {
    bytes snapshot = 6;      // Canonical JSON (RFC 8785)
    bytes patch = 7;         // JSON Patch Array (RFC 6902)
  }

  // Integrity (Merkle Link)
  string parent_hash = 8;    // SHA-256 of the previous transaction blob
  string schema_version = 9; // Version of schema used for validation
}
```

### 3.2 Determinism & Canonicalization

For the `parent_hash` verification to work (refer to `docs/04_EXECUTION.md`), the serialization of the transaction object must be bit-perfect reproducible.

1.  **Field Ordering:** Protobuf serializers must strictly adhere to field tag order.
2.  **JSON Canonicalization:** The `snapshot` bytes MUST be generated using **RFC 8785 (JCS - JSON Canonicalization Scheme)**.
    * Object keys sorted lexicographically.
    * No whitespace.
    * Strict UTF-8 encoding.

## 4. Log-Structured Storage on Git

LedgerDB implements a storage engine where data is never overwritten, only appended. This mimics the properties of an LSM-Tree (Log-Structured Merge Tree).

### 4.1 The "Append-Only" Blob Model

When a write operation occurs:
1.  A new `Transaction` object is instantiated in memory.
2.  It is serialized to bytes (Protobuf).
3.  Git computes the SHA-256 of the blob content.
4.  The blob is compressed (zlib) and written to `.git/objects/`.

At this stage, the data exists in the object store as a loose object but is not yet part of the "World State".

### 4.2 The Tree Replacement Strategy

To make the new transaction visible, we perform a **Copy-on-Write** operation on the Git Tree (similar to how Btrfs or ZFS handle snapshots):

1.  The leaf directory `DOC_<Hash>` is duplicated in memory.
2.  The `HEAD` blob entry in this virtual tree is updated to point to the new transaction file.
3.  A new Tree object representing `DOC_<Hash>` is hashed and written.
4.  This change "bubbles up" the tree hierarchy, resulting in a new Root Tree hash.
5.  A new **Commit** object wraps the Root Tree.

### 4.3 History Modes (Commit Ancestry)

The manifest controls whether Git commit ancestry is preserved:

* **append (default):** Each write creates a commit with a parent, preserving history. Transactions are stored as `tx/<timestamp>_<op>.txpb`.
* **amend:** Each write creates a root commit (no parent) and rewrites `tx/current.txpb`, keeping only the latest document state. Indexers must reset if the previous commit is not found.

### 4.4 Materialized State Tree

To reduce indexing complexity, LedgerDB can materialize current state alongside history:

* **History Tree:** `documents/<collection>/<hash>/tx/<timestamp>_*.txpb` (append-only).
* **State Tree:** `state/<collection>/<hash>/tx/current.txpb` (latest snapshot/tombstone).

Indexers can read `state/` to apply only changed documents (O(changes)), while history stays intact for audit.

### 4.5 Compaction (Garbage Collection)

Since every delta creates a new blob, the `.git` directory grows indefinitely. LedgerDB relies on `git gc` for physical storage optimization:
* **Packing:** Loose objects are combined into Packfiles.
* **Delta Compression:** Git automatically delta-compresses similar blobs (e.g., successive Snapshots) inside the packfile, providing efficient storage even for full snapshots.

## 5. System Interface

LedgerDB exposes a strictly consistent Key-Value interface.

### 5.1 `Put(collection, key, context, payload) -> Result<CommitID, Error>`
Persists a full snapshot of a document.
* **Context:** The `HEAD` SHA-256 of the stream known to the client. Used for Optimistic Concurrency Control.
* **Payload:** Canonical JSON.
* **Mechanism:** Writes a `TxV3` blob with `Op=PUT`. Resets the Delta Chain length to 0.

### 5.2 `Patch(collection, key, context, ops) -> Result<CommitID, Error>`
Appends a delta transaction.
* **Ops:** Array of JSON Patch operations (e.g., `[{ "op": "replace", "path": "/status", "value": "active" }]`).
* **Mechanism:**
    1.  Loads current state (Rehydration).
    2.  Applies patch in memory.
    3.  Validates against Schema.
    4.  Persists *only* the delta instructions in a `TxV3` blob with `Op=PATCH`.

### 5.3 `Get(collection, key) -> Result<Document, Error>`
Retrieves the current state.
* **Output:** `Document { Data: Map, Head: Hash, Commit: Hash }`.
* **Mechanism:** Performs "Read-Repair" or "Rehydration" by walking the transaction chain backwards via `parent_hash` until a Snapshot (`Op=PUT`) is found, then replaying deltas forward.

## 6. Performance Implications

| Metric | Complexity | Rationale |
| :--- | :--- | :--- |
| **Write Latency** | $O(1) + \text{Hash}$ | Writes are sequential appends. Cost is dominated by SHA-256 calculation. No disk seek for free space. |
| **Read Latency** | $O(K)$ | Reading requires traversing $K$ delta transactions. |
| **Storage Efficiency** | High | Git Packfiles use binary delta compression, minimizing space for repetitive data. |

*Note: To mitigate read amplification ($O(K)$), the system enforces a policy where if $K > 50$, the next write is forced to be a `PUT` (Snapshot).*

## 7. Conclusion

The storage architecture of LedgerDB sacrifices "in-place update" speed for **Immutability** and **Verifiability**. By strictly mapping logical documents to sharded physical paths and utilizing a deterministic binary format, we build a foundation where every database state is a valid Git commit, inheriting the durability and distribution properties of the underlying Version Control System.
