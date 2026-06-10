# Performance: Reads and Query

Reads in LedgerDB split cleanly into two paths. The first is a direct blob read from the Git object store — what `ledgerdb get` and the SDK's `Client.Get` do. The second is a query through the SQLite sidecar, populated by `ledgerdb index watch` — what `ledgerdb query` and `ledgerdb query explain` do. They have different cost models, different scaling shapes, and different freshness semantics. This page covers both, the bench that exercises the direct-read path, the cost of building and maintaining the sidecar that the query path depends on, and the tradeoffs between point lookups and scans.

## What this page covers

- The direct-read path: what `Client.Get` does, what `bench/reads_test.go` measures, and how layout affects the cost.
- The sidecar-query path: when the sidecar wins over direct reads, and what it costs to keep up to date.
- The catch-up cost of bringing a fresh sidecar online from a populated repository, and what `bench/sync_test.go` measures.
- The role of declared indexes: more indexes means more apply-time work in `index watch` and faster query-time access.
- The `--fast` and `--mode` flags on `index watch` and how each shifts the catch-up shape.

## What this page does not cover

- Query language semantics. `ledgerdb query` and the underlying SQL surface are documented in [Querying and Indexing Strategy](Concepts-Indexing).
- Watcher operability beyond performance — error handling, audit log, metrics. See [Observability Overview](Observability-Overview) and the per-pillar pages.
- Concurrent reader scaling. The benches are single-goroutine; multi-reader behavior is bounded by the underlying SQLite WAL mode and the Git pack reader's locking, neither of which is exercised by the current harness.

## Direct reads: `Client.Get`

The SDK's `Get` resolves a document by collection and ID directly from the Git object store, with no sidecar dependency (`pkg/ledgerdbsdk/doc.go:62-74`). The path:

1. Open the stream for the document. For `flat` layout the stream lives at `collections/<col>/<doc-id>.pb`; for `sharded` at `collections/<col>/<aa>/<bb>/<doc-id>.pb`. The lookup walks the Git tree starting from the head commit.
2. Read the latest transaction blob referenced from the stream's tip.
3. Decode the transaction with `internal/infra/txv3`.
4. If the operation was a snapshot (`PUT`), the payload is the document. If it was a `PATCH`, replay the patch chain back to the most recent snapshot to reconstruct the document state.

The cost is bounded by (a) the depth of the tree walk and (b) the depth of any patch chain since the last snapshot. The tree walk is constant-ish (one level for flat, three for sharded). The patch-chain depth depends on the workload: a document that is always `Put` (full snapshot replacement) is one decode; a document that is `Patch`-ed N times since the last `Put` is N+1 decodes plus N JSON Patch applications.

The bench:

```go
func benchGet(b *testing.B, layout ledgerdbsdk.StreamLayout) {
    client, _ := setupRepo(b, layout, ledgerdbsdk.HistoryModeAppend)
    ctx := context.Background()

    b.StopTimer()
    keys := make([]string, readCorpusSize)
    for i := 0; i < readCorpusSize; i++ {
        keys[i] = fmt.Sprintf("doc-%05d", i)
        if _, err := client.Put(ctx, benchCollection, keys[i], samplePayload(i)); err != nil {
            b.Fatalf("seed put: %v", err)
        }
    }
    rng := rand.New(rand.NewSource(1))
    b.ReportAllocs()
    b.StartTimer()

    for i := 0; i < b.N; i++ {
        key := keys[rng.Intn(readCorpusSize)]
        if _, err := client.Get(ctx, benchCollection, key); err != nil {
            b.Fatalf("get %s: %v", key, err)
        }
    }
}
```

(`bench/reads_test.go:30-52`)

`readCorpusSize` defaults to 1,000 documents but is overridable via `BENCH_READ_CORPUS` (`reads_test.go:19`). Each iteration picks a uniform random key from the seeded corpus and reads it. Because the seed phase only does `Put`s, every read resolves a one-tx stream — there is no patch chain to replay. That makes the bench a baseline for "best-case read cost given an already-warm Git pack cache." Workloads with deep patch chains will pay proportionally more per read.

The benches expose the layout dimension (`BenchmarkGetFlat` vs `BenchmarkGetSharded`) so the cost of the additional tree walk for sharded layouts is measurable. At 1,000 documents both layouts perform similarly; the sharded path costs more per-read in tree walks but each tree is smaller, so the effects roughly cancel. At higher cardinalities the picture changes: the flat layout's collection tree becomes a large blob that has to be parsed for every read, while the sharded layout's per-level trees stay small. The crossover matches the write-side crossover discussed in [Performance Write Throughput](Performance-Write-Throughput).

To exercise larger corpora, override `BENCH_READ_CORPUS`:

```sh
BENCH_READ_CORPUS=10000 go test -bench=BenchmarkGet -benchtime=1x ./bench
BENCH_READ_CORPUS=100000 go test -bench=BenchmarkGet -benchtime=1x ./bench
```

The 1x benchtime keeps the seed phase from running multiple times. Issue #19 in the project tracker calls out the 10k/100k tier explicitly as the next bench dimension to characterize.

## Why query through the sidecar

`Client.Get` is the right path for point lookups by document ID. It is the wrong path for everything else — filter by attribute, aggregate by field, range scan by timestamp, full-text search. The Git object store is content-addressed, not value-indexed; there is no efficient way to ask "give me all documents where `status = 'open'`" without reading every document in the collection.

That is what the SQLite sidecar is for. `ledgerdb index watch` materializes every committed transaction into a SQLite database (`internal/infra/sqliteindex/`), one row per document version, with declared columns extracted from the payload according to the schema. Queries run against that database through `ledgerdb query` and `ledgerdb query explain`, using SQLite's standard SQL surface plus the indexes declared on the collection.

The cost model splits:

- **Direct read** — single document by ID. Cheap, no sidecar dependency, freshness is "whatever is in the local Git working set." Latency is bounded by tree walk plus patch-chain replay.
- **Sidecar query** — anything filter- or aggregate-shaped. Cheap *per query* once the sidecar is caught up, expensive to keep the sidecar caught up, freshness is bounded by the watcher's polling interval.

The freshness boundary is the operational tradeoff. A direct read after a local write sees the write immediately. A sidecar query after a local write sees the write only after `index watch` has applied it to the SQLite database, which happens on the next pass — bounded by `--interval` (default 5s in `internal/cli/commands.go:577`).

## Sidecar catch-up cost

When `index watch` starts against a populated repository with no prior sidecar (or with a stale sidecar that fell out of sync), it walks the commit history from the recorded state forward and applies every transaction to the database. The cost is approximately linear in `(commits since recorded state) × (txs per commit)`, with constants depending on operation mix and declared index count.

The bench:

```go
func benchIndexSync(b *testing.B, layout ledgerdbsdk.StreamLayout) {
    ctx := context.Background()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        b.StopTimer()
        client, _ := setupRepo(b, layout, ledgerdbsdk.HistoryModeAppend)
        for j := 0; j < syncCommitCount; j++ {
            docID := fmt.Sprintf("doc-%05d", j)
            if _, err := client.Put(ctx, benchCollection, docID, samplePayload(j)); err != nil {
                b.Fatalf("seed put: %v", err)
            }
        }
        b.StartTimer()

        if _, err := client.SyncIndex(ctx); err != nil {
            b.Fatalf("sync: %v", err)
        }
    }
}
```

(`bench/sync_test.go:16-35`)

`syncCommitCount` is 200 (`sync_test.go:14`). Each iteration:

1. Stops the timer.
2. Builds a fresh repo with 200 Put commits.
3. Starts the timer.
4. Runs one `SyncIndex` call that applies all 200 commits to the empty sidecar.

The reported `ns/op` is therefore the cost of applying 200 commits to a fresh SQLite database. Both layouts are exercised because the watcher's commit walk reads the per-document streams, and the layout affects how those streams are traversed.

To characterize larger histories, edit `syncCommitCount` or fork the bench. The expectation is roughly linear scaling — the per-tx work is bounded and the SQLite transaction commit cost amortizes well when `--batch-commits` is set higher than 1. The default `--batch-commits=1` (`commands.go:582`) is conservative; raising it to 64 or 256 amortizes the SQLite fsync across more applied transactions, which is the most common catch-up tuning lever (see [Performance Tuning Knobs](Performance-Tuning-Knobs)).

The end-to-end story: a fresh replica needs to catch up the full history from upstream before queries return correct results. For a repository with hundreds of thousands of commits, that is minutes to hours. Two tools shorten the window: `maintenance snapshot` (collapses the patch chain so the catch-up starts from a snapshot rather than the full history, see `docs/PERFORMANCE.md` §5) and `index watch --fast` (relaxes SQLite durability during catch-up, see below).

## `--fast` and `--mode`

Two flags on `index watch` change the catch-up cost shape.

**`--fast`** (`internal/cli/commands.go:583`) opens the SQLite database with relaxed durability options (`internal/infra/sqliteindex.OpenWithOptions{Fast: true}`). Concretely this sets `PRAGMA synchronous=OFF` and similar tweaks that trade durability for throughput. A crash mid-catch-up loses some applied transactions, which is fine because the watcher will re-apply them on restart from the recorded state. The flag is intended for catch-up scenarios only: long-running replicas should not stay in fast mode because a crash then leaves the sidecar in an inconsistent state relative to the recorded high-water mark. Flip to fast, catch up, drop back to default, restart.

**`--mode state|history`** (`internal/cli/commands.go:584`) picks between two source-of-truth strategies for the sync. `state` mode (the default) consults the materialized state tree at the head commit and applies the diff between the recorded state hash and the head's state hash. This is O(documents changed since last apply), which is fast when only a small fraction of the corpus has changed. `history` mode walks the full commit chain since the last recorded commit and applies each transaction in order. This is O(commits since last apply).

The right choice depends on the workload. A replica that is nearly caught up wants `state` mode — the per-pass cost is proportional to the actual delta. A replica that is far behind wants `history` mode for the catch-up window because the state-diff path effectively re-builds the entire current state, while the history walk processes only the transactions that actually happened. The watcher will fall back automatically when state mode is unavailable: see `service.go:79-90`, which catches `ErrStateUnavailable` and retries through `syncHistory`.

For a fresh replica with a long upstream history, the typical posture is `--mode history --fast --batch-commits 256`. The first two relax durability and pick the appropriate walk strategy; the third amortizes SQLite commit cost across more applied transactions. Drop back to defaults once caught up.

## Declared indexes

Collection schemas can declare secondary indexes via `ledgerdb schema` (see `internal/domain/index.go` and the CLI command surface). Each declared index is materialized in the SQLite database by `EnsureIndexes` (`internal/app/index/ports.go:35`), called from `materializeIndexes` (`internal/app/index/service.go:424-436`) the first time the watcher encounters each collection per sync pass.

Indexes change the cost model in two ways. At apply time, every PUT/PATCH for an indexed collection has to update each declared index — more indexes means more SQLite work per applied tx. At query time, indexed columns enable point lookups and range scans rather than sequential scans — more indexes means faster queries against the indexed columns.

The tradeoff is the conventional one for any indexed store. Add an index when the queries against the column are frequent enough that the per-query savings exceed the per-write cost. Composite indexes (multiple columns) and unique-modifier indexes are supported as of commit `cdc4a85` ("feat(collection): support composite indexes with unique modifier (#120)"); the unique modifier additionally enforces a uniqueness constraint at apply time, which catches duplicate-id problems at the watcher rather than at query time.

The bench harness does not currently parameterize index count. Workloads with many indexes will see `BenchmarkIndexSync` numbers grow proportionally; that is expected and is the cost of trading apply-time work for query-time speed.

## When the sidecar is the wrong answer

Two scenarios push back the other way.

First, a replica that does not need queries does not need a sidecar. Pure ingest-and-mirror deployments (this replica receives writes, pushes to upstream, never asks "find me documents matching X") can skip `index watch` entirely and save the catch-up cost and the per-tx apply cost. The cost of querying the Git object store directly via `Client.Get` is bounded for point lookups, and the absence of a sidecar is one less moving piece.

Second, a query that is fundamentally a scan over the entire corpus is no faster through SQLite than through a direct walk. If the query is "give me all documents in the collection," the sidecar has to read every row anyway. The win from the sidecar comes from indexes that turn the scan into a point lookup or a narrow range scan. Workloads that always full-scan see less benefit.

The decision lives one level above LedgerDB: does the application need filter/aggregate access? If yes, the sidecar is the right answer and the catch-up cost is the price. If no, direct reads are sufficient and the watcher is unnecessary overhead.

## See also

- [Performance Overview](Performance-Overview)
- [Performance Write Throughput](Performance-Write-Throughput)
- [Performance Tuning Knobs](Performance-Tuning-Knobs)
- [Performance Bench Harness](Performance-Bench-Harness)
- [Querying and Indexing Strategy](Concepts-Indexing)
- [Observability Metrics](Observability-Metrics) — the `ledgerdb_index_sync_duration_seconds` histogram exposed by `index watch`.
- `bench/reads_test.go` — direct-read benches.
- `bench/sync_test.go` — sidecar catch-up benches.
- `internal/app/index/service.go` — the sync loop, the state vs history strategies, and the index materialization path.
- `internal/infra/sqliteindex/` — the sidecar implementation and `OpenWithOptions{Fast: true}`.
- `docs/PERFORMANCE.md` §4 (sync interval tuning), §5 (snapshot threshold).
