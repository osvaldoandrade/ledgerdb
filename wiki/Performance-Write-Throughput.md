# Performance: Write Throughput

Writes in LedgerDB are the most expensive operation the system performs. Every `put`, `patch`, or `delete` ends in a Git commit on `refs/heads/main`, every commit pays for transaction encoding plus blob writes plus tree updates plus a compare-and-swap against the ref, and the entire path is serialized by the CAS guard. This page covers what the bench harness measures for that path, the role CAS contention plays in scaling concurrent writers, how `--history-mode amend` and `--sync=false` shift the cost shape, and how to reason about the numbers your own bench run produces.

## What this page covers

- The bench files that exercise the write path: `bench/writes_test.go` (full SDK round-trip) and `internal/infra/gitrepo/tx_store_bench_test.go` plus `tx_store_concurrency_test.go` (the CAS retry layer).
- What each benchmark actually measures, step-by-step.
- How layout (`flat` vs `sharded`) and history mode (`append` vs `amend`) move per-iteration cost.
- The mechanics of CAS contention: when it appears, how the retry schedule absorbs it, and when it surfaces as `ErrHeadChanged` to the caller.
- The effect of `--sync` (auto-push) on the write hot path.
- Why this page does not publish absolute throughput numbers.

## What this page does not cover

- Auto-fetch latency. The benches disable `AutoSync` because the repos have no remote (`bench/bench_test.go:48`); the cost of `git fetch` before a write is real but workload-dependent and not exercised here.
- Signing cost. The benches do not sign. See `docs/PERFORMANCE.md` §3 for the cost-by-backend table (SSH 30-60ms, GPG 50-100ms, HSM 100-400ms per signed commit).
- Network push latency. Auto-push runs only when `--sync` is enabled and a remote is configured; the benches have neither.

## `bench/writes_test.go`

The file declares four `Benchmark*` entry points wrapping two parameterized helpers:

```go
func BenchmarkPutFlat(b *testing.B)    { benchPut(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPutSharded(b *testing.B) { benchPut(b, ledgerdbsdk.StreamLayoutSharded) }

func BenchmarkPatchFlat(b *testing.B)    { benchPatch(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPatchSharded(b *testing.B) { benchPatch(b, ledgerdbsdk.StreamLayoutSharded) }
```

(`bench/writes_test.go:63-67`)

Each helper iterates over both history modes (`append` and `amend`) and runs the inner loop as a Go sub-benchmark. So a single `go test -bench=. ./bench` invocation produces eight measured combinations: layout × history-mode × {Put, Patch}.

The Put loop is straightforward (`writes_test.go:30-35`):

```go
for i := 0; i < b.N; i++ {
    docID := fmt.Sprintf("doc-%d", i)
    if _, err := client.Put(ctx, benchCollection, docID, samplePayload(i)); err != nil {
        b.Fatalf("put: %v", err)
    }
}
```

Each iteration creates a new `doc-N` and writes a fresh snapshot (`{"i":N,"name":"bench","tags":["a","b"]}`). Because the document IDs are distinct per iteration, there is no CAS contention with prior iterations (the parent-hash check for each doc is trivially satisfied on first write), but every iteration still races against itself through the single `refs/heads/main` ref — though sequentially within one goroutine, so the race never materializes. The bench measures the uncontested write cost end-to-end.

The Patch loop seeds one document and patches it repeatedly (`writes_test.go:46-58`):

```go
const docID = "patch-target"
if _, err := client.Put(ctx, benchCollection, docID, samplePayload(0)); err != nil {
    b.Fatalf("seed put: %v", err)
}

b.ReportAllocs()
b.ResetTimer()
for i := 0; i < b.N; i++ {
    if _, err := client.Patch(ctx, benchCollection, docID, patchOps(i+1)); err != nil {
        b.Fatalf("patch: %v", err)
    }
}
```

The seed Put is excluded from the measurement window via `b.ResetTimer()`. Each measured iteration applies a JSON Patch replacing the `i` field. The patch path is more expensive than the put path because the SDK must load the prior document state from the Git store before applying the patch — see `internal/app/doc/` for the patch service. The bench captures both paths so the delta is measurable.

The SDK client is constructed with `AutoSync: false` and `AutoWatch: false` (`bench_test.go:48-58`), which means: no `git fetch` before the write, no auto-push after, and no background `index watch` running. The bench measures the local write path only. A production deployment with `--sync` enabled adds network round-trips on every write; the bench does not measure that.

## What each iteration pays for

A single `client.Put` iteration in the bench walks roughly this sequence:

1. **Canonicalize the payload.** `internal/infra/canonicaljson` normalizes the JSON byte representation so that semantically equal documents hash identically. Cost: bounded by document size, typically microseconds for a small JSON object.
2. **Encode the transaction.** `internal/infra/txv3.Encoder` produces the protobuf representation of the `domain.Transaction`. Cost: again microseconds.
3. **Compute the tx hash.** `internal/infra/hash.SHA256` over the encoded bytes.
4. **Write the tx blob.** Stored as a Git blob object under the object store. Cost: one filesystem write plus a `fdatasync` depending on the Git options.
5. **Walk and update the stream tree.** For `flat` layout, the document lives at `collections/<col>/<doc-id>.pb`, one tree level deep. For `sharded`, it lives at `collections/<col>/<aa>/<bb>/<doc-id>.pb`, three tree levels deep. Every level of the tree that contains the new entry must be rewritten as a new tree object.
6. **Write the commit.** Includes the new root tree, the parent commit hash, the author/committer metadata, and a synthesized message containing the `tx_id`.
7. **CAS on `refs/heads/main`.** `tx_store.go:139` enters the retry loop; on success the ref advances atomically; on conflict the loop sleeps the jittered backoff and retries.

The bench measures the wall-clock cost of all seven steps per iteration. The dominant step varies: for `flat` layouts on small collections, steps 2-4 dominate (the tree is small, encoding and blob I/O are most of the work); for `sharded` layouts and larger collections, step 5 grows (more tree levels to rewrite, but each level is smaller so the work per level is bounded); for any write under contention, step 7 dominates.

## Layout and history mode

Sharded layouts cost more per write on a fresh repository because they materialize more tree objects: three levels instead of one. The per-write cost is bounded (each level rewrites a small tree), but it is measurably higher than the flat path on the first few writes. The crossover comes from collection cardinality, not per-write cost: at low document counts the flat tree is cheap to rewrite because it has few entries; at high document counts the flat tree becomes a large blob that is rewritten on every commit, and the per-write cost balloons. The `docs/PERFORMANCE.md` §1 guidance of "use flat below 10k docs, sharded above" captures this empirically; the bench validates the *shape* of the curve, not a hard threshold.

History mode flips a different lever. `append` mode (`internal/domain/config.go:42`) creates a new commit for every write that references its parent, building a chain. `amend` mode rewrites the head commit so only the latest state is reachable from `refs/heads/main`. The amend path saves on storage growth (older commits become unreachable and are eventually collected by `git gc`) and reduces `git log` walking cost for downstream consumers, but it does the same amount of object-store work per write — the savings show up in the long-tail GC cost and the integrity-verify cost (`docs/PERFORMANCE.md` §2), not in the per-iteration bench number.

The bench prints both modes' numbers as sub-benchmarks so the operator can compare directly. The expectation is that `append` and `amend` are within a few percent of each other on per-write cost; the divergence is in the steady-state storage profile, not the per-write hot path.

## CAS contention

The CAS layer is the part of the write path that does not scale linearly with writers. The retry policy is two compile-time constants in `internal/infra/gitrepo/tx_store.go:31-32`:

```go
casMaxRetries  = 5
casBackoffBase = 25 * time.Millisecond
```

Each retry doubles the base delay and adds full jitter (`jitteredBackoff` at `tx_store.go:276-283`). The worst-case cumulative wait is 25 + 50 + 100 + 200 + 400 = 775ms; the average is roughly half of that. After exhausting the budget, the loser surfaces `domain.ErrHeadChanged` to the caller (`tx_store.go:188-192`).

Two benches exercise this layer directly.

`BenchmarkFixedBackoffSchedule` and `BenchmarkJitteredBackoffSchedule` in `internal/infra/gitrepo/tx_store_bench_test.go` measure the wait pattern in isolation, without surrounding Git operations. The fixed-schedule worst case is the 775ms total above; the jittered schedule averages to roughly half. Both report `worst_us` / `sample_us` as `b.ReportMetric` values. These benches are useful for understanding the bound, not for measuring throughput.

`BenchmarkCASContention` in `internal/infra/gitrepo/tx_store_concurrency_test.go:348-405` is the one that matters operationally. It uses `b.RunParallel` to drive concurrent writers against a single shared `doc_id`, with a `cascadeRetryCounter` recording every CAS retry attempt:

```go
b.RunParallel(func(pb *testing.PB) {
    id := writerSeq.Add(1)
    j := int64(0)
    for pb.Next() {
        // ... build and PutTx a patch against the hot doc ...
        if errors.Is(err, domain.ErrHeadChanged) {
            // refresh head and continue
        }
    }
})
b.ReportMetric(float64(counter.total()), "cas_retries")
```

The reported `cas_retries` metric is the number of times the loop entered backoff, summed across all writers. A run with `-benchtime=10s -cpu=8` typically shows tens to thousands of retries depending on the host's scheduling; the throughput in `ns/op` reflects the effective write cost *after* losers backed off and retried.

The complementary test `TestCASRetryDistribution_Ramp` (`tx_store_concurrency_test.go:139-343`) walks a ramp of 1, 4, 16, 32, 64 concurrent writers across two scenarios — each writer to its own document (low contention on the parent-hash check, high contention on the ref) and all writers to a shared "hot" document (high contention on the parent-hash check, which short-circuits before the CAS loop runs). The test logs a per-attempt histogram so the operator can see how the retry distribution shifts as concurrency grows. With one writer the retry count is zero on both scenarios (asserted); with sixteen or more writers at least one retry appears on at least one path (asserted).

The operational takeaways:

- A single-writer workload is uncontended. CAS retries are irrelevant.
- Concurrent writers against disjoint documents pay CAS-ref contention only. The retry budget absorbs realistic contention up to several dozen writers; beyond that, surfacing `ErrHeadChanged` to the application is normal and the application should retry at its own layer.
- Concurrent writers against the same document fail fast: the parent-hash check rejects most attempts before the CAS loop runs, and the application sees `ErrHeadChanged` quickly. This is correct — two writers cannot both legitimately produce a patch from the same parent state.
- Raising `casMaxRetries` to 10 (a compile-time change) increases the contention budget at the cost of longer worst-case latency. The `docs/PERFORMANCE.md` §6 guidance is that a retry count of 10 with the same base delay absorbs realistic ingestion bursts.

## `--sync=false` and the write hot path

The root command's persistent `--sync` flag (`internal/cli/root.go:69`) controls whether each write auto-fetches before and auto-pushes after. With `--sync=true` (the default), every write that touches a repo with a configured remote does:

1. `git fetch` to pull any new upstream commits.
2. The full local write path above.
3. `git push` to publish the new commit.

The fetch and push add network round-trips. For a workstation against a local-network remote, that is single-digit-to-tens of milliseconds per round-trip. For a remote across the public internet, it can be hundreds of milliseconds.

Disabling auto-sync (`--sync=false` or `LEDGERDB_AUTO_SYNC=false`) removes both round-trips and turns each write into a local-only operation. The cost shape collapses to what the benches measure. The tradeoff is that the local repo can diverge from the remote between explicit `ledgerdb push` invocations, which is fine for ingestion patterns that batch writes and push periodically and problematic for patterns that expect every write to be immediately visible cluster-wide.

The bench harness sets `AutoSync: false` unconditionally because the temp-dir repos have no remote (`bench/bench_test.go:48`). To measure the auto-sync overhead, the operator needs to configure a remote (loopback or otherwise) and re-run with `AutoSync: true`. The bench files do not currently parameterize this.

## How `--history-mode amend` changes the picture

In `append` mode, every write extends the commit chain. The local Git history grows linearly with write count, the `git log` walking cost grows linearly, and the `git gc` budget grows accordingly.

In `amend` mode, every write rewrites the head commit. The chain stays short (one commit deep from any anchor point), the old commits become unreachable from `refs/heads/main`, and `git gc` collects them on the next maintenance pass. The per-write *bench* cost is comparable to append; the savings appear in:

- `git log` walking cost on downstream consumers (constant rather than linear).
- Storage growth (bounded by the current document set rather than the full history).
- Integrity verify cost (walks the tree only, not the chain — see `docs/PERFORMANCE.md` §2).
- Replica catch-up cost (one fast-forward rather than N commits to apply).

The cost is that the audit trail is destroyed. For regulated workloads this is unacceptable; for cache-like or scratchpad collections it is the right tradeoff. The setting is per-collection at creation time (`ledgerdb collection create --history amend`) and is recorded in the manifest at `internal/domain/manifest.go:12`.

## Why this page does not publish absolute numbers

The benches print `ns/op`, `B/op`, and `allocs/op` per iteration. Converting to "X writes per second" requires multiplying by the goroutine count and accounting for the cost of the bench harness itself (the spinner, the temp-dir setup, the SDK client lifecycle). The harness is not currently tuned for that — `setupRepo` runs inside each benchmark invocation, the temp-dir is on whatever filesystem `b.TempDir()` resolves to, and there is no pinned-CPU or fixed-frequency posture.

Where `docs/PERFORMANCE.md` cites concrete numbers, this page references them with a citation rather than restating them. The signing-cost table at `docs/PERFORMANCE.md` §3 is the canonical reference for the per-commit signing overhead under SSH/GPG/HSM. Everything else is "run the bench on your hardware."

The path to published numbers is Epic [#8](https://github.com/osvaldoandrade/ledgerdb/issues/8). Until that lands, the bench harness is reproducible methodology, not a leaderboard.

## See also

- [Performance Overview](Performance-Overview)
- [Performance Read And Query](Performance-Read-And-Query)
- [Performance Tuning Knobs](Performance-Tuning-Knobs)
- [Performance Bench Harness](Performance-Bench-Harness)
- [Observability Metrics](Observability-Metrics) — the `ledgerdb_cas_retries_observed_total` counter exposed by `index watch`.
- `bench/writes_test.go` — the end-to-end write bench.
- `internal/infra/gitrepo/tx_store.go` — the CAS loop and the retry budget.
- `internal/infra/gitrepo/tx_store_bench_test.go` — backoff schedule micro-benches.
- `internal/infra/gitrepo/tx_store_concurrency_test.go` — the CAS retry ramp test and `BenchmarkCASContention`.
- `docs/PERFORMANCE.md` §3 (signing cost), §6 (CAS retry tuning).
