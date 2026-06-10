# Performance: Bench Harness

The bench harness lives under `bench/` and is a set of Go testing-package benchmarks that drive the public SDK (`pkg/ledgerdbsdk`) end-to-end against a fresh repository in `b.TempDir()`. This page is the operator's reference for what each bench measures, how to run it, how to read the output, and what variance to expect.

The point of running these is to understand the *shape* of the cost on your hardware — how does write cost change with layout? what does sidecar catch-up cost per commit? — rather than to reproduce an absolute throughput number. The harness does not currently pin CPU frequency, drop page cache between iterations, or include auto-push round-trips, so absolute figures are not comparable across hosts.

## What this page covers

- The five bench files under `bench/` and what each one exercises.
- The complementary benches under `internal/infra/gitrepo/` for the CAS retry layer.
- How to run each bench with `go test -bench`.
- How to interpret the `ns/op`, `B/op`, `allocs/op`, and custom-metric output.
- Variance sources and the recommendation to take medians across runs.
- Where pprof captures go if you wire them in.

## What this page does not cover

- A leaderboard of absolute throughput numbers. See [Performance Overview](Performance-Overview) for why this section publishes methodology rather than figures.
- The full Go testing-package documentation. Familiarity with `go test -bench=.`, `-benchtime`, and `-count` is assumed.
- Production load testing. The harness measures Go-level per-operation cost; production load testing typically involves a separate harness that simulates real traffic patterns and includes the auto-sync round-trips.

## The bench files

### `bench/bench_test.go` — the harness

No `Benchmark*` functions of its own. Provides `setupRepo` (line 28), which initializes a fresh repo with a configurable `StreamLayout` and `HistoryMode`, opens the SQLite index, and returns an opened SDK client. The client is closed via `b.Cleanup`. `AutoSync` is disabled because the bench repos have no remote configured (line 48). `AutoWatch` is disabled because the bench drives `SyncIndex` explicitly.

Every other bench file under `bench/` calls `setupRepo` to acquire a client, so the per-iteration cost of any of those benches includes whatever cost `setupRepo` itself imposes — repo init, manifest write, sidecar open. For benches that run inside an `b.StopTimer()` / `b.StartTimer()` boundary (the snapshot and sync benches), the setup is excluded from the measurement window; for the write and read benches, it runs once before the loop and is amortized.

### `bench/writes_test.go` — write throughput

Four entry points, each running both `append` and `amend` history modes as sub-benchmarks:

```go
func BenchmarkPutFlat(b *testing.B)    { benchPut(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPutSharded(b *testing.B) { benchPut(b, ledgerdbsdk.StreamLayoutSharded) }
func BenchmarkPatchFlat(b *testing.B)    { benchPatch(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPatchSharded(b *testing.B) { benchPatch(b, ledgerdbsdk.StreamLayoutSharded) }
```

`benchPut` writes a fresh document per iteration. `benchPatch` seeds one document and patches it repeatedly. Both measure the full local write path: encode, blob write, tree update, commit, CAS (uncontested in this single-goroutine bench).

Detailed coverage: [Performance Write Throughput](Performance-Write-Throughput).

### `bench/reads_test.go` — direct-read throughput

Two entry points:

```go
func BenchmarkGetFlat(b *testing.B)    { benchGet(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkGetSharded(b *testing.B) { benchGet(b, ledgerdbsdk.StreamLayoutSharded) }
```

Seeds `BENCH_READ_CORPUS` documents (default 1,000, overridable via env var at `bench/reads_test.go:19`), then performs uniform-random key lookups against the populated corpus. Measures the cost of `Client.Get` end-to-end: tree walk to locate the stream, blob read, tx decode.

Detailed coverage: [Performance Read And Query](Performance-Read-And-Query).

### `bench/snapshot_test.go` — snapshot compaction

One entry point:

```go
func BenchmarkSnapshotCompaction(b *testing.B)
```

Runs as three sub-benchmarks at chain depths of 5, 50, and 500. Each iteration:

1. Stops the timer.
2. Builds a fresh repo, seeds one document, patches it `depth` times.
3. Starts the timer.
4. Runs `maintenance.SnapshotService.Snapshot` with threshold 1, which collapses the entire chain into one snapshot commit.

Uses the in-module `internal/app/maintenance` service directly because the SDK does not yet expose a `Snapshot` method (`TODO(#27)`, see `bench/snapshot_test.go:29`).

### `bench/sync_test.go` — sidecar catch-up

Two entry points:

```go
func BenchmarkIndexSyncFlat(b *testing.B)    { benchIndexSync(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkIndexSyncSharded(b *testing.B) { benchIndexSync(b, ledgerdbsdk.StreamLayoutSharded) }
```

Each iteration pre-populates 200 commits (`syncCommitCount` at line 14), then times one `SyncIndex` call that applies all 200 to a fresh sidecar. The reported `ns/op` is therefore the cost of applying 200 commits, not one — divide by 200 to get per-commit cost.

Detailed coverage: [Performance Read And Query](Performance-Read-And-Query).

### `internal/infra/gitrepo/tx_store_bench_test.go` — backoff schedule

Two micro-benches:

```go
func BenchmarkFixedBackoffSchedule(b *testing.B)
func BenchmarkJitteredBackoffSchedule(b *testing.B)
```

Measure the cumulative wait across all CAS attempts under the fixed and jittered schedules. Both report custom metrics (`worst_us` and `sample_us` respectively) via `b.ReportMetric`. Useful for understanding the retry-budget bound, not for measuring throughput.

### `internal/infra/gitrepo/tx_store_concurrency_test.go` — CAS contention

```go
func BenchmarkCASContention(b *testing.B)
func TestCASRetryDistribution_Ramp(t *testing.T)
```

The benchmark drives `PutTx` against a shared "hot" stream via `b.RunParallel`, with the `casRetryHook` installed to count retries. Reports `ns/op` plus a custom `cas_retries` metric.

The test (not a benchmark, but operationally informative) walks a ramp of 1, 4, 16, 32, 64 concurrent writers across two scenarios — each writer to its own document and all writers to a shared hot document — and logs a per-attempt histogram. Run with `go test -v` to see the histogram in the test output.

Detailed coverage: [Performance Write Throughput](Performance-Write-Throughput).

## Running the benches

The standard invocations:

```sh
# All benches under bench/ with default benchtime (1s per bench).
go test -bench=. ./bench

# Reads only, larger corpus.
BENCH_READ_CORPUS=10000 go test -bench=BenchmarkGet -benchtime=1x ./bench

# Writes only, longer benchtime for stable numbers.
go test -bench=BenchmarkPut -benchtime=5s ./bench

# CAS contention with explicit parallelism.
go test -bench=BenchmarkCASContention -benchtime=10s -cpu=8 ./internal/infra/gitrepo

# Backoff micro-benches.
go test -bench=BackoffSchedule -benchtime=1000000x ./internal/infra/gitrepo
```

A few conventions worth knowing:

- `-bench=<pattern>` is a regular expression matched against function names.
- `-benchtime=Ns` runs each bench for at least N seconds. `-benchtime=Nx` runs each bench exactly N iterations. The latter is the right form for benches that have expensive setup inside the iteration loop (like the sync and snapshot benches) where you want a fixed number of measured iterations.
- `-count=N` repeats each bench N times, which is useful for taking medians.
- `-cpu=N1,N2,...` runs each bench at each `GOMAXPROCS` value, which matters for `b.RunParallel` benches.
- `-v` is informative for benches that emit `t.Logf` or `b.Logf` lines (the CAS ramp test in particular).

## Reading the output

A typical `go test -bench=BenchmarkPutFlat -benchtime=2s ./bench` output:

```
goos: darwin
goarch: arm64
pkg: github.com/osvaldoandrade/ledgerdb/bench
BenchmarkPutFlat/append-12       320      6248312 ns/op    1542384 B/op    18432 allocs/op
BenchmarkPutFlat/amend-12        318      6291201 ns/op    1538920 B/op    18411 allocs/op
PASS
ok      github.com/osvaldoandrade/ledgerdb/bench  4.872s
```

Field-by-field:

- `BenchmarkPutFlat/append-12` — bench name, sub-bench, and `GOMAXPROCS` (12).
- `320` — number of iterations completed in the benchtime window.
- `6248312 ns/op` — average wall time per iteration in nanoseconds (about 6.2ms here).
- `1542384 B/op` — average bytes allocated per iteration (about 1.5MB; this is dominated by Git object encoding and tree rewrites).
- `18432 allocs/op` — average allocation count per iteration.

To convert `ns/op` to ops/second: `1e9 / ns_per_op`. A bench at 6.2ms/op is roughly 161 ops/sec single-threaded on this hardware. Multiply by the realistic concurrent-writer count for the workload, then subtract the CAS contention overhead at that concurrency (see [Performance Write Throughput](Performance-Write-Throughput)).

For the benches that emit custom metrics:

```
BenchmarkCASContention-8    1024    1184281 ns/op    cas_retries=287
```

The `cas_retries=287` is the total retries observed across all parallel writers during the bench run. Divide by `b.N` (iterations) to get retries per write, or read as a raw count to understand whether the contention is dominating the cost.

The snapshot bench output looks like:

```
BenchmarkSnapshotCompaction/depth=5-12      152    7821432 ns/op    ...
BenchmarkSnapshotCompaction/depth=50-12      48   24123871 ns/op    ...
BenchmarkSnapshotCompaction/depth=500-12      5  201932103 ns/op    ...
```

The depth scales by 10x at each step; the cost scales sub-linearly because the snapshot path reads the final state once and writes one commit regardless of chain depth. The chain-walk cost is the linear component.

## Variance

Variance per iteration is dominated by:

- **Filesystem timing.** SQLite WAL fsync and Git object-store writes are syscall-bound. Background filesystem activity, page-cache state, and SSD garbage collection all introduce variance.
- **Go runtime scheduling.** GC pauses, goroutine scheduling, and OS scheduler decisions add noise on the order of tens of microseconds per iteration.
- **Temperature throttling.** Sustained benches on laptops can trigger thermal throttling, which shows up as throughput degradation late in long runs.

A reasonable discipline is to run any bench at least three times and take the median. The `-count=N` flag does this in one invocation:

```sh
go test -bench=BenchmarkPut -benchtime=2s -count=5 ./bench
```

The output produces five lines per bench, which you can median manually or pipe through `benchstat` (`go install golang.org/x/perf/cmd/benchstat@latest`) for statistical summaries and across-run deltas.

For benches with significant setup-per-iteration cost (the snapshot and sync benches), `-benchtime=1x` is often the right choice to keep total wall-clock bounded while still getting meaningful per-iteration timings. Run the iteration count up explicitly:

```sh
for i in 1 2 3 4 5; do
  go test -bench=BenchmarkIndexSyncFlat -benchtime=1x ./bench 2>&1 | grep ns/op
done
```

Then median the five lines by hand.

## pprof captures

The current bench harness does not include built-in pprof captures. To profile a specific bench, use the standard Go test flags:

```sh
go test -bench=BenchmarkPutSharded -benchtime=5s \
  -cpuprofile=/tmp/cpu.pb.gz \
  -memprofile=/tmp/mem.pb.gz \
  -blockprofile=/tmp/block.pb.gz \
  -mutexprofile=/tmp/mutex.pb.gz \
  ./bench
```

Then read with `go tool pprof`:

```sh
go tool pprof -top -cum /tmp/cpu.pb.gz
go tool pprof -top -cum /tmp/mutex.pb.gz
```

The conventions are the standard Go ones. `-cum` ranks by cumulative samples (function plus everything it called), which is usually what you want for finding bottlenecks. `-flat` ranks by samples in the function itself, which is useful when a single leaf is the suspected hot spot.

A typical CPU profile for `BenchmarkPutSharded` will show time distributed across: protobuf encode (`internal/infra/txv3`), canonical JSON encode (`internal/infra/canonicaljson`), Git object hashing (SHA-1 plus SHA-256 for tx hash), tree rewrites in `go-git`, and SQLite work in the index sync path if running through the SDK end-to-end. No single function should dominate above roughly 20-30%; if one does, that is the next investigation target.

A mutex profile is most informative for the CAS contention bench. Run it under `BenchmarkCASContention` and look for stacks in `internal/infra/gitrepo/tx_store.go`.

## What to do when numbers do not match expectations

The most common diagnostic shape is "my throughput dropped after a code change." The investigation order:

1. Re-run the bench three to five times. Variance is real and a single low number is often noise.
2. Compare the `B/op` and `allocs/op` columns. A regression in allocations is often the precursor to a regression in `ns/op`; finding the new allocation site with a memprofile is usually the next step.
3. Take a CPU profile under the regressed bench. Look for new entries in the top-cum view that were not there before.
4. Take a mutex profile if the regression is concurrency-shaped. New contention almost always shows up as a new top-cum entry.

The most common diagnostic shape for a fresh deployment is "my numbers are lower than the documentation suggests." The most likely cause is hardware: a workstation on a slow SSD will see write benches at single-digit ms/op while a server with a fast NVMe will see sub-millisecond. The ratios between benches (Put vs Patch, flat vs sharded, append vs amend) are more stable across hardware than the absolute numbers; if the ratios are preserved, the system is healthy on your hardware.

If `BenchmarkCASContention` reports zero retries even at high `-cpu` values, the bench is not actually contending — most often because `GOMAXPROCS` is too low or because the host is single-threaded. Bump `-cpu=8` or higher explicitly.

## See also

- [Performance Overview](Performance-Overview)
- [Performance Write Throughput](Performance-Write-Throughput)
- [Performance Read And Query](Performance-Read-And-Query)
- [Performance Tuning Knobs](Performance-Tuning-Knobs)
- `bench/bench_test.go` — the harness and `setupRepo`.
- `bench/writes_test.go`, `bench/reads_test.go`, `bench/snapshot_test.go`, `bench/sync_test.go` — the four bench surfaces.
- `internal/infra/gitrepo/tx_store_bench_test.go` — backoff micro-benches.
- `internal/infra/gitrepo/tx_store_concurrency_test.go` — CAS contention bench and ramp test.
- `docs/PERFORMANCE.md` §7 — the operator-facing benchmark methodology guidance.
