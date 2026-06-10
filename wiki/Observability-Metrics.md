# Observability: Metrics

Metrics are the quantitative pillar in LedgerDB. Where logs tell you what one sync pass did and the audit log tells you which transactions actually committed, metrics tell you how many transactions per second the replica is keeping up with, how far behind upstream it is, how often a sync pass fails, how long a sync pass takes, and how often the CAS loop in the underlying gitrepo store has to retry. They are emitted by exactly one command — `ledgerdb index watch` — and they are emitted only when the operator opts in.

This page documents the five collectors defined in `internal/app/index/metrics.go`, the labels they carry, the rationale for the counter/gauge/histogram choices, the loopback-only-by-default bind policy, how to enable scraping, and how the dashboard at `dashboards/grafana/ledgerdb-watch.json` and the alerts in `docs/ALERTS.md` consume them.

## What this page covers

- The full catalog of metrics emitted by `ledgerdb index watch`.
- The opt-in `--metrics-addr` flag and the `--metrics-allow-public` safety net.
- The private-registry choice and why the metrics layer does not touch the Prometheus default registry.
- The label cardinality discipline (`collection` plus a small bucketed `reason`).
- The relationship between `OnTxApplied`, `OnSyncError`, `OnSyncDuration`, and `OnReplicationFetch` callbacks and the collectors they drive.
- How to scrape these series and what the alerts in `docs/ALERTS.md` express against them.

## What this page does not cover

- Per-CLI-invocation metrics for one-shot commands like `put` or `patch`. There are none; metrics are tied to the long-running `index watch` loop.
- Distributed tracing metrics or RED/USE rollups derived from spans. Tracing is on the roadmap (Epic [#4](https://github.com/osvaldoandrade/ledgerdb/issues/4)) and not in v0.2.x.
- Process-level metrics like Go runtime memory or goroutine counts. The private registry only registers LedgerDB's own collectors; the standard `prometheus.NewGoCollector` is not added, deliberately, so the exposed surface stays minimal and predictable.

## Opt-in: `--metrics-addr`

The flag is declared in `internal/cli/commands.go:585`:

```go
cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "",
    "Bind Prometheus /metrics on host:port (use 'auto' for 127.0.0.1:9090). Empty disables metrics.")
```

The wiring that follows at `commands.go:484-502` constructs the `*Metrics` instance only when the flag is non-empty, calls `metrics.RunServer` to bind the listener, defers a graceful shutdown, and prints the bound address to stderr unless `--quiet` was set. The default is empty, which means no listener, no HTTP server, no Prometheus collectors registered. Zero overhead when off.

The convenience value `auto` resolves to `127.0.0.1:9090` (`metrics.go:154-156`). That is enough for most workstation use; production deployments typically set an explicit `host:port` that aligns with their Prometheus scrape config.

The bind is loopback-only by default. `assertLoopback` at `metrics.go:176-188` accepts only `127.0.0.0/8`, `::1`, and the literal `localhost`. A bind to `0.0.0.0` or to a public interface returns `ErrPublicBindRefused`:

```go
var ErrPublicBindRefused = errors.New("metrics endpoint refusing to bind on non-loopback host; pass --metrics-allow-public to override")
```

(`metrics.go:144`)

That posture matches v0.2.x's overall workstation-class default — LedgerDB does not, by accident, expose its observability surface to the network. Production deployments that scrape from a separate host pass `--metrics-allow-public` explicitly, and the operator who did so is on record having made that decision.

The HTTP server itself is plain `net/http` with a five-second `ReadHeaderTimeout` (`metrics.go:166-171`) and a `promhttp.HandlerFor` bound to the private registry. There is no authentication on `/metrics`. The expectation is that the bind address is reachable only from trusted scrapers (the loopback default makes that the trivial case).

## The private registry

`NewMetrics` constructs a fresh `prometheus.Registry` and registers every collector against it (`metrics.go:41-79`). It never touches `prometheus.DefaultRegisterer`. This matters for three reasons.

First, the binary is a CLI. The same process can in principle invoke `index watch` more than once in a test or a script; default-registry registration would either deduplicate (and silently merge state across runs) or panic on the second registration. The private registry sidesteps both failure modes.

Second, the `/metrics` endpoint serves only LedgerDB's series. There is no `go_*` runtime metric, no `process_*` family, no `promhttp_*` self-instrumentation. Five names appear in the exposition, plus standard Prometheus exposition metadata. A scrape's payload is bounded and the names are stable.

Third, the metrics layer can be embedded by a SDK consumer or a test harness without contaminating the embedder's own Prometheus registry. The `Registry()` accessor at `metrics.go:85-87` is the explicit hook for that case.

The cost of the choice is that some operators expect to see `go_goroutines` and `process_resident_memory_bytes` alongside the application series. They will not. The runtime metrics for a `ledgerdb index watch` are best collected by a node-level exporter alongside it.

## The metric catalog

All five collectors are constructed in `internal/app/index/metrics.go:40-81`. The wire-up to the sync service is one-way: the service calls `OnTxApplied`, `OnSyncError`, `OnSyncDuration`, and `OnReplicationFetch` (defined on the `Observer` interface at `internal/app/index/ports.go:70-75`), and `*Metrics` increments the corresponding collector.

### `ledgerdb_tx_applied_total`

```
# TYPE ledgerdb_tx_applied_total counter
# HELP ledgerdb_tx_applied_total Total transactions applied to the SQLite index by ledgerdb index watch.
ledgerdb_tx_applied_total{collection="orders"} 184217
ledgerdb_tx_applied_total{collection="users"}  3902
```

Defined at `metrics.go:44-50`. Labelled by `collection`. Empty collection names are normalized to the literal string `unknown` in `OnTxApplied` (`metrics.go:90-96`) so the series stays queryable even if a tx slips through with no collection.

The increment happens inside `applyTxs` in `internal/app/index/service.go:360-367`, inside the SQLite transaction but before the commit. Strictly speaking the counter reflects transactions that were applied to the in-memory store-tx; the SQLite commit immediately after either ratifies the batch or rolls it back. Under rollback (rare but possible on a sidecar I/O error), the counter has overcounted by the size of the failed batch. The discrepancy is bounded by `--batch-commits` and is generally not visible in steady state; the counter is a "how busy is the watcher" signal, not an exact ledger of durable applies.

The PromQL pattern is `rate(ledgerdb_tx_applied_total[5m])`, broken out by `collection` with `sum by (collection) (...)`. The Grafana dashboard at `dashboards/grafana/ledgerdb-watch.json` puts this on the first panel.

### `ledgerdb_sync_errors_total`

```
# TYPE ledgerdb_sync_errors_total counter
# HELP ledgerdb_sync_errors_total Total errors encountered while syncing the index, labelled by collection and reason.
ledgerdb_sync_errors_total{collection="_",reason="commit_not_found"} 12
ledgerdb_sync_errors_total{collection="_",reason="fetch_unavailable"} 1
```

Defined at `metrics.go:51-57`. Labelled by `collection` and `reason`. The `reason` is produced by `classifyErr` in `internal/app/index/service.go:95-116`, which buckets known sentinel errors (`ErrCommitNotFound`, `ErrFetchUnavailable`, `ErrMissingDocument`, `ErrPatchUnsupported`, `ErrStateUnavailable`, `ErrMergeCommitUnsupported`, `context.Canceled`/`context.DeadlineExceeded`) into eight low-cardinality strings, with everything else bucketed as `other`. Empty values for both labels are normalized to `_` (`metrics.go:101-109`) so the metric is always present in the exposition.

The deliberate cardinality cap is the point. A naive implementation that used the raw `err.Error()` as the label would explode the time-series count under any failure mode that varies a path or an offset in the error message. The bucketing is what makes the rule `rate(ledgerdb_sync_errors_total[5m]) > 0.1` (the `LedgerDBSyncErrorRateHigh` alert in `docs/ALERTS.md`) safe to deploy on a shared Prometheus.

The increment is called from `SyncService.Sync` (`service.go:54-60`) on the *first* error returned from `syncInternal`, not per failed tx. That keeps the rate proportional to sync passes, not to retry storms — under sustained failure the counter advances once per polling interval (`--interval`, default 5s) regardless of how many internal tx-level errors lurk underneath.

### `ledgerdb_replication_lag_seconds`

```
# TYPE ledgerdb_replication_lag_seconds gauge
# HELP ledgerdb_replication_lag_seconds Approximate seconds since the last fetch observed new commits. Zero when the index is current.
ledgerdb_replication_lag_seconds 12.3
```

Defined at `metrics.go:58-61`. Unlabelled. The semantics are intentionally approximate: it is *not* the wall-clock difference between the upstream's latest commit timestamp and the local apply time. It is the wall-clock difference between *now* and the last fetch that brought in at least one new commit.

The update path is `OnReplicationFetch` at `metrics.go:119-128`:

```go
func (m *Metrics) OnReplicationFetch(commitsObserved int) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if commitsObserved > 0 {
        m.lastFetchObserve = time.Now()
        m.replicationLag.Set(0)
        return
    }
    m.replicationLag.Set(time.Since(m.lastFetchObserve).Seconds())
}
```

When a fetch brought in new commits, the lag resets to zero. When a fetch brought in nothing, the lag advances monotonically since the last productive fetch. On a quiet repository this means the gauge grows linearly with wall time until either the upstream produces a new commit or somebody concludes from the trend that something is wrong.

The signal is intentionally noisy on its own — a one-minute lag on a repo that gets one commit per hour is normal — which is why the `LedgerDBReplicationLagHigh` alert in `docs/ALERTS.md` uses `> 60` with `for: 5m`. The combination filters out the legitimate quiet-repo case and surfaces the actual stalled-watcher case.

### `ledgerdb_index_sync_duration_seconds`

```
# TYPE ledgerdb_index_sync_duration_seconds histogram
# HELP ledgerdb_index_sync_duration_seconds Wall time spent in a single index sync iteration.
ledgerdb_index_sync_duration_seconds_bucket{le="0.005"} 0
ledgerdb_index_sync_duration_seconds_bucket{le="0.01"}  3
ledgerdb_index_sync_duration_seconds_bucket{le="0.025"} 18
...
ledgerdb_index_sync_duration_seconds_bucket{le="+Inf"} 12834
ledgerdb_index_sync_duration_seconds_sum 1842.7
ledgerdb_index_sync_duration_seconds_count 12834
```

Defined at `metrics.go:62-66` with `prometheus.DefBuckets` (the default `.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10` seconds). Unlabelled. Observed once per sync iteration in `SyncService.Sync` (`service.go:52-55`), covering the full path of fetch (if enabled) + commit listing + tx decode + sidecar apply + state write.

The histogram is the right shape because the SLO question is always a percentile question. "Most sync iterations complete in under 50ms but the 99th percentile is at 800ms" is operationally different from "the average sync iteration is 75ms," and only the histogram lets you separate the two. `histogram_quantile(0.99, rate(ledgerdb_index_sync_duration_seconds_bucket[5m]))` gives you the p99; the same expression with `0.5` gives you the median.

Default buckets cover the typical regime well. A sync iteration that goes past one second is already worth investigating (either the upstream has produced an unusual number of commits or the sidecar is slow); the tail buckets out to 10 seconds catch the unusual cases without bloating the series count.

### `ledgerdb_cas_retries_observed_total`

```
# TYPE ledgerdb_cas_retries_observed_total counter
# HELP ledgerdb_cas_retries_observed_total Best-effort count of CAS retries observed by ledgerdb index watch.
ledgerdb_cas_retries_observed_total 0
```

Defined at `metrics.go:67-70`. Unlabelled. The "observed" qualifier in the name is load-bearing: the counter is incremented only if the watcher's own write paths trigger CAS retries, and the index-watch loop primarily reads. Writers (the `put`/`patch`/`delete` CLI commands) drive CAS retries against `refs/heads/main` through `internal/infra/gitrepo/tx_store.go` (the `casRetryHook` at line 38), but those are short-lived processes without a metrics endpoint of their own.

In practice this counter reflects the watcher's incidental write traffic (state-tree updates, sidecar reset commits) and stays near zero on read-only replicas. The alert in `docs/ALERTS.md` named `LedgerDBCASContention` references `ledgerdb_cas_retries_total`, which is a planned aggregation across writer processes; the observed counter is a precursor and a partial view. Operators who want a true cluster-wide CAS retry rate need a writer-side metrics endpoint, which is not present in v0.2.x.

## Scraping configuration

A minimal Prometheus scrape config for a single watcher:

```yaml
scrape_configs:
  - job_name: ledgerdb-watch
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:9090']
```

For a fleet of replicas, the conventional pattern is one target per host, each running `ledgerdb index watch --metrics-addr 127.0.0.1:9090` with the Prometheus scraper either co-located on each host (pulling from loopback) or sitting on a control-plane host with `--metrics-allow-public` enabled on the watchers and a firewall constraining the source IP.

A 15-second scrape interval is the default in `docs/ALERTS.md` and the right starting value. The metrics surface is small enough that scraping every 5 seconds is also fine; the `for:` clauses on the alerts in `docs/ALERTS.md` are sized to absorb the resulting jitter.

The Grafana dashboard ships at `dashboards/grafana/ledgerdb-watch.json` and is keyed to the metric names above. It surfaces transactions-applied throughput, replication lag, sync error rate, sync duration percentiles, the CAS retry counter, and the standard `up{job="ledgerdb-watch"}` liveness probe. Each panel includes a description that names the source metric so dashboard-to-source-code navigation is one click.

## Per-collection cardinality

`ledgerdb_tx_applied_total` and `ledgerdb_sync_errors_total` carry a `collection` label. The cardinality budget is one series per active collection in the workload. For LedgerDB's intended workstation-to-small-server scale, that is single digits to low hundreds of collections, well within any Prometheus's capacity.

There is deliberately no `doc_id` or `tx_id` label on any metric. Document and transaction identifiers are unbounded cardinality; per-doc metrics would blow up the TSDB on any non-toy workload. Per-doc visibility lives in the [Observability Audit Log](Observability-Audit-Log) (which is a file, not a metric) and in structured logs (see [Observability Logging](Observability-Logging)) — both of which scale through different mechanisms than Prometheus.

If a particular deployment has a small enough collection count that per-collection latency histograms would be tractable, the change is mechanical: add a `WithLabelValues(collection)` to the histogram and observe per-collection in `OnSyncDuration`. The codebase does not do this because the typical operator question is fleet-level ("is the sync loop slow on this host"), not per-collection ("is the sync loop slow when processing orders specifically"). The audit log answers the latter for the cases where it matters.

## Alerts

The cookbook in `docs/ALERTS.md` defines six alert rules keyed to the metrics above:

- `LedgerDBReplicationLagHigh` — `ledgerdb_replication_lag_seconds > 60` for 5 minutes.
- `LedgerDBSyncErrorRateHigh` — `rate(ledgerdb_sync_errors_total[5m]) > 0.1` for 5 minutes.
- `LedgerDBWatchStalled` — zero `rate(ledgerdb_tx_applied_total[5m])` *and* positive `deriv(ledgerdb_replication_lag_seconds[5m])` for 10 minutes.
- `LedgerDBCASContention` — `rate(ledgerdb_cas_retries_total[5m]) > 10` for 5 minutes (the cookbook references the cluster-aggregate name; the v0.2.x series is the per-watcher `ledgerdb_cas_retries_observed_total`).
- `LedgerDBGCPressure` — `ledgerdb_loose_objects_count > 10000` for 15 minutes (the metric is sourced from `internal/cli/cmd_stats.go:46` rather than the watcher metrics path).
- `LedgerDBWatchCrash` — `up{job="ledgerdb-watch"} == 0` for 2 minutes.

The cookbook is the operator-facing companion to this page; this page is the source-of-truth for which Go file each series comes from.

## See also

- [Observability Overview](Observability-Overview)
- [Observability Logging](Observability-Logging)
- [Observability Audit Log](Observability-Audit-Log)
- [Performance Overview](Performance-Overview)
- `internal/app/index/metrics.go` — collector definitions and the loopback bind policy.
- `internal/app/index/service.go` — the `Observer` call sites that drive the counters.
- `internal/cli/commands.go` — the `--metrics-addr` flag and the `RunServer` wiring.
- `docs/ALERTS.md` — the Prometheus alerting cookbook.
- `dashboards/grafana/ledgerdb-watch.json` — the reference Grafana dashboard.
