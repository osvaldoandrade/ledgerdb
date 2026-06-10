# Observability Overview

LedgerDB is a CLI-first system. Most operations — `init`, `put`, `patch`, `query`, `inspect` — are short-lived processes that print a result, exit, and leave nothing behind to observe. The one long-running surface is `ledgerdb index watch`, the background loop that tails new commits on `refs/heads/main` and applies them to the SQLite sidecar. That command is the only place in the binary where observability needs to compose into something an operator can keep open in a Grafana tab. The rest of the CLI is observed by reading its stdout, its exit code, and the structured log lines it emits to stderr.

This section frames how LedgerDB v0.2.x treats observability and what the operator gets in return for opting in. There are three pillars in scope today, not four: structured logs (via `log/slog`), opt-in Prometheus metrics on `index watch`, and the opt-in audit log on `index watch`. Distributed tracing is not wired in. The reason is documented below.

## What this page covers

- The three observability pillars that exist today, and which file each lives in.
- Why each pillar is opt-in rather than always-on, and what the tradeoff is.
- A worked example: an operator chasing why the SQLite sidecar fell behind upstream.
- Pointers to the per-pillar pages that go deep.

## What this page does not cover

- Distributed tracing. It is on the roadmap (Epic [#4](https://github.com/osvaldoandrade/ledgerdb/issues/4) — "Opt-in instrumentation for `ledgerdb index watch`") but the current shipping surface is metrics + audit only. There is no W3C `traceparent` propagation, no OTLP exporter, and no per-tx span. A repo that wants causal-chain visibility today has to derive it from the audit log's `tx_id` field plus structured slog records, which is narrative rather than causal.
- The webhook/notification path. LedgerDB does not deliver outbound webhooks; downstream consumers are expected to tail the sidecar or invoke `ledgerdb index watch` themselves.
- Performance counters internal to Git operations beyond CAS retries. The retry counter exists (`ledgerdb_cas_retries_observed_total` in `internal/app/index/metrics.go:67-70`) but its source is a best-effort observation, not a Pebble-style group-commit instrument.

## The three pillars

LedgerDB's observability surface is small on purpose. Each pillar answers a different shape of question.

Structured logs answer "what did this specific CLI invocation do, in what order, and with what error." Every command goes through `platform.ConfigureLogger` in `internal/platform/logger.go:17-42`, which honors `--log-level` and `--log-format` (or `LEDGERDB_LOG_LEVEL` / `LEDGERDB_LOG_FORMAT` env vars). The default is text on stderr; flipping to JSON is one flag. Logs are the only pillar that is always on — every command emits them whether the operator asked for them or not. See [Observability Logging](Observability-Logging) for the level model, the format toggle, and the conventions around fields like `collection`, `doc_id`, and `tx_id`.

Metrics answer "how many, how fast, at which percentile, with what error rate." They are exposed by `ledgerdb index watch` only, and only when the operator passes `--metrics-addr`. The collectors are defined in `internal/app/index/metrics.go:40-81` and registered against a private `prometheus.Registry` (line 41), not the global default registry. That matters because it means embedding `index watch` in tests or invoking it repeatedly does not bleed counters between runs. Five series are exported: `ledgerdb_tx_applied_total`, `ledgerdb_sync_errors_total`, `ledgerdb_replication_lag_seconds`, `ledgerdb_index_sync_duration_seconds`, and `ledgerdb_cas_retries_observed_total`. See [Observability Metrics](Observability-Metrics) for the full catalog, the label cardinality, the loopback-only-by-default bind policy, and how to scrape.

The audit log answers "what changed on this replica, when, and which tx authored each change." It is also opt-in (`--audit-log <path>` on `index watch`). The implementation is `internal/app/index/audit.go:36-83`. The output is JSON Lines, one record per applied transaction; the schema is fixed at six fields (`ts`, `tx_id`, `collection`, `doc_id`, `op`, `actor`) and the actor is hard-coded to `index-watch`. The audit log is deliberately a faithful record of *applied* transactions only — sync-level errors go to slog and metrics, not to the audit file (`audit.go:135`). See [Observability Audit Log](Observability-Audit-Log) for the format, the compliance posture, and how it composes with the other two pillars.

The three pillars share a single fan-out point: the `Observer` interface in `internal/app/index/ports.go:70-75`. `SyncService.SetObserver` (`internal/app/index/service.go:27-29`) registers exactly one observer, and `CombinedObserver` in `internal/app/index/audit.go:175-189` lets the CLI wire metrics and audit together when both flags are supplied. There is no global registry, no init-side effect, no Prometheus default registration. The CLI explicitly checks each flag and explicitly hands the resulting `*Metrics` or `*AuditLogger` to the service (`internal/cli/commands.go:484-531`). That is the only path by which either pillar gets data.

## Why each pillar is opt-in

The pattern is: zero overhead when off, well-defined cost when on.

Metrics are opt-in because binding a TCP listener for `/metrics` is a deployment decision, not a library decision. The default is no listener and no HTTP server. When the operator passes `--metrics-addr 127.0.0.1:9090`, the metrics server binds loopback-only — a non-loopback bind is refused by `assertLoopback` in `metrics.go:176-188` unless `--metrics-allow-public` is also passed. That matches the v0.2.x posture of treating LedgerDB as a workstation-class daemon that should not, by accident, expose itself on `0.0.0.0`.

The audit log is opt-in because writing one record per applied tx adds a per-tx syscall on the hot path. The cost is small (the writer is `bufio`-buffered and flushed on a one-second ticker by default, `audit.go:88-110`) but it is not zero, and a workstation user running `index watch` for live development does not necessarily want a JSON Lines file growing in the corner. The flag is the audit decision.

Structured logging is always on because the cost of emitting an info line through a `slog.JSONHandler` is negligible compared to a Git transport round-trip or a SQLite transaction commit. The level defaults to `info`, which keeps the volume low; bumping to `debug` is appropriate during incidents and benign in steady state. See [Observability Logging](Observability-Logging) for the level discipline.

## A worked example: the sidecar fell behind

The Prometheus alert `LedgerDBReplicationLagHigh` (documented in `docs/ALERTS.md`) fires because `ledgerdb_replication_lag_seconds` has been above 60 for five minutes on `replica-3`. The investigation walks all three pillars in turn.

The operator opens the Grafana dashboard at `dashboards/grafana/ledgerdb-watch.json` and confirms the lag is real. The same dashboard graphs `rate(ledgerdb_tx_applied_total[5m])` and the operator notices the apply rate has dropped to roughly zero in the same window. So the sidecar is not catching up because the watcher is not applying anything. Two hypotheses: either the upstream has no new commits (a benign case the alert should not have fired on, but worth ruling out), or the watcher is hitting an error every pass.

The operator pivots to `rate(ledgerdb_sync_errors_total[5m])` filtered to `instance="replica-3"`. The counter is climbing at about one error per second. The `reason` label is `commit_not_found` — the classification comes from `classifyErr` in `internal/app/index/service.go:95-116`, which buckets known sentinel errors into a small, low-cardinality set so the metric stays queryable. So the watcher is iterating, failing on a missing commit, and not advancing. Metrics have done their job.

The operator switches to the watcher's structured logs. The log level on the host is `info`; a quick edit of the systemd unit to flip `LEDGERDB_LOG_LEVEL=debug` and a restart gives the next pass's full narrative. The debug log lines from `internal/app/index/` and the surrounding `internal/cli/commands.go:537-572` loop show the watcher trying to fast-forward against a commit hash that does not resolve locally. The repo on `replica-3` has fallen behind, and the upstream's `refs/heads/main` has been rewritten — most likely by an `index watch --mode state` reset on a writer that flipped to `--history-mode amend` (see `service.go:127-138` for the reset path). Logs have done their job.

The operator confirms the hypothesis by opening the audit log at `/var/log/ledgerdb/audit.jsonl` and reading the tail. The last applied `tx_id` is from three days ago, on the same `collection` the writer has been rewriting. The JSON Lines records are decisive about which transactions made it onto the sidecar and which did not, because they are written only inside `OnTxApplied` (`audit.go:113-130`), inside the SQLite transaction's commit boundary. The audit log has done its job.

The fix is operational: re-run `ledgerdb index watch --once` with `AllowReset` enabled (which the CLI infers from the manifest's `HistoryMode=amend`, see `commands.go:543`) so the watcher rebuilds the sidecar from the new history. The metric resets, the alert clears, the audit log resumes accumulating new tx records. None of those steps required attaching a debugger, none of them required re-deploying, and the chain — metric to log to audit — was walked entirely from data already in the operator's hands.

## How the rest of this section is organized

[Observability Logging](Observability-Logging) covers slog configuration, the `--log-level` and `--log-format` flags, environment-variable overrides, JSON-versus-text encoding, the convention of carrying `collection` and `tx_id` on every relevant record, and how to ship the resulting JSONL stream to an aggregator. [Observability Metrics](Observability-Metrics) covers the five Prometheus collectors emitted by `index watch`, the label cardinality discipline, the loopback-only-by-default bind policy, and how the dashboard at `dashboards/grafana/ledgerdb-watch.json` consumes them. [Observability Audit Log](Observability-Audit-Log) covers the JSON Lines schema, the flush semantics, the deliberate decision to log applied transactions only (not errors), and how operators use the file for compliance and incident review.

The throughline is composition. None of the pillars is sufficient alone — a metric tells you the rate, a log line tells you the narrative of a single pass, the audit log tells you which transactions actually committed. The three together compose into the investigative workflow the worked example above walks.

## See also

- [Observability Logging](Observability-Logging)
- [Observability Metrics](Observability-Metrics)
- [Observability Audit Log](Observability-Audit-Log)
- [Performance Overview](Performance-Overview)
- `docs/ALERTS.md` — the Prometheus alerting cookbook keyed to the metric names this section documents.
- `docs/PERFORMANCE.md` — operational tuning guidance for `index watch`.
- `dashboards/grafana/ledgerdb-watch.json` — the reference Grafana dashboard that consumes the metric series.
