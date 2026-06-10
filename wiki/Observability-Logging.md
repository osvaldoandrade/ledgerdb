# Observability: Structured Logging

Structured logging is the narrative pillar in LedgerDB and the only one that is always on. Every CLI invocation — `init`, `put`, `query`, `index watch`, `maintenance gc`, all of them — runs through the same logger construction path and writes records to stderr in either text or JSON. The logger is not a library detail tucked inside a service; it is wired in `PersistentPreRunE` on the root cobra command so the level and format settings apply uniformly to every subcommand without each subcommand having to know about logging.

This page documents the logger configuration in `internal/platform/logger.go`, the `--log-level` and `--log-format` flags wired in `internal/cli/root.go`, the corresponding `LEDGERDB_LOG_*` environment overrides, the JSON-versus-text tradeoff, and the conventions around structured fields used by `internal/app/index/` and the surrounding CLI code.

## What this page covers

- How `platform.ConfigureLogger` builds an `slog.Logger` and installs it as the process default.
- The four-level model and what each level is for in a CLI context.
- The flag and environment-variable surface that controls level and format.
- The JSON-versus-text decision and when to flip.
- The structured-field conventions used in the index/sync hot path.
- How to ship logs from a long-running `index watch` to an aggregator.

## What this page does not cover

- Distributed tracing. There is no `traceparent` propagation in v0.2.x. Log records do not carry a span context because no span exists. See [Observability Overview](Observability-Overview) for the roadmap.
- Per-request log throttling or sampling. The volume in a CLI is bounded by the operator's invocation rate; there is no sampler in the pipeline.
- Log shipping inside the binary. LedgerDB writes to stderr; everything from there onward is a deployment decision.

## Logger setup

The constructor lives at `internal/platform/logger.go:17-42`:

```go
func ConfigureLogger(levelValue, formatValue string, out io.Writer) (*slog.Logger, error) {
    level, err := ParseLogLevel(levelValue)
    if err != nil {
        return nil, err
    }
    format, err := ParseLogFormat(formatValue)
    if err != nil {
        return nil, err
    }
    handlerOpts := &slog.HandlerOptions{Level: level}
    var handler slog.Handler
    switch format {
    case LogFormatJSON:
        handler = slog.NewJSONHandler(out, handlerOpts)
    case LogFormatText:
        handler = slog.NewTextHandler(out, handlerOpts)
    default:
        return nil, fmt.Errorf("unsupported log format %q", formatValue)
    }
    logger := slog.New(handler)
    slog.SetDefault(logger)
    return logger, nil
}
```

Three decisions are baked in.

First, the level is a plain `slog.Level` selected at construction time, not a `slog.LevelVar`. That means the level cannot be raised or lowered at runtime; flipping `--log-level debug` for an investigation requires re-invoking the CLI. For `ledgerdb index watch` that is a process restart, which is acceptable because the process is intended to be supervised (systemd, docker, kubernetes) and a restart is a one-line operation.

Second, the handler is selected at construction time too. There is no per-record routing between JSON and text; the whole process emits one format. The text handler is the default because most LedgerDB invocations are interactive at a terminal, where the text form (`time=... level=INFO msg="..." key=value`) reads better than JSON.

Third, `slog.SetDefault(logger)` installs the constructed logger as the process default. Every piece of code that calls `slog.Info`, `slog.Warn`, `slog.Error`, or `slog.Debug` from anywhere in the binary will route through this handler. That is what lets the CLI configure logging once at the root command and let every service downstream use the package-level slog helpers without plumbing a logger argument through every constructor.

The output sink is `cmd.ErrOrStderr()` (`internal/cli/root.go:45`), which resolves to `os.Stderr` for normal invocations. The convention is that stdout carries the command's result (the JSON envelope for `--json`, the human-readable summary otherwise) and stderr carries log lines, spinners, and progress text. Tools that scrape one without the other get a clean separation.

## Flags and environment variables

The root command declares the two relevant flags at `internal/cli/root.go:65-66`:

```go
cmd.PersistentFlags().StringVar(&opts.LogLevel, "log-level", opts.LogLevel,
    "Log level (debug, info, warn, error)")
cmd.PersistentFlags().StringVar(&opts.LogFormat, "log-format", opts.LogFormat,
    "Log format (text, json)")
```

Both are persistent, which means they apply to every subcommand. The default values are pulled from environment variables earlier in the same file:

```go
LogLevel:  envDefault("LEDGERDB_LOG_LEVEL", "info"),
LogFormat: envDefault("LEDGERDB_LOG_FORMAT", "text"),
```

(`internal/cli/root.go:28-29`)

The precedence is the conventional one: an explicit `--log-level` flag wins over `LEDGERDB_LOG_LEVEL` wins over the built-in default of `info`. Same for format. A `systemd` unit running `ledgerdb index watch` can set `Environment=LEDGERDB_LOG_FORMAT=json` once in the unit file and never need to remember the flag at the call site.

`ParseLogLevel` at `internal/platform/logger.go:44-58` accepts `debug`, `info`, `warn`/`warning`, and `error`, case-insensitive, with leading and trailing whitespace trimmed. The empty string maps to `info` rather than rejecting; that keeps `LEDGERDB_LOG_LEVEL=""` behaving the same as unset. Any other value returns an error and the CLI exits with the message before doing real work.

`ParseLogFormat` at `internal/platform/logger.go:60-70` accepts only `text` and `json`. The empty string maps to `text`. Same case-insensitive, trim-whitespace policy.

## The level model

Four levels, with discipline about what each means in a CLI process.

`debug` is for development and post-incident investigation. It surfaces every iteration of `index watch`'s sync loop, every commit hash considered, every CAS retry attempt the underlying gitrepo store made. The cost is volume: a `ledgerdb index watch --interval 1s` at debug emits multiple records per second per pass even on a quiet repo. The discipline is to use it for a bounded window, not as the steady-state setting.

`info` is the operational baseline. The default. For one-shot CLI commands the typical info-level output is one or two lines per invocation summarizing what was done. For `index watch` the typical pattern is one record per sync pass that brought in new transactions, or silence on passes that did not — the `--only-changes` flag (`commands.go:578`) is the explicit gate.

`warn` is for recoverable problems. A fetch that failed but will be retried on the next interval; a sync pass that hit an `ErrCommitNotFound` and intends to reset; a CAS round that exhausted its retries and surfaced `domain.ErrHeadChanged` for the caller to handle. The defining property is that the next iteration of the loop, or the next CLI invocation, has a reasonable chance of succeeding without operator intervention.

`error` is for unrecoverable problems within the scope of the current invocation. For one-shot commands that usually means a non-zero exit follows the log line; for `index watch` it means the loop has hit a condition the watcher cannot work around (a corrupt sidecar, a refused metrics bind, a destination filesystem that does not exist).

The discipline is to use `warn` for things that auto-recover and `error` for things that need human attention. Sustained `error` lines should page; sustained `warn` should not but should be inspected. The convention is enforced by code review, not by tooling — there is no schema check.

## Structured field conventions

LedgerDB does not use slog's `With(...)` to bind global fields like `service` or `env`, because the CLI is a single-binary single-process tool and those fields would be constant for every line. What it does use is per-record structured keys at call sites that have meaningful context.

The audit log defines the canonical short-key vocabulary at `internal/app/index/audit.go:20-27`:

```go
type AuditRecord struct {
    Timestamp  string `json:"ts"`
    TxID       string `json:"tx_id"`
    Collection string `json:"collection"`
    DocID      string `json:"doc_id"`
    Op         string `json:"op"`
    Actor      string `json:"actor"`
}
```

The same key names are the convention for slog fields elsewhere in the codebase. A log line about a specific transaction should carry `tx_id`; a line about a collection-scoped operation should carry `collection`; a line about a specific document should carry `doc_id`. Operations against `refs/heads/main` carry `ref` when relevant. Error strings go in a field literally named `err` (not `error`, which collides with the JSON convention some aggregators use for error objects).

What you will *not* find in fields: full document payloads, JSON Patch bodies, or any free-form blob from the transaction snapshot. Payloads can carry customer data and are unbounded in size; logs are searchable forever. The audit log records the `tx_id` and `op` so the reader can reconstruct what happened by looking up the tx blob in the Git store, not by reading it from the log file.

The metrics layer uses the same vocabulary for Prometheus labels: `collection` is a label on `ledgerdb_tx_applied_total` and `ledgerdb_sync_errors_total`, and a `reason` label is added to errors using the low-cardinality bucket from `classifyErr` in `internal/app/index/service.go:95-116`. That uniformity is what lets an operator pivot from a metric (`ledgerdb_sync_errors_total{collection="orders",reason="commit_not_found"}`) directly to a log query (`collection=orders AND msg contains "commit_not_found"`) without renaming fields.

## JSON versus text

The default is text because the CLI is interactive. A `ledgerdb put` that fails with a CAS retry exhaustion is easier to read as

```
time=2026-06-10T14:32:11.482Z level=WARN msg="head changed" collection=orders doc_id=ord-42
```

than as the equivalent JSON one-liner. The text handler is also greppable in a way that JSON parsers occasionally surprise on.

The recommendation flips for `ledgerdb index watch` in production: set `LEDGERDB_LOG_FORMAT=json` in the unit file so the supervisor captures structured records that aggregators (Loki, Vector, OpenSearch) parse natively. The set of fields is identical between the two formats — slog handlers are pure encoders — so flipping the format does not change what gets logged, only how it is encoded.

A subtle point: the `--log-format` flag is global, not per-record. There is no way to ask for JSON for the index-watch loop and text for the surrounding CLI banner. The two have to share a format. In practice this is fine because production deployments only invoke `index watch` and want JSON throughout; interactive sessions only invoke short-lived commands and want text throughout.

## Shipping logs to an aggregator

LedgerDB does no in-process log shipping. There is no file rotation, no buffered HTTP exporter, no Loki sink. Output goes to stderr and the deployment is expected to capture it. The conventions match what every other modern Go service does:

- Under `systemd`, `journalctl -u ledgerdb-watch -f` reads the stderr stream; `journalctl -o json` re-encodes the captured lines.
- Under `docker run`, the container runtime captures stderr; `docker logs` reads it; any docker log driver (`fluentd`, `gelf`, `awslogs`) ships it.
- Under `kubernetes`, the container runtime captures stderr to the node's log directory; the standard DaemonSet log shipper (Fluent Bit, Vector, Promtail) tails it and ships it to the aggregator of choice.

The reason the binary has no opinion on shipping is the same reason most CLI tools do not: the right shipper is a function of the host, not the application. A shipping pipeline inside the binary would couple LedgerDB to one aggregator and force the binary to retain a local buffer if the aggregator were unreachable. Stderr has the right failure mode: if the collector is down, the lines queue in the supervisor's buffer until either the collector returns or the buffer rotates, and LedgerDB itself does not block on logging.

A typical Loki query to find sync errors on a specific collection for a recent incident window looks roughly like:

```
{service="ledgerdb-watch"} | json | level="WARN" | collection="orders"
```

The same shape with `level="ERROR"` surfaces the unrecoverable cases. A query for a single transaction's narrative is `tx_id="01JCAS..."` plus `service="ledgerdb-watch"`; combined with the audit log entry for the same `tx_id` (see [Observability Audit Log](Observability-Audit-Log)), the operator has both the journal and the receipt for that change.

## What logs do not tell you

Logs are narrative, not quantitative. You cannot derive a histogram from log lines, you cannot reliably count `level=WARN` occurrences for a SLO, and you should not try. For "how fast is the index applying transactions" the answer is `rate(ledgerdb_tx_applied_total[5m])` (see [Observability Metrics](Observability-Metrics)). For "did this specific transaction commit on this specific replica" the answer is a grep against the JSON Lines audit file (see [Observability Audit Log](Observability-Audit-Log)). Logs are the place to look when you have a question shaped like "what was happening at this moment" and you already know roughly where to look.

## See also

- [Observability Overview](Observability-Overview)
- [Observability Metrics](Observability-Metrics)
- [Observability Audit Log](Observability-Audit-Log)
- [CLI Reference](CLI-Reference)
- `internal/platform/logger.go` — the constructor and the level/format parsers.
- `internal/cli/root.go` — where the flags and environment defaults are wired.
- `docs/ALERTS.md` — alert rules whose log-side companions follow the field conventions described here.
