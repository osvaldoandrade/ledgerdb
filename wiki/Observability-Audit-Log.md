# Observability: Audit Log

The audit log is the third observability pillar in LedgerDB and the only one whose primary purpose is compliance rather than operations. Where metrics tell you the rate and logs tell you the narrative, the audit log is a faithful, append-only, machine-readable record of every transaction that was applied to the local SQLite sidecar by `ledgerdb index watch`. It exists so that a compliance reviewer, an SRE reconstructing an incident, or a downstream change-data-capture consumer can answer the question "did this tx commit on this replica, and when" without needing to walk the Git history or query the sidecar.

The implementation is `internal/app/index/audit.go`. It was added in commit 7179d14 ("feat(index): add opt-in metrics and audit log to `index watch`", PR #124) as part of Epic [#4](https://github.com/osvaldoandrade/ledgerdb/issues/4).

## What this page covers

- The JSON Lines schema and the field-by-field meaning.
- The opt-in `--audit-log` and `--audit-flush-interval` flags.
- The flush semantics and the durability tradeoff implied.
- The deliberate decision to record applied transactions only, not errors.
- How the audit log composes with metrics and logs in an investigation.
- Use cases: compliance review, incident reconstruction, downstream CDC.

## What this page does not cover

- Per-document audit of *reads*. The file records mutating tx applies only; queries against the sidecar are not in scope.
- Cross-replica reconciliation. Each replica writes its own audit file; correlating two replicas is the reader's job and is straightforward because `tx_id` is a content-addressed identifier shared across replicas.
- Tamper-evidence beyond what the underlying filesystem provides. The audit file is plain JSON Lines and is not signed; tamper-evidence in LedgerDB sits in the Git commit-and-signature chain, not in this file. See `docs/06_INTEGRITY.md` for the integrity story.

## Opt-in: `--audit-log`

The flag is declared in `internal/cli/commands.go:587-588`:

```go
cmd.Flags().StringVar(&auditLogPath, "audit-log", "",
    "Append JSON Lines audit records to the given path ('-' for stdout). Empty disables the audit log.")
cmd.Flags().DurationVar(&auditFlushInterval, "audit-flush-interval", time.Second,
    "Interval between buffered audit-log flushes.")
```

The default is empty. When empty, no audit logger is constructed and no per-tx callback is invoked — zero overhead. When non-empty, the watcher opens the destination with `O_APPEND|O_CREATE|O_WRONLY` mode `0o644` (`audit.go:69`) and starts a background flusher (`audit.go:88-110`).

Two destination forms are special:

- The literal `-` writes to `os.Stdout` (`audit.go:66-67`). Useful for piping into another process (`ledgerdb index watch --audit-log - | jq ...`) or for capturing the audit stream alongside structured logs in a single supervisor pipeline.
- An empty string disables the logger entirely; the CLI never constructs one and the sync service's observer chain never receives an audit observer (`commands.go:504-517` and `commands.go:519-531`).

Any other value is a filesystem path. The file is opened in append mode so re-running the watcher does not truncate prior records.

The flush interval defaults to one second. Setting it lower reduces the worst-case window of records lost on a hard kill (the writer is `bufio`-buffered; records written between flushes live in memory only). Setting it higher amortizes filesystem syscalls across more records.

## The schema

The record type is fixed at six fields (`audit.go:20-27`):

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

A typical line:

```json
{"ts":"2026-06-10T14:32:11.482938Z","tx_id":"01JCAS7F3A8E2D1B4C4A5E9D6F1234","collection":"orders","doc_id":"ord-42","op":"PATCH","actor":"index-watch"}
```

Field-by-field:

- `ts` is RFC 3339 with nanosecond precision in UTC (`audit.go:115`). The clock source is `time.Now` by default; the field is set inside `OnTxApplied` (`audit.go:113-130`) at the moment the record is queued, not at the moment the SQLite transaction commits. The two are within microseconds of each other in practice, and the field is intended as a "when did the replica observe this tx" timestamp, not a "when was this tx authored" timestamp. The authoring timestamp lives on the transaction itself in the Git store.
- `tx_id` is the content-addressed transaction identifier (ULID-prefixed, see `internal/infra/ident/`). Stable across replicas because it is computed from the tx bytes. This is the join key: a `tx_id` in the audit log maps to a row in the SQLite sidecar's tx table, to a blob in the Git store, and to a `tx_id` field on any structured log line emitted while processing the same tx.
- `collection` and `doc_id` identify the target document. Both come from the decoded transaction (`internal/app/index/service.go:360-367`); both are passed through verbatim from the producer.
- `op` is the transaction operation: `PUT`, `PATCH`, `MERGE`, or `DELETE`. Sourced from `domain.TxOp.String()` so the string form is the canonical one declared in `internal/domain/tx.go`.
- `actor` is hard-coded to the literal string `index-watch` (`audit.go:16, 120`). The current schema does not distinguish between watcher instances on different hosts; if multi-watcher correlation matters, the hostname must come from the shipping pipeline (journald, syslog, log forwarder), not from the record itself.

The schema is intentionally compact. The keys are three-or-four-character strings to keep the per-record byte cost low; there is no version field, no envelope, no nested object. Adding fields in the future requires versioning the record shape; the current pragma is that this minimal schema is enough for the use cases the audit log is intended for.

## Flush semantics

The writer is wrapped in `bufio.NewWriter` (`audit.go:77`). Records are encoded with `json.Marshal` inside `OnTxApplied` (`audit.go:113-130`), written to the buffer under a mutex, and flushed either on the ticker interval set by `Start` (`audit.go:88-110`) or on `Close` (`audit.go:152-164`).

The ticker is the only path that produces durable bytes during steady-state operation. A power loss between two ticks loses every record buffered since the last flush. The default of one second means the worst-case loss window is roughly one second of applied transactions, which for a workstation-class watcher is on the order of tens to low hundreds of records depending on workload.

The tradeoff is explicit. A flush interval of zero would force `Flush` after every record, eliminating the loss window but adding a `write(2)` syscall per applied tx and meaningfully changing the hot-path cost. The current default optimizes for the common case where the sidecar's own SQLite WAL is the durability boundary that matters and the audit log is the observability record on top of it. If a deployment's compliance posture treats the audit log as the primary durable record, set `--audit-flush-interval 0` (or a small duration like `10ms`) and measure the impact.

`Close` is safe to call multiple times (`audit.go:152-164`) and is invoked from a deferred close in the CLI (`commands.go:514-516`). The close path stops the ticker, drains the buffer with a final `Flush`, and closes the underlying file if one was opened. On graceful shutdown (`SIGTERM` to the watcher) the buffer is flushed before the process exits; on `SIGKILL` it is not.

## Applied transactions only

A point worth emphasizing: the audit log records transactions that were *applied* to the sidecar, not sync errors, not retried fetches, not the high-level outcome of a sync pass. The four `Observer` callbacks defined at `internal/app/index/ports.go:70-75` are split such that `OnSyncError`, `OnSyncDuration`, and `OnReplicationFetch` are no-ops on the audit logger (`audit.go:135-141`):

```go
// OnSyncError satisfies Observer. Sync-level errors are surfaced via metrics
// and slog; we deliberately do not write them to the audit log so the file
// stays a faithful record of applied txs only.
func (a *AuditLogger) OnSyncError(string, string) {}
```

The reason is that the audit log's value is precisely its narrow scope. A compliance reviewer asking "did this tx commit" wants a single source of truth; mixing error records in would force the reviewer to filter by `op` or by some discriminator field, and a record with no document context (a sync-level fetch failure has no `tx_id` or `doc_id`) does not fit the schema anyway. Errors are visible in the [Observability Metrics](Observability-Metrics) counter `ledgerdb_sync_errors_total` (bucketed by reason) and in the structured logs (see [Observability Logging](Observability-Logging)). The audit log is the receipt; the log is the journal.

The corollary is that the absence of an audit record for a given `tx_id` is *not* by itself evidence the transaction did not commit. The watcher could have been running without `--audit-log` set; the file could have been rotated by an external log manager; the flush could have been pending at the moment a crash occurred. A complete compliance posture treats the audit log as one of several signals and reconciles it against the sidecar and the Git store.

## Composition with metrics and logs

The three observability pillars in LedgerDB share one fan-out point: the `Observer` interface and the `CombinedObserver` in `internal/app/index/audit.go:166-189`. When the operator enables both `--metrics-addr` and `--audit-log`, the CLI wires them together:

```go
var observers []indexapp.Observer
if metrics != nil {
    observers = append(observers, metrics)
}
if audit != nil {
    observers = append(observers, audit)
}
if observer := indexapp.NewCombinedObserver(observers...); observer != nil {
    service.SetObserver(observer)
}
```

(`internal/cli/commands.go:519-531`)

The `CombinedObserver.OnTxApplied` fans the same event out to every observer in order (`audit.go:191-195`). That means a single tx apply produces exactly one Prometheus counter increment, one audit record, and (depending on log level and call sites) any number of slog records. The three pillars are decoupled in their backing store but coupled in their event source — they cannot disagree about which transactions occurred, only about whether the operator was capturing them.

A worked investigation typically walks the three in order:

1. Metric: `rate(ledgerdb_tx_applied_total{collection="orders"}[5m])` shows the apply rate dropped to zero in a five-minute window.
2. Log: filter the structured logs to the same window. The records show what the sync loop was doing — likely a `commit_not_found` warn and a reset.
3. Audit: `tail -n 100 /var/log/ledgerdb/audit.jsonl` shows the last applied `tx_id`. The reader has a precise checkpoint of how far the replica got before the disruption.

The audit log's role in that chain is the precise checkpoint. The metric tells you "something stopped"; the log tells you "and here is why"; the audit tells you "and here is the last clean state."

## Use cases

**Compliance review.** Regulated workloads (financial ledgers, consent records, audit trails) need a per-tx, per-replica record of what was applied and when. The audit log is exactly that. The expected pattern is to ship the JSON Lines file to a tamper-evident archive (write-once storage, an external SIEM, a cryptographically-signed log shipper) and to retain it for the regulatory retention window. The file format is stable across releases; the field set is small enough that downstream parsers can treat the schema as fixed.

**Incident reconstruction.** When a downstream consumer reports a missing document update, the audit file on the replica that fed that consumer is the first place to look. A grep for the `doc_id` and `tx_id` in question either confirms the watcher applied the change (in which case the consumer's bug is downstream) or confirms the watcher did not (in which case the upstream Git history is the next layer to inspect). The investigation does not require restoring a SQLite backup or replaying the Git log; the audit file is the authoritative replica-side record.

**Downstream change-data-capture.** Some pipelines tail the audit file as a poor-man's CDC stream. The file is append-only, the records are self-contained, and the `tx_id` provides a stable cursor for restart. The pattern is straightforward but the caveats are real: the file is best-effort flushed (see flush semantics above), records can in principle be re-emitted if the watcher restarts mid-write, and there is no order guarantee across collections beyond the order in which the watcher applied them. For richer CDC semantics, querying the SQLite sidecar directly is the better path. The audit log is a useful low-tech alternative when "tail a file" is enough.

## Operating notes

The file grows monotonically until rotation. There is no in-process rotation; this is consistent with LedgerDB's stance that log shipping is a deployment concern. Use `logrotate` or an equivalent system-level rotation policy. The watcher will not notice the rotation because it holds an open file handle on the original inode; rotate via `copytruncate` if the watcher cannot be restarted, or arrange a `SIGTERM`-and-restart cycle if the rotation should produce a clean cut. The watcher's deferred close flushes the buffer before exiting, so a controlled restart loses no records.

Disk-space sizing is workload-dependent. A workstation tailing a few hundred tx per hour produces a few hundred kilobytes per day. A high-throughput replica applying tens of transactions per second produces megabytes per minute. The record is small (typically 150-300 bytes) but the volume scales with the tx rate, which is unbounded.

The audit logger is independent of the structured-log level. Even with `--log-level error`, `OnTxApplied` still fires and the audit file still grows — the audit logger does not consult slog. That separation is intentional: the audit log records data events, not control-plane chatter, and is not throttled by the verbosity knob that governs developer-facing output.

## See also

- [Observability Overview](Observability-Overview)
- [Observability Metrics](Observability-Metrics)
- [Observability Logging](Observability-Logging)
- [Operations and CLI Strategy](SDK-CLI-Reference)
- `internal/app/index/audit.go` — the writer, the schema, the flush loop, and the `CombinedObserver` fan-out.
- `internal/app/index/ports.go` — the `Observer` interface that `AuditLogger` implements.
- `internal/cli/commands.go` (lines 504-531) — the CLI wiring that constructs the audit logger and composes it with metrics.
- `docs/06_INTEGRITY.md` — the broader integrity story that the audit log complements.
