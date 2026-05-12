# LedgerDB Alerting Cookbook

This document is a copy-paste PromQL alerting cookbook for LedgerDB
deployments running `ledgerdb index watch` with the Prometheus metrics
endpoint enabled.

The rules below assume the standard metric names exposed by issues #9,
#12, and #15:

| Metric                                  | Type       | Meaning                                                       |
|-----------------------------------------|------------|---------------------------------------------------------------|
| `ledgerdb_tx_applied_total`             | counter    | Transactions applied to the sidecar                           |
| `ledgerdb_sync_errors_total`            | counter    | Errors during a watch sync pass                               |
| `ledgerdb_cas_retries_total`            | counter    | CAS retries on `refs/heads/main`                              |
| `ledgerdb_replication_lag_seconds`      | gauge      | Seconds between upstream commit time and local apply time     |
| `ledgerdb_loose_objects_count`          | gauge      | Number of loose objects in the Git store                      |
| `up{job="ledgerdb-watch"}`              | gauge      | Standard Prometheus liveness probe                            |

Every rule includes `severity`, `summary`, `description`, and a
`runbook_url` annotation. Replace `https://runbooks.example.com/`
with your operational runbook root before deploying.

---

## How to load these rules

Save the YAML below as `/etc/prometheus/rules/ledgerdb.yaml` (or your
equivalent path) and reference it from `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/rules/ledgerdb.yaml
```

Reload Prometheus (`SIGHUP` or `curl -X POST .../-/reload`) and confirm
in the UI under *Status → Rules* that all six rules show `ok`.

---

## Alert rules

### 1. `LedgerDBReplicationLagHigh`

A replica is falling behind the upstream by more than a minute. Either
the watcher is throttled, the upstream is overloaded, or the network
path between them is degraded.

```yaml
groups:
  - name: ledgerdb-replication
    interval: 30s
    rules:
      - alert: LedgerDBReplicationLagHigh
        expr: ledgerdb_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB replication lag above 60s on {{ $labels.instance }}"
          description: |
            Instance {{ $labels.instance }} has been lagging upstream by
            {{ $value | humanizeDuration }} for at least 5 minutes.
            Replicas falling further behind risk stale reads and missed
            change-data-capture events. Check upstream commit volume,
            local CPU saturation, and the watch process logs.
          runbook_url: "https://runbooks.example.com/ledgerdb/replication-lag"
```

### 2. `LedgerDBSyncErrorRateHigh`

The watcher is logging errors on the sync hot path faster than once
every ten seconds. This usually indicates Git transport issues, sidecar
corruption, or a misconfigured ref.

```yaml
groups:
  - name: ledgerdb-sync
    interval: 30s
    rules:
      - alert: LedgerDBSyncErrorRateHigh
        expr: rate(ledgerdb_sync_errors_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB sync error rate above 0.1/s on {{ $labels.instance }}"
          description: |
            The watcher on {{ $labels.instance }} is reporting
            {{ $value | humanize }} errors per second over the last 5
            minutes. Inspect logs for the failing sync pass, validate
            the remote refspec, and confirm filesystem health on the
            sidecar volume.
          runbook_url: "https://runbooks.example.com/ledgerdb/sync-errors"
```

### 3. `LedgerDBWatchStalled`

No transactions have been applied for ten minutes *and* replication lag
is increasing. A flat tx-applied rate alone could be a quiet
repository; combined with rising lag it almost always means the watcher
is stuck.

```yaml
groups:
  - name: ledgerdb-stall
    interval: 30s
    rules:
      - alert: LedgerDBWatchStalled
        expr: |
          rate(ledgerdb_tx_applied_total[5m]) == 0
          and
          deriv(ledgerdb_replication_lag_seconds[5m]) > 0
        for: 10m
        labels:
          severity: critical
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB watch appears stalled on {{ $labels.instance }}"
          description: |
            No transactions applied for 10 minutes on
            {{ $labels.instance }} while replication lag is still
            increasing. The watcher is likely blocked on a sync, a
            sidecar transaction, or a stuck CAS retry. Check the
            process state, then restart with verbose logging if needed.
          runbook_url: "https://runbooks.example.com/ledgerdb/watch-stalled"
```

### 4. `LedgerDBCASContention`

CAS retries are climbing. A handful per minute is normal; ten per
second indicates a hot document ID or two writers fighting over the
same collection.

```yaml
groups:
  - name: ledgerdb-cas
    interval: 30s
    rules:
      - alert: LedgerDBCASContention
        expr: rate(ledgerdb_cas_retries_total[5m]) > 10
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-store
        annotations:
          summary: "LedgerDB CAS contention on {{ $labels.instance }}"
          description: |
            CAS retry rate on {{ $labels.instance }} is
            {{ $value | humanize }}/s over the last 5 minutes. The
            current retry budget (5 attempts, 25ms exponential backoff;
            see internal/infra/gitrepo/tx_store.go:28-32) caps total
            wait at ~775ms; sustained contention will surface as user-
            visible write latency. Identify hot document IDs via
            `ledgerdb diag hot-keys`, then either shard the offending
            collection or serialize writes upstream.
          runbook_url: "https://runbooks.example.com/ledgerdb/cas-contention"
```

### 5. `LedgerDBGCPressure`

The loose-object count has climbed above 10,000, which is the
conventional threshold where Git operations start to feel slow.
Schedule `ledgerdb maintenance gc` and consider whether the repository
needs a snapshot.

```yaml
groups:
  - name: ledgerdb-storage
    interval: 1m
    rules:
      - alert: LedgerDBGCPressure
        expr: ledgerdb_loose_objects_count > 10000
        for: 15m
        labels:
          severity: warning
          component: ledgerdb-store
        annotations:
          summary: "LedgerDB loose object count above 10k on {{ $labels.instance }}"
          description: |
            {{ $labels.instance }} has {{ $value | humanize }} loose
            objects. This usually means many small writes since the
            last gc, an aborted import, or a long-running watch with
            no snapshot. Run `ledgerdb maintenance gc`; if the count
            stays high consider `ledgerdb maintenance snapshot` on
            the busiest collection (see docs/PERFORMANCE.md §5).
          runbook_url: "https://runbooks.example.com/ledgerdb/gc-pressure"
```

### 6. `LedgerDBWatchCrash`

Standard liveness alert. The watcher's scrape target has been down for
two minutes; either the process crashed or Prometheus has lost
network reach to it.

```yaml
groups:
  - name: ledgerdb-liveness
    interval: 15s
    rules:
      - alert: LedgerDBWatchCrash
        expr: up{job="ledgerdb-watch"} == 0
        for: 2m
        labels:
          severity: critical
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB watch process down on {{ $labels.instance }}"
          description: |
            Prometheus has been unable to scrape ledgerdb-watch on
            {{ $labels.instance }} for at least 2 minutes. The
            process has likely crashed or the host is unreachable.
            Inspect systemd/journalctl, restart the unit, and capture
            the last 500 lines of logs into the incident ticket.
          runbook_url: "https://runbooks.example.com/ledgerdb/watch-crash"
```

---

## Optional: combined alert group

Some operators prefer one file per service. The following groups all
six rules into a single `groups:` document that can be dropped in
verbatim.

```yaml
groups:
  - name: ledgerdb
    interval: 30s
    rules:
      - alert: LedgerDBReplicationLagHigh
        expr: ledgerdb_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB replication lag above 60s on {{ $labels.instance }}"
          description: "Lag {{ $value | humanizeDuration }} for 5m+. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/replication-lag"

      - alert: LedgerDBSyncErrorRateHigh
        expr: rate(ledgerdb_sync_errors_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB sync error rate above 0.1/s on {{ $labels.instance }}"
          description: "Sync errors at {{ $value | humanize }}/s for 5m+. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/sync-errors"

      - alert: LedgerDBWatchStalled
        expr: |
          rate(ledgerdb_tx_applied_total[5m]) == 0
          and
          deriv(ledgerdb_replication_lag_seconds[5m]) > 0
        for: 10m
        labels:
          severity: critical
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB watch appears stalled on {{ $labels.instance }}"
          description: "No tx applied for 10m and lag rising. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/watch-stalled"

      - alert: LedgerDBCASContention
        expr: rate(ledgerdb_cas_retries_total[5m]) > 10
        for: 5m
        labels:
          severity: warning
          component: ledgerdb-store
        annotations:
          summary: "LedgerDB CAS contention on {{ $labels.instance }}"
          description: "CAS retries at {{ $value | humanize }}/s for 5m+. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/cas-contention"

      - alert: LedgerDBGCPressure
        expr: ledgerdb_loose_objects_count > 10000
        for: 15m
        labels:
          severity: warning
          component: ledgerdb-store
        annotations:
          summary: "LedgerDB loose object count above 10k on {{ $labels.instance }}"
          description: "Loose objects at {{ $value | humanize }} for 15m+. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/gc-pressure"

      - alert: LedgerDBWatchCrash
        expr: up{job="ledgerdb-watch"} == 0
        for: 2m
        labels:
          severity: critical
          component: ledgerdb-watch
        annotations:
          summary: "LedgerDB watch process down on {{ $labels.instance }}"
          description: "Scrape target down for 2m+. See runbook."
          runbook_url: "https://runbooks.example.com/ledgerdb/watch-crash"
```

---

## Tuning notes

- **`for:` durations** are deliberately conservative. Lower them in dev
  and test environments to surface issues faster; raise them on noisy
  hosts to suppress flaps. As a rule of thumb, set `for:` to at least
  twice your scrape interval.
- **Severity mapping** assumes a two-level routing (warning →
  business-hours channel, critical → page). Adjust labels to match
  your Alertmanager routing tree.
- **Per-instance grouping**: all alerts label by `$labels.instance` so
  Alertmanager grouping by `instance` will keep one incident per host
  rather than fanning out.
- **Cardinality**: every rule above is keyed only on `instance` and
  `job`; none introduce high-cardinality labels. Safe to deploy on
  shared Prometheus.
- **False positives to expect**: `LedgerDBWatchStalled` will fire after
  a planned `ledgerdb maintenance snapshot` if the operation runs
  longer than ten minutes. Silence the alert for the maintenance
  window or scope the silence to the affected instance.

## Validation checklist

Before merging changes to these rules into production:

- [ ] `promtool check rules ledgerdb.yaml` returns clean.
- [ ] `promtool test rules` covers at least one fire and one resolve
      transition per rule.
- [ ] Each `runbook_url` resolves to a page with restart, escalation,
      and known-issue sections.
- [ ] The Grafana dashboard at `dashboards/grafana/ledgerdb-watch.json`
      shows the same metrics referenced here, so on-call can pivot
      from alert to dashboard in one click.
