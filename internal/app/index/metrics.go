package index

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics exposes the Prometheus collectors used by the `ledgerdb index watch`
// command and implements the Observer interface so the SyncService can drive
// them with per-tx events.
//
// Metrics is opt-in: it is constructed only when --metrics-addr is provided
// on the watch command. Its collectors are registered against an isolated
// Registry so it never touches Prometheus' global default registry — this
// keeps the CLI well behaved when embedded or invoked repeatedly in tests.
type Metrics struct {
	registry *prometheus.Registry

	txApplied        *prometheus.CounterVec
	syncErrors       *prometheus.CounterVec
	replicationLag   prometheus.Gauge
	syncDuration     prometheus.Histogram
	casRetriesTotal  prometheus.Counter
	lastFetchObserve time.Time
	mu               sync.Mutex
}

// NewMetrics returns a Metrics instance with its collectors registered against
// a private Registry. Construction does not bind any sockets; call
// (*Metrics).Server to obtain an HTTP server.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		txApplied: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ledgerdb_tx_applied_total",
				Help: "Total transactions applied to the SQLite index by ledgerdb index watch.",
			},
			[]string{"collection"},
		),
		syncErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "ledgerdb_sync_errors_total",
				Help: "Total errors encountered while syncing the index, labelled by collection and reason.",
			},
			[]string{"collection", "reason"},
		),
		replicationLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ledgerdb_replication_lag_seconds",
			Help: "Approximate seconds since the last fetch observed new commits. Zero when the index is current.",
		}),
		syncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ledgerdb_index_sync_duration_seconds",
			Help:    "Wall time spent in a single index sync iteration.",
			Buckets: prometheus.DefBuckets,
		}),
		casRetriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ledgerdb_cas_retries_observed_total",
			Help: "Best-effort count of CAS retries observed by ledgerdb index watch.",
		}),
		lastFetchObserve: time.Now(),
	}
	registry.MustRegister(
		metrics.txApplied,
		metrics.syncErrors,
		metrics.replicationLag,
		metrics.syncDuration,
		metrics.casRetriesTotal,
	)
	return metrics
}

// Registry exposes the underlying Prometheus registry for tests and advanced
// embedding scenarios.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// OnTxApplied increments ledgerdb_tx_applied_total for the tx's collection.
func (m *Metrics) OnTxApplied(event TxEvent) {
	collection := event.Collection
	if collection == "" {
		collection = "unknown"
	}
	m.txApplied.WithLabelValues(collection).Inc()
}

// OnSyncError increments ledgerdb_sync_errors_total. Empty collection or
// reason values are normalized to non-empty placeholders so the metric is
// always queryable.
func (m *Metrics) OnSyncError(collection, reason string) {
	if collection == "" {
		collection = "_"
	}
	if reason == "" {
		reason = "unknown"
	}
	m.syncErrors.WithLabelValues(collection, reason).Inc()
}

// OnSyncDuration observes a sync iteration's wall time.
func (m *Metrics) OnSyncDuration(seconds float64) {
	m.syncDuration.Observe(seconds)
}

// OnReplicationFetch updates ledgerdb_replication_lag_seconds. When new commits
// were observed the lag is reset to zero; otherwise it grows monotonically
// since the last fetch that brought in new commits.
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

// ServerConfig configures how Metrics.Server binds.
type ServerConfig struct {
	// Addr is the host:port to bind. The literal "auto" resolves to
	// 127.0.0.1:9090 as a convenience for users who do not care about the
	// exact port.
	Addr string
	// AllowPublic must be true to bind to a non-loopback address. By default
	// the metrics endpoint refuses to bind on 0.0.0.0 or any non-loopback
	// host to follow the loopback-only-by-default posture of LedgerDB v0.2.x.
	AllowPublic bool
}

// ErrPublicBindRefused is returned when ServerConfig.Addr resolves to a
// non-loopback host and AllowPublic was not set.
var ErrPublicBindRefused = errors.New("metrics endpoint refusing to bind on non-loopback host; pass --metrics-allow-public to override")

// Server returns an *http.Server pre-configured to serve /metrics. The caller
// is responsible for calling ListenAndServe and Shutdown. The listener is not
// started here so the caller can integrate with their context lifecycle.
//
// The returned server has Addr already set to the resolved address; pass it
// to net.Listen("tcp", srv.Addr) yourself if you need a custom listener.
func (m *Metrics) Server(cfg ServerConfig) (*http.Server, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if strings.EqualFold(addr, "auto") {
		addr = "127.0.0.1:9090"
	}
	if addr == "" {
		return nil, fmt.Errorf("metrics addr is empty")
	}
	if !cfg.AllowPublic {
		if err := assertLoopback(addr); err != nil {
			return nil, err
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry}))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}, nil
}

// assertLoopback returns nil only when the host portion of addr is a loopback
// address (127.0.0.0/8 or ::1) or the conventional "localhost".
func assertLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid metrics addr %q: %w", addr, err)
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return ErrPublicBindRefused
}

// RunServer is a convenience helper that binds the listener and serves until
// ctx is canceled or ListenAndServe returns an error other than ErrServerClosed.
// It returns the address actually bound (useful when port :0 was requested).
func (m *Metrics) RunServer(ctx context.Context, cfg ServerConfig, onError func(error)) (string, func() error, error) {
	srv, err := m.Server(cfg)
	if err != nil {
		return "", nil, err
	}
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return "", nil, fmt.Errorf("bind metrics endpoint: %w", err)
	}
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && onError != nil {
			onError(err)
		}
	}()
	stop := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
	go func() {
		<-ctx.Done()
		_ = stop()
	}()
	return listener.Addr().String(), stop, nil
}
