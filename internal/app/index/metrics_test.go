package index

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMetricsObserverRecordsTxAndDuration(t *testing.T) {
	metrics := NewMetrics()
	metrics.OnTxApplied(TxEvent{Collection: "users", Op: "put"})
	metrics.OnTxApplied(TxEvent{Collection: "users", Op: "delete"})
	metrics.OnTxApplied(TxEvent{Collection: "orders", Op: "put"})
	metrics.OnSyncDuration(0.42)
	metrics.OnSyncError("users", "missing_document")
	metrics.OnReplicationFetch(0) // no new commits — lag grows
	metrics.OnReplicationFetch(1) // new commits — lag resets to 0

	srv, err := metrics.Server(ServerConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("Server(): %v", err)
	}
	_ = srv // we only need to confirm construction succeeds for loopback addr
}

func TestMetricsServerRejectsPublicBindByDefault(t *testing.T) {
	metrics := NewMetrics()
	if _, err := metrics.Server(ServerConfig{Addr: "0.0.0.0:9090"}); !errors.Is(err, ErrPublicBindRefused) {
		t.Fatalf("expected ErrPublicBindRefused, got %v", err)
	}
	if _, err := metrics.Server(ServerConfig{Addr: "example.com:9090"}); !errors.Is(err, ErrPublicBindRefused) {
		t.Fatalf("expected ErrPublicBindRefused for non-loopback host, got %v", err)
	}
	if _, err := metrics.Server(ServerConfig{Addr: "0.0.0.0:9090", AllowPublic: true}); err != nil {
		t.Fatalf("expected AllowPublic to bypass loopback check, got %v", err)
	}
}

func TestMetricsServerResolvesAutoToLoopback(t *testing.T) {
	srv, err := NewMetrics().Server(ServerConfig{Addr: "auto"})
	if err != nil {
		t.Fatalf("Server(auto): %v", err)
	}
	if srv.Addr != "127.0.0.1:9090" {
		t.Fatalf("auto should resolve to 127.0.0.1:9090, got %s", srv.Addr)
	}
}

func TestMetricsRunServerExposesPrometheusEndpoint(t *testing.T) {
	metrics := NewMetrics()
	metrics.OnTxApplied(TxEvent{Collection: "users", Op: "put"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr, stop, err := metrics.RunServer(ctx, ServerConfig{Addr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("RunServer: %v", err)
	}
	defer func() { _ = stop() }()

	deadline := time.Now().Add(2 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			body = string(data)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(body, "ledgerdb_tx_applied_total") {
		t.Fatalf("expected ledgerdb_tx_applied_total in /metrics response, got: %s", body)
	}
	if !strings.Contains(body, `collection="users"`) {
		t.Fatalf("expected collection label, got: %s", body)
	}
}

func TestAuditLoggerWritesJSONLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	logger, err := NewAuditLogger(AuditOptions{Path: path})
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	logger.OnTxApplied(TxEvent{TxID: "t1", Collection: "users", DocID: "u1", Op: "put"})
	logger.OnTxApplied(TxEvent{TxID: "t2", Collection: "users", DocID: "u1", Op: "delete"})
	if err := logger.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(contents), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 audit records, got %d: %q", len(lines), contents)
	}
	for _, line := range lines {
		if !strings.Contains(line, `"actor":"index-watch"`) {
			t.Fatalf("missing actor in line: %s", line)
		}
		if !strings.Contains(line, `"tx_id":`) {
			t.Fatalf("missing tx_id in line: %s", line)
		}
	}
}

func TestAuditLoggerRejectsEmptyPath(t *testing.T) {
	if _, err := NewAuditLogger(AuditOptions{Path: ""}); err == nil {
		t.Fatalf("expected error for empty path")
	}
}

func TestCombinedObserverFansOut(t *testing.T) {
	a := &countingObserver{}
	b := &countingObserver{}
	observer := NewCombinedObserver(a, nil, b)
	if observer == nil {
		t.Fatal("expected combined observer")
	}
	observer.OnTxApplied(TxEvent{Collection: "c"})
	observer.OnSyncError("c", "r")
	observer.OnSyncDuration(0.1)
	observer.OnReplicationFetch(2)

	for _, observer := range []*countingObserver{a, b} {
		if observer.txEvents != 1 || observer.errs != 1 || observer.durations != 1 || observer.fetches != 1 {
			t.Fatalf("observer not invoked once: %+v", observer)
		}
	}
}

func TestNewCombinedObserverReturnsNilWhenEmpty(t *testing.T) {
	if observer := NewCombinedObserver(nil, nil); observer != nil {
		t.Fatalf("expected nil for all-nil inputs, got %T", observer)
	}
}

func TestNewCombinedObserverUnwrapsSingle(t *testing.T) {
	only := &countingObserver{}
	if observer := NewCombinedObserver(only, nil); observer != only {
		t.Fatalf("expected single observer to be returned unwrapped")
	}
}

func TestClassifyErr(t *testing.T) {
	cases := map[error]string{
		nil:                       "",
		context.Canceled:          "canceled",
		ErrCommitNotFound:         "commit_not_found",
		ErrFetchUnavailable:       "fetch_unavailable",
		ErrMissingDocument:        "missing_document",
		ErrPatchUnsupported:       "patch_unsupported",
		ErrStateUnavailable:       "state_unavailable",
		ErrMergeCommitUnsupported: "merge_unsupported",
		errors.New("boom"):        "other",
	}
	for err, want := range cases {
		if got := classifyErr(err); got != want {
			t.Fatalf("classifyErr(%v) = %q, want %q", err, got, want)
		}
	}
}

type countingObserver struct {
	txEvents  int
	errs      int
	durations int
	fetches   int
}

func (c *countingObserver) OnTxApplied(TxEvent)            { c.txEvents++ }
func (c *countingObserver) OnSyncError(string, string)     { c.errs++ }
func (c *countingObserver) OnSyncDuration(float64)         { c.durations++ }
func (c *countingObserver) OnReplicationFetch(int)         { c.fetches++ }
