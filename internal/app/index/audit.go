package index

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// auditActor is the fixed actor value emitted by the watch command.
const auditActor = "index-watch"

// AuditRecord is the JSON Lines envelope emitted for every applied tx when
// --audit-log is set. Fields are kept short to keep the file compact.
type AuditRecord struct {
	Timestamp  string `json:"ts"`
	TxID       string `json:"tx_id"`
	Collection string `json:"collection"`
	DocID      string `json:"doc_id"`
	Op         string `json:"op"`
	Actor      string `json:"actor"`
}

// AuditLogger writes per-tx audit records to a buffered writer and flushes on
// a fixed interval. It implements Observer's OnTxApplied callback; the other
// Observer hooks are no-ops on this type so it can be composed with Metrics
// via a Combine adapter.
//
// AuditLogger is safe for use from a single goroutine. The flush loop runs in
// the background until Close or the context passed to Start is canceled.
type AuditLogger struct {
	mu        sync.Mutex
	writer    *bufio.Writer
	closer    io.Closer
	stop      chan struct{}
	done      chan struct{}
	started   atomic.Bool
	startOnce sync.Once
	stopOnce  sync.Once
	clock     func() time.Time
}

// AuditOptions controls AuditLogger behavior.
type AuditOptions struct {
	// Path is the output destination. "-" means stdout; any other value is
	// opened with O_APPEND|O_CREATE|O_WRONLY mode 0o644.
	Path string
	// FlushInterval defaults to 1 second when zero.
	FlushInterval time.Duration
}

// NewAuditLogger opens the configured destination and returns a logger. The
// caller must invoke Start to begin background flushing and Close to release
// resources.
func NewAuditLogger(opts AuditOptions) (*AuditLogger, error) {
	var writer io.Writer
	var closer io.Closer
	switch opts.Path {
	case "":
		return nil, fmt.Errorf("audit log path is empty")
	case "-":
		writer = os.Stdout
	default:
		file, err := os.OpenFile(opts.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open audit log %q: %w", opts.Path, err)
		}
		writer = file
		closer = file
	}
	return &AuditLogger{
		writer: bufio.NewWriter(writer),
		closer: closer,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		clock:  time.Now,
	}, nil
}

// Start launches the background flush goroutine. It returns immediately. Pass
// the watch command's context so audit flushing stops when the command exits.
// Calling Start more than once is a no-op after the first invocation.
func (a *AuditLogger) Start(ctx context.Context, flushInterval time.Duration) {
	a.startOnce.Do(func() {
		a.started.Store(true)
		if flushInterval <= 0 {
			flushInterval = time.Second
		}
		go func() {
			defer close(a.done)
			ticker := time.NewTicker(flushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-a.stop:
					return
				case <-ticker.C:
					_ = a.Flush()
				}
			}
		}()
	})
}

// OnTxApplied writes a JSON Lines record for the given event.
func (a *AuditLogger) OnTxApplied(event TxEvent) {
	record := AuditRecord{
		Timestamp:  a.clock().UTC().Format(time.RFC3339Nano),
		TxID:       event.TxID,
		Collection: event.Collection,
		DocID:      event.DocID,
		Op:         event.Op,
		Actor:      auditActor,
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.writer.Write(encoded)
	_ = a.writer.WriteByte('\n')
}

// OnSyncError satisfies Observer. Sync-level errors are surfaced via metrics
// and slog; we deliberately do not write them to the audit log so the file
// stays a faithful record of applied txs only.
func (a *AuditLogger) OnSyncError(string, string) {}

// OnSyncDuration satisfies Observer.
func (a *AuditLogger) OnSyncDuration(float64) {}

// OnReplicationFetch satisfies Observer.
func (a *AuditLogger) OnReplicationFetch(int) {}

// Flush writes any buffered records to the underlying destination.
func (a *AuditLogger) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.writer.Flush()
}

// Close stops the background flusher, flushes pending writes, and closes the
// underlying file (if any). Close is safe to call multiple times.
func (a *AuditLogger) Close() error {
	a.stopOnce.Do(func() { close(a.stop) })
	if a.started.Load() {
		<-a.done
	}
	if err := a.Flush(); err != nil {
		return err
	}
	if a.closer != nil {
		return a.closer.Close()
	}
	return nil
}

// CombinedObserver fans out Observer callbacks to multiple observers. The
// order of notification matches the order observers were supplied.
type CombinedObserver struct {
	observers []Observer
}

// NewCombinedObserver returns an Observer that fans out to all non-nil
// supplied observers. When no observers are supplied it returns nil so the
// caller can skip SetObserver entirely.
func NewCombinedObserver(observers ...Observer) Observer {
	filtered := make([]Observer, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			filtered = append(filtered, observer)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) == 1 {
		return filtered[0]
	}
	return &CombinedObserver{observers: filtered}
}

func (c *CombinedObserver) OnTxApplied(event TxEvent) {
	for _, observer := range c.observers {
		observer.OnTxApplied(event)
	}
}

func (c *CombinedObserver) OnSyncError(collection, reason string) {
	for _, observer := range c.observers {
		observer.OnSyncError(collection, reason)
	}
}

func (c *CombinedObserver) OnSyncDuration(seconds float64) {
	for _, observer := range c.observers {
		observer.OnSyncDuration(seconds)
	}
}

func (c *CombinedObserver) OnReplicationFetch(commitsObserved int) {
	for _, observer := range c.observers {
		observer.OnReplicationFetch(commitsObserved)
	}
}
