package ledgerdbsdk

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	indexapp "github.com/osvaldoandrade/ledgerdb/internal/app/index"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/canonicaljson"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/jsonpatch"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/sqliteindex"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
)

type IndexSyncResult struct {
	Reset        bool
	Fetched      bool
	Commits      int
	TxsApplied   int
	DocsUpserted int
	DocsDeleted  int
	Collections  int
	LastCommit   string
}

type IndexedDoc struct {
	DocID             string
	Payload           json.RawMessage
	TxHash            string
	TxID              string
	Op                string
	SchemaVersion     string
	UpdatedAtUnixNano int64
	Deleted           bool
}

// OpenIndex opens the SQLite index database.
func (c *Client) OpenIndex(ctx context.Context) error {
	c.mu.Lock()
	if c.indexStore != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	store, err := sqliteindex.OpenWithOptions(c.cfg.Index.DBPath, sqliteindex.OpenOptions{Fast: c.cfg.Index.Fast})
	if err != nil {
		return err
	}
	if err := store.DB().PingContext(ctx); err != nil {
		_ = store.Close()
		return fmt.Errorf("ping sqlite: %w", err)
	}

	c.mu.Lock()
	c.indexStore = store
	c.db = store.DB()
	c.mu.Unlock()
	return nil
}

// DB exposes the SQLite database handle.
func (c *Client) DB() (*sql.DB, error) {
	return c.indexDB()
}

// Query runs a SQL query against the SQLite sidecar.
func (c *Client) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := c.indexDB()
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, query, args...)
}

// GetIndexed reads a document from the SQLite sidecar (key-value path).
func (c *Client) GetIndexed(ctx context.Context, collection, docID string) (IndexedDoc, error) {
	db, err := c.indexDB()
	if err != nil {
		return IndexedDoc{}, err
	}
	table := tableNameForCollection(collection)
	stmt := fmt.Sprintf(
		`SELECT doc_id, payload, tx_hash, tx_id, op, schema_version, updated_at, deleted FROM %s WHERE doc_id = ?`,
		quoteIdent(table),
	)
	var payload []byte
	var schemaVersion sql.NullString
	var updatedAt int64
	var deleted int
	var record IndexedDoc
	err = db.QueryRowContext(ctx, stmt, docID).Scan(
		&record.DocID,
		&payload,
		&record.TxHash,
		&record.TxID,
		&record.Op,
		&schemaVersion,
		&updatedAt,
		&deleted,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexedDoc{}, ErrNotFound
		}
		return IndexedDoc{}, err
	}
	if schemaVersion.Valid {
		record.SchemaVersion = schemaVersion.String
	}
	record.Payload = payload
	record.UpdatedAtUnixNano = updatedAt
	record.Deleted = deleted == 1
	return record, nil
}

// GetIndexedInto reads a document from the SQLite sidecar and unmarshals it.
func (c *Client) GetIndexedInto(ctx context.Context, collection, docID string, target any) (IndexedDoc, error) {
	doc, err := c.GetIndexed(ctx, collection, docID)
	if err != nil {
		return IndexedDoc{}, err
	}
	if len(doc.Payload) > 0 {
		if err := json.Unmarshal(doc.Payload, target); err != nil {
			return IndexedDoc{}, err
		}
	}
	return doc, nil
}

// SyncIndex runs a single index sync.
func (c *Client) SyncIndex(ctx context.Context) (IndexSyncResult, error) {
	service, opts, err := c.indexSyncService()
	if err != nil {
		return IndexSyncResult{}, err
	}
	result, err := service.Sync(ctx, c.cfg.RepoPath, opts)
	if err != nil {
		return IndexSyncResult{}, err
	}
	return IndexSyncResult{
		Reset:        result.Reset,
		Fetched:      result.Fetched,
		Commits:      result.Commits,
		TxsApplied:   result.TxsApplied,
		DocsUpserted: result.DocsUpserted,
		DocsDeleted:  result.DocsDeleted,
		Collections:  result.Collections,
		LastCommit:   result.LastCommit,
	}, nil
}

// StartIndexWatch starts a polling loop to keep SQLite in sync.
func (c *Client) StartIndexWatch(ctx context.Context) error {
	if c.cfg.Index.Interval <= 0 {
		return fmt.Errorf("index watch interval must be > 0")
	}
	if c.cfg.Index.Jitter < 0 {
		return fmt.Errorf("index watch jitter must be >= 0")
	}
	c.watchMu.Lock()
	if c.watchCancel != nil {
		c.watchMu.Unlock()
		return ErrWatchRunning
	}
	c.watchMu.Unlock()

	service, opts, err := c.indexSyncService()
	if err != nil {
		return err
	}

	watchCtx, cancel := context.WithCancel(ctx)
	results := make(chan IndexSyncResult, 1)
	errs := make(chan error, 1)

	go func() {
		defer close(results)
		defer close(errs)

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			result, err := service.Sync(watchCtx, c.cfg.RepoPath, opts)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errs <- err
				}
				return
			}

			if c.cfg.Index.EmitResults && (!c.cfg.Index.OnlyChanges || hasIndexChanges(result)) {
				results <- IndexSyncResult{
					Reset:        result.Reset,
					Fetched:      result.Fetched,
					Commits:      result.Commits,
					TxsApplied:   result.TxsApplied,
					DocsUpserted: result.DocsUpserted,
					DocsDeleted:  result.DocsDeleted,
					Collections:  result.Collections,
					LastCommit:   result.LastCommit,
				}
			}

			wait := c.cfg.Index.Interval
			if c.cfg.Index.Jitter > 0 {
				wait += time.Duration(rng.Int63n(int64(c.cfg.Index.Jitter)))
			}
			timer := time.NewTimer(wait)
			select {
			case <-watchCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()

	c.watchMu.Lock()
	c.watchCancel = cancel
	c.watchErr = errs
	c.watchResults = results
	c.watchMu.Unlock()
	return nil
}

// WatchResults returns a channel that emits index sync summaries.
func (c *Client) WatchResults() <-chan IndexSyncResult {
	c.watchMu.Lock()
	defer c.watchMu.Unlock()
	return c.watchResults
}

// WatchErrors returns a channel that emits watch errors.
func (c *Client) WatchErrors() <-chan error {
	c.watchMu.Lock()
	defer c.watchMu.Unlock()
	return c.watchErr
}

// StopIndexWatch stops the watch loop.
func (c *Client) StopIndexWatch() error {
	c.watchMu.Lock()
	cancel := c.watchCancel
	errs := c.watchErr
	c.watchCancel = nil
	c.watchErr = nil
	c.watchResults = nil
	c.watchMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if errs != nil {
		if err := <-errs; err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func (c *Client) indexSyncService() (*indexapp.SyncService, indexapp.SyncOptions, error) {
	store, err := c.ensureIndexStore()
	if err != nil {
		return nil, indexapp.SyncOptions{}, err
	}
	opts, err := c.syncOptions()
	if err != nil {
		return nil, indexapp.SyncOptions{}, err
	}
	service := indexapp.NewSyncService(
		c.store,
		c.store,
		store,
		canonicaljson.Canonicalizer{},
		txv3.Decoder{},
		jsonpatch.Patcher{},
		hash.SHA256{},
	)
	return service, opts, nil
}

func hasIndexChanges(result indexapp.SyncResult) bool {
	return result.Commits > 0 || result.TxsApplied > 0 || result.DocsUpserted > 0 || result.DocsDeleted > 0
}

func tableNameForCollection(collection string) string {
	return "collection_" + collection
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
