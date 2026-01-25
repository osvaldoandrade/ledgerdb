package ledgerdbsdk

import (
	"context"
	"encoding/json"
	"errors"

	docapp "github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/canonicaljson"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/ident"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/jsonpatch"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
)

type PutResult struct {
	CommitHash string
	TxHash     string
	TxID       string
}

type Doc struct {
	Payload json.RawMessage
	TxHash  string
	TxID    string
	Op      string
}

type DocMeta struct {
	TxHash string
	TxID   string
	Op     string
}

type LogEntry struct {
	TxID       string
	TxHash     string
	ParentHash string
	Timestamp  int64
	Op         string
}

type RevertOptions struct {
	TxID   string
	TxHash string
}

// Get reads a document directly from the ledger (key-value path).
func (c *Client) Get(ctx context.Context, collection, docID string) (Doc, error) {
	service := docapp.NewGetService(c.store, txv3.Decoder{}, hash.SHA256{}, jsonpatch.Patcher{}, c.layout)
	result, err := service.Get(ctx, c.cfg.RepoPath, collection, docID)
	if err != nil {
		return Doc{}, mapDocErr(err)
	}
	return Doc{
		Payload: result.Payload,
		TxHash:  result.TxHash,
		TxID:    result.TxID,
		Op:      result.Op.String(),
	}, nil
}

// GetInto reads a document and unmarshals into the provided target.
func (c *Client) GetInto(ctx context.Context, collection, docID string, target any) (DocMeta, error) {
	doc, err := c.Get(ctx, collection, docID)
	if err != nil {
		return DocMeta{}, err
	}
	if len(doc.Payload) > 0 {
		if err := json.Unmarshal(doc.Payload, target); err != nil {
			return DocMeta{}, err
		}
	}
	return DocMeta{TxHash: doc.TxHash, TxID: doc.TxID, Op: doc.Op}, nil
}

// Put writes a full snapshot payload (JSON bytes).
func (c *Client) Put(ctx context.Context, collection, docID string, payload []byte) (PutResult, error) {
	idGen := ident.NewULIDGenerator()
	service := docapp.NewPutService(
		c.store,
		canonicaljson.Canonicalizer{},
		txv3.Encoder{},
		hash.SHA256{},
		platform.RealClock{},
		idGen,
		c.layout,
		c.historyMode,
	)
	result, err := c.withAutoSync(ctx, func() (docapp.PutResult, error) {
		return service.Put(ctx, c.cfg.RepoPath, collection, docID, payload)
	})
	if err != nil {
		return PutResult{}, mapDocErr(err)
	}
	return PutResult{CommitHash: result.CommitHash, TxHash: result.TxHash, TxID: result.TxID}, nil
}

// PutJSON marshals a Go value and writes it as a document snapshot.
func (c *Client) PutJSON(ctx context.Context, collection, docID string, value any) (PutResult, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return PutResult{}, err
	}
	return c.Put(ctx, collection, docID, payload)
}

// Patch applies JSON Patch operations (RFC 6902).
func (c *Client) Patch(ctx context.Context, collection, docID string, ops []byte) (PutResult, error) {
	idGen := ident.NewULIDGenerator()
	service := docapp.NewPatchService(
		c.store,
		c.store,
		canonicaljson.Canonicalizer{},
		txv3.Encoder{},
		txv3.Decoder{},
		jsonpatch.Patcher{},
		hash.SHA256{},
		platform.RealClock{},
		idGen,
		c.layout,
		c.historyMode,
	)
	result, err := c.withAutoSync(ctx, func() (docapp.PutResult, error) {
		return service.Patch(ctx, c.cfg.RepoPath, collection, docID, ops)
	})
	if err != nil {
		return PutResult{}, mapDocErr(err)
	}
	return PutResult{CommitHash: result.CommitHash, TxHash: result.TxHash, TxID: result.TxID}, nil
}

// PatchJSON marshals patch operations and applies them.
func (c *Client) PatchJSON(ctx context.Context, collection, docID string, ops any) (PutResult, error) {
	payload, err := json.Marshal(ops)
	if err != nil {
		return PutResult{}, err
	}
	return c.Patch(ctx, collection, docID, payload)
}

// Delete marks a document as deleted (tombstone).
func (c *Client) Delete(ctx context.Context, collection, docID string) (PutResult, error) {
	idGen := ident.NewULIDGenerator()
	service := docapp.NewDeleteService(
		c.store,
		c.store,
		txv3.Encoder{},
		txv3.Decoder{},
		hash.SHA256{},
		platform.RealClock{},
		idGen,
		c.layout,
		c.historyMode,
	)
	result, err := c.withAutoSync(ctx, func() (docapp.PutResult, error) {
		return service.Delete(ctx, c.cfg.RepoPath, collection, docID)
	})
	if err != nil {
		return PutResult{}, mapDocErr(err)
	}
	return PutResult{CommitHash: result.CommitHash, TxHash: result.TxHash, TxID: result.TxID}, nil
}

// Revert rewinds a document to a previous transaction.
func (c *Client) Revert(ctx context.Context, collection, docID string, opts RevertOptions) (PutResult, error) {
	idGen := ident.NewULIDGenerator()
	service := docapp.NewRevertService(
		c.store,
		c.store,
		canonicaljson.Canonicalizer{},
		txv3.Encoder{},
		txv3.Decoder{},
		jsonpatch.Patcher{},
		hash.SHA256{},
		platform.RealClock{},
		idGen,
		c.layout,
		c.historyMode,
	)
	result, err := c.withAutoSync(ctx, func() (docapp.PutResult, error) {
		return service.Revert(ctx, c.cfg.RepoPath, collection, docID, docapp.RevertOptions{
			TxID:   opts.TxID,
			TxHash: opts.TxHash,
		})
	})
	if err != nil {
		return PutResult{}, mapDocErr(err)
	}
	return PutResult{CommitHash: result.CommitHash, TxHash: result.TxHash, TxID: result.TxID}, nil
}

// Log returns the transaction history for a document.
func (c *Client) Log(ctx context.Context, collection, docID string) ([]LogEntry, error) {
	service := docapp.NewLogService(c.store, txv3.Decoder{}, hash.SHA256{}, c.layout)
	entries, err := service.Log(ctx, c.cfg.RepoPath, collection, docID)
	if err != nil {
		return nil, mapDocErr(err)
	}
	out := make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, LogEntry{
			TxID:       entry.TxID,
			TxHash:     entry.TxHash,
			ParentHash: entry.ParentHash,
			Timestamp:  entry.Timestamp,
			Op:         entry.Op.String(),
		})
	}
	return out, nil
}

// Fetch pulls remote updates into the local repo.
func (c *Client) Fetch(ctx context.Context) error {
	return c.store.Fetch(ctx, c.cfg.RepoPath)
}

// Push sends local commits to the remote.
func (c *Client) Push(ctx context.Context) error {
	return c.store.Push(ctx, c.cfg.RepoPath)
}

func (c *Client) withAutoSync(ctx context.Context, fn func() (docapp.PutResult, error)) (docapp.PutResult, error) {
	if c.cfg.AutoSync {
		if err := c.store.Fetch(ctx, c.cfg.RepoPath); err != nil {
			return docapp.PutResult{}, err
		}
	}
	result, err := fn()
	if err != nil {
		return docapp.PutResult{}, err
	}
	if c.cfg.AutoSync {
		if err := c.store.Push(ctx, c.cfg.RepoPath); err != nil {
			return docapp.PutResult{}, err
		}
	}
	return result, nil
}

func mapDocErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, docapp.ErrDocNotFound) {
		return ErrNotFound
	}
	return err
}
