package doc

import (
	"context"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// DefaultLogPageLimit is the page size used when callers do not set Limit.
const DefaultLogPageLimit = 100

type LogService struct {
	store   ReadStore
	decoder Decoder
	hasher  Hasher
	layout  domain.StreamLayout
}

// LogOptions configures cursor-based pagination for LogService.LogPaginated.
type LogOptions struct {
	// Limit caps the number of entries returned in this page. Values <= 0 use
	// DefaultLogPageLimit.
	Limit int
	// Cursor is an opaque token returned by a previous page. Empty means start
	// at the head (newest entry).
	Cursor string
}

// LogPage is the result of a paginated log call.
type LogPage struct {
	Entries    []LogEntry
	NextCursor string
}

func NewLogService(store ReadStore, decoder Decoder, hasher Hasher, layout domain.StreamLayout) *LogService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	return &LogService{
		store:   store,
		decoder: decoder,
		hasher:  hasher,
		layout:  layout,
	}
}

// Log returns the full transaction history for a document, head-first.
func (s *LogService) Log(ctx context.Context, repoPath, collection, docID string) ([]LogEntry, error) {
	return s.collectEntries(ctx, repoPath, collection, docID)
}

// LogPaginated returns a slice of the transaction history bounded by opts.Limit
// and anchored at opts.Cursor. Entries are emitted head-first (newest first).
//
// The cursor is opaque (base64(JSON)). It is stable across calls: as long as
// the underlying tx chain is not rewritten (history-mode append), the same
// cursor will resume on the same boundary.
func (s *LogService) LogPaginated(ctx context.Context, repoPath, collection, docID string, opts LogOptions) (LogPage, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultLogPageLimit
	}

	entries, err := s.collectEntries(ctx, repoPath, collection, docID)
	if err != nil {
		return LogPage{}, err
	}

	start := 0
	if opts.Cursor != "" {
		anchor, err := decodeLogCursor(opts.Cursor)
		if err != nil {
			return LogPage{}, err
		}
		// Skip everything up to and including the cursor entry.
		idx := findEntryByHash(entries, anchor.TxHash)
		if idx < 0 {
			return LogPage{}, ErrInvalidCursor
		}
		start = idx + 1
	}

	if start >= len(entries) {
		return LogPage{Entries: []LogEntry{}}, nil
	}

	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}

	page := LogPage{Entries: entries[start:end]}
	if end < len(entries) {
		page.NextCursor = encodeLogCursor(entries[end-1])
	}
	return page, nil
}

func (s *LogService) collectEntries(ctx context.Context, repoPath, collection, docID string) ([]LogEntry, error) {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return nil, ErrCollectionRequired
	}
	if !domain.IsValidCollectionName(collection) {
		return nil, ErrInvalidCollection
	}

	docID = strings.TrimSpace(docID)
	if docID == "" {
		return nil, ErrDocIDRequired
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return nil, err
	}

	streamPath := domain.StreamPath(s.layout, collection, docID)
	headHash, err := s.store.LoadStreamHead(ctx, absRepoPath, streamPath)
	if err != nil {
		return nil, err
	}
	if headHash == "" {
		return nil, ErrDocNotFound
	}

	txBlobs, err := s.store.LoadStreamTxs(ctx, absRepoPath, streamPath)
	if err != nil {
		return nil, err
	}

	index, err := buildTxIndex(txBlobs, s.decoder, s.hasher)
	if err != nil {
		return nil, err
	}

	chain, err := buildTxChain(headHash, index)
	if err != nil {
		return nil, err
	}

	ordered := make([]LogEntry, 0, len(chain))
	for _, entry := range chain {
		ordered = append(ordered, LogEntry{
			TxID:       entry.Tx.TxID,
			TxHash:     entry.Hash,
			ParentHash: entry.Tx.ParentHash,
			Timestamp:  entry.Tx.Timestamp,
			Op:         entry.Tx.Op,
		})
	}

	return ordered, nil
}

func findEntryByHash(entries []LogEntry, txHash string) int {
	for i := range entries {
		if entries[i].TxHash == txHash {
			return i
		}
	}
	return -1
}
