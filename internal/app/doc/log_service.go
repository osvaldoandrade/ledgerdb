package doc

import (
	"context"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type LogService struct {
	store   ReadStore
	decoder Decoder
	hasher  Hasher
	layout  domain.StreamLayout
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

func (s *LogService) Log(ctx context.Context, repoPath, collection, docID string) ([]LogEntry, error) {
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
