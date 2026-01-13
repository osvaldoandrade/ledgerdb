package doc

import (
	"context"
	"errors"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type GetService struct {
	store   ReadStore
	decoder Decoder
	hasher  Hasher
	patcher Patcher
	layout  domain.StreamLayout
}

func NewGetService(store ReadStore, decoder Decoder, hasher Hasher, patcher Patcher, layout domain.StreamLayout) *GetService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	return &GetService{
		store:   store,
		decoder: decoder,
		hasher:  hasher,
		patcher: patcher,
		layout:  layout,
	}
}

func (s *GetService) Get(ctx context.Context, repoPath, collection, docID string) (GetResult, error) {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return GetResult{}, ErrCollectionRequired
	}
	if !domain.IsValidCollectionName(collection) {
		return GetResult{}, ErrInvalidCollection
	}

	docID = strings.TrimSpace(docID)
	if docID == "" {
		return GetResult{}, ErrDocIDRequired
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return GetResult{}, err
	}

	streamPath := domain.StreamPath(s.layout, collection, docID)
	statePath := domain.StatePath(s.layout, collection, docID)

	stateBlob, err := s.store.LoadHeadTx(ctx, absRepoPath, statePath)
	if err != nil && !errors.Is(err, ErrDocNotFound) {
		return GetResult{}, err
	}
	if err == nil && len(stateBlob.Bytes) > 0 {
		stateTx, err := s.decoder.Decode(stateBlob.Bytes)
		if err != nil {
			return GetResult{}, err
		}
		switch stateTx.Op {
		case domain.TxOpDelete:
			return GetResult{}, ErrDocDeleted
		case domain.TxOpPut, domain.TxOpMerge:
			if len(stateTx.Snapshot) > 0 {
				return GetResult{
					Payload: stateTx.Snapshot,
					TxHash:  s.hasher.SumHex(stateBlob.Bytes),
					TxID:    stateTx.TxID,
					Op:      stateTx.Op,
				}, nil
			}
		}
	}
	headHash, err := s.store.LoadStreamHead(ctx, absRepoPath, streamPath)
	if err != nil {
		return GetResult{}, err
	}
	if headHash == "" {
		return GetResult{}, ErrDocNotFound
	}

	txBlobs, err := s.store.LoadStreamTxs(ctx, absRepoPath, streamPath)
	if err != nil {
		return GetResult{}, err
	}

	index, err := buildTxIndex(txBlobs, s.decoder, s.hasher)
	if err != nil {
		return GetResult{}, err
	}

	chain, err := buildTxChain(headHash, index)
	if err != nil {
		return GetResult{}, err
	}

	doc, headTx, err := rehydrateChain(ctx, chain, s.patcher)
	if err != nil {
		return GetResult{}, err
	}

	return GetResult{
		Payload: doc,
		TxHash:  headHash,
		TxID:    headTx.TxID,
		Op:      headTx.Op,
	}, nil
}
