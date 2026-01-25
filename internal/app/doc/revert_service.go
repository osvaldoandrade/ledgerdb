package doc

import (
	"context"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type RevertService struct {
	readStore   ReadStore
	writeStore  WriteStore
	canonical   Canonicalizer
	encoder     Encoder
	decoder     Decoder
	patcher     Patcher
	hasher      Hasher
	clock       Clock
	idGen       IDGenerator
	layout      domain.StreamLayout
	historyMode domain.HistoryMode
}

func NewRevertService(readStore ReadStore, writeStore WriteStore, canonical Canonicalizer, encoder Encoder, decoder Decoder, patcher Patcher, hasher Hasher, clock Clock, idGen IDGenerator, layout domain.StreamLayout, historyMode domain.HistoryMode) *RevertService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	historyMode = domain.NormalizeHistoryMode(historyMode)
	return &RevertService{
		readStore:   readStore,
		writeStore:  writeStore,
		canonical:   canonical,
		encoder:     encoder,
		decoder:     decoder,
		patcher:     patcher,
		hasher:      hasher,
		clock:       clock,
		idGen:       idGen,
		layout:      layout,
		historyMode: historyMode,
	}
}

func (s *RevertService) Revert(ctx context.Context, repoPath, collection, docID string, opts RevertOptions) (PutResult, error) {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return PutResult{}, ErrCollectionRequired
	}
	if !domain.IsValidCollectionName(collection) {
		return PutResult{}, ErrInvalidCollection
	}

	docID = strings.TrimSpace(docID)
	if docID == "" {
		return PutResult{}, ErrDocIDRequired
	}

	txID := strings.TrimSpace(opts.TxID)
	txHash := strings.TrimSpace(opts.TxHash)
	if txID == "" && txHash == "" {
		return PutResult{}, ErrTxReferenceRequired
	}
	if txID != "" && txHash != "" {
		return PutResult{}, ErrTxReferenceAmbiguous
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return PutResult{}, err
	}

	streamPath := domain.StreamPath(s.layout, collection, docID)
	headHash, err := s.readStore.LoadStreamHead(ctx, absRepoPath, streamPath)
	if err != nil {
		return PutResult{}, err
	}
	if headHash == "" {
		return PutResult{}, ErrDocNotFound
	}

	txBlobs, err := s.readStore.LoadStreamTxs(ctx, absRepoPath, streamPath)
	if err != nil {
		return PutResult{}, err
	}

	index, err := buildTxIndex(txBlobs, s.decoder, s.hasher)
	if err != nil {
		return PutResult{}, err
	}

	targetHash, err := selectTargetHash(index, txID, txHash)
	if err != nil {
		return PutResult{}, err
	}

	targetEntry := index[targetHash]
	if targetEntry.Tx.Op == domain.TxOpDelete {
		deleteSvc := NewDeleteService(s.writeStore, s.readStore, s.encoder, s.decoder, s.hasher, s.clock, s.idGen, s.layout, s.historyMode)
		return deleteSvc.Delete(ctx, absRepoPath, collection, docID)
	}

	chain, err := buildTxChain(targetHash, index)
	if err != nil {
		return PutResult{}, err
	}

	doc, _, err := rehydrateChain(ctx, chain, s.patcher)
	if err != nil {
		return PutResult{}, err
	}

	putSvc := NewPutService(s.writeStore, s.canonical, s.encoder, s.hasher, s.clock, s.idGen, s.layout, s.historyMode)
	return putSvc.Put(ctx, absRepoPath, collection, docID, doc)
}

func selectTargetHash(index map[string]txChainEntry, txID, txHash string) (string, error) {
	if txHash != "" {
		if _, ok := index[txHash]; ok {
			return txHash, nil
		}
		return "", ErrTxNotFound
	}

	var match string
	for hash, entry := range index {
		if entry.Tx.TxID != txID {
			continue
		}
		if match != "" {
			return "", ErrTxReferenceAmbiguous
		}
		match = hash
	}
	if match == "" {
		return "", ErrTxNotFound
	}
	return match, nil
}
