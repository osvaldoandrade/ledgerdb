package doc

import (
	"context"
	"errors"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type PatchService struct {
	writeStore    WriteStore
	readStore     ReadStore
	canonicalizer Canonicalizer
	encoder       Encoder
	decoder       Decoder
	patcher       Patcher
	hasher        Hasher
	clock         Clock
	idGen         IDGenerator
	layout        domain.StreamLayout
	historyMode   domain.HistoryMode
}

func NewPatchService(writeStore WriteStore, readStore ReadStore, canonicalizer Canonicalizer, encoder Encoder, decoder Decoder, patcher Patcher, hasher Hasher, clock Clock, idGen IDGenerator, layout domain.StreamLayout, historyMode domain.HistoryMode) *PatchService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	historyMode = domain.NormalizeHistoryMode(historyMode)
	return &PatchService{
		writeStore:    writeStore,
		readStore:     readStore,
		canonicalizer: canonicalizer,
		encoder:       encoder,
		decoder:       decoder,
		patcher:       patcher,
		hasher:        hasher,
		clock:         clock,
		idGen:         idGen,
		layout:        layout,
		historyMode:   historyMode,
	}
}

func (s *PatchService) Patch(ctx context.Context, repoPath, collection, docID string, patch []byte) (PutResult, error) {
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

	if len(patch) == 0 {
		return PutResult{}, ErrPayloadRequired
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

	currentDoc, err := s.loadCurrentDoc(ctx, absRepoPath, collection, docID, streamPath, headHash)
	if err != nil {
		return PutResult{}, err
	}

	canonicalPatch, err := s.canonicalizer.Canonicalize(ctx, patch)
	if err != nil {
		return PutResult{}, err
	}

	if s.patcher == nil {
		return PutResult{}, ErrPatchUnsupported
	}
	updatedDoc, err := s.patcher.Apply(ctx, currentDoc, canonicalPatch)
	if err != nil {
		return PutResult{}, err
	}

	txID, err := s.idGen.NewID()
	if err != nil {
		return PutResult{}, err
	}

	tx := domain.Transaction{
		TxID:       txID,
		Timestamp:  s.clock.Now().UnixNano(),
		Collection: collection,
		DocID:      docID,
	}
	if s.historyMode == domain.HistoryModeAmend {
		snapshot, err := s.canonicalizer.Canonicalize(ctx, updatedDoc)
		if err != nil {
			return PutResult{}, err
		}
		tx.Op = domain.TxOpMerge
		tx.Snapshot = snapshot
	} else {
		tx.Op = domain.TxOpPatch
		tx.Patch = canonicalPatch
		tx.ParentHash = headHash
	}

	encoded, err := s.encoder.Encode(tx)
	if err != nil {
		return PutResult{}, err
	}

	txHash := s.hasher.SumHex(encoded)
	stateTx, stateEncoded, stateHash, err := s.buildStateTx(ctx, tx, updatedDoc)
	if err != nil {
		return PutResult{}, err
	}
	result, err := s.writeStore.PutTx(ctx, TxWrite{
		RepoPath:     absRepoPath,
		StreamPath:   streamPath,
		TxBytes:      encoded,
		TxHash:       txHash,
		Tx:           tx,
		StatePath:    domain.StatePath(s.layout, collection, docID),
		StateTxBytes: stateEncoded,
		StateTxHash:  stateHash,
		StateTx:      stateTx,
	})
	if err != nil {
		return PutResult{}, err
	}

	if result.TxHash == "" {
		result.TxHash = txHash
	}
	if result.TxID == "" {
		result.TxID = txID
	}

	return result, nil
}

func (s *PatchService) loadCurrentDoc(ctx context.Context, repoPath, collection, docID, streamPath, headHash string) ([]byte, error) {
	statePath := domain.StatePath(s.layout, collection, docID)
	stateBlob, err := s.readStore.LoadHeadTx(ctx, repoPath, statePath)
	if err != nil && !errors.Is(err, ErrDocNotFound) {
		return nil, err
	}
	if err == nil && len(stateBlob.Bytes) > 0 {
		stateTx, err := s.decoder.Decode(stateBlob.Bytes)
		if err == nil {
			switch stateTx.Op {
			case domain.TxOpDelete:
				return nil, ErrDocDeleted
			case domain.TxOpPut, domain.TxOpMerge:
				if len(stateTx.Snapshot) > 0 {
					return stateTx.Snapshot, nil
				}
			}
		}
	}

	txBlobs, err := s.readStore.LoadStreamTxs(ctx, repoPath, streamPath)
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

	currentDoc, _, err := rehydrateChain(ctx, chain, s.patcher)
	if err != nil {
		return nil, err
	}
	return currentDoc, nil
}

func (s *PatchService) buildStateTx(ctx context.Context, historyTx domain.Transaction, updatedDoc []byte) (domain.Transaction, []byte, string, error) {
	stateTx := historyTx
	stateTx.ParentHash = ""
	if historyTx.Op == domain.TxOpPatch {
		snapshot, err := s.canonicalizer.Canonicalize(ctx, updatedDoc)
		if err != nil {
			return domain.Transaction{}, nil, "", err
		}
		stateTx.Op = domain.TxOpMerge
		stateTx.Patch = nil
		stateTx.Snapshot = snapshot
	}
	encoded, err := s.encoder.Encode(stateTx)
	if err != nil {
		return domain.Transaction{}, nil, "", err
	}
	txHash := s.hasher.SumHex(encoded)
	return stateTx, encoded, txHash, nil
}
