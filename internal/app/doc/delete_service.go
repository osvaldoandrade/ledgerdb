package doc

import (
	"context"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type DeleteService struct {
	writeStore  WriteStore
	readStore   ReadStore
	encoder     Encoder
	decoder     Decoder
	hasher      Hasher
	clock       Clock
	idGen       IDGenerator
	layout      domain.StreamLayout
	historyMode domain.HistoryMode
}

func NewDeleteService(writeStore WriteStore, readStore ReadStore, encoder Encoder, decoder Decoder, hasher Hasher, clock Clock, idGen IDGenerator, layout domain.StreamLayout, historyMode domain.HistoryMode) *DeleteService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	historyMode = domain.NormalizeHistoryMode(historyMode)
	return &DeleteService{
		writeStore:  writeStore,
		readStore:   readStore,
		encoder:     encoder,
		decoder:     decoder,
		hasher:      hasher,
		clock:       clock,
		idGen:       idGen,
		layout:      layout,
		historyMode: historyMode,
	}
}

func (s *DeleteService) Delete(ctx context.Context, repoPath, collection, docID string) (PutResult, error) {
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

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return PutResult{}, err
	}

	streamPath := domain.StreamPath(s.layout, collection, docID)
	headBlob, err := s.readStore.LoadHeadTx(ctx, absRepoPath, streamPath)
	if err != nil {
		return PutResult{}, err
	}
	if len(headBlob.Bytes) == 0 {
		return PutResult{}, ErrDocNotFound
	}

	headTx, err := s.decoder.Decode(headBlob.Bytes)
	if err != nil {
		return PutResult{}, err
	}
	if headTx.Op == domain.TxOpDelete {
		return PutResult{}, ErrDocDeleted
	}

	parentHash := ""
	if s.historyMode != domain.HistoryModeAmend {
		parentHash = s.hasher.SumHex(headBlob.Bytes)
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
		Op:         domain.TxOpDelete,
		ParentHash: parentHash,
	}

	encoded, err := s.encoder.Encode(tx)
	if err != nil {
		return PutResult{}, err
	}

	txHash := s.hasher.SumHex(encoded)
	stateTx := tx
	stateTx.ParentHash = ""
	stateEncoded := encoded
	stateHash := txHash
	if tx.ParentHash != "" {
		stateEncoded, err = s.encoder.Encode(stateTx)
		if err != nil {
			return PutResult{}, err
		}
		stateHash = s.hasher.SumHex(stateEncoded)
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
