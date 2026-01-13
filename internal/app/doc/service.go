package doc

import (
	"context"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type PutService struct {
	store         WriteStore
	canonicalizer Canonicalizer
	encoder       Encoder
	hasher        Hasher
	clock         Clock
	idGen         IDGenerator
	layout        domain.StreamLayout
	historyMode   domain.HistoryMode
}

func NewPutService(store WriteStore, canonicalizer Canonicalizer, encoder Encoder, hasher Hasher, clock Clock, idGen IDGenerator, layout domain.StreamLayout, historyMode domain.HistoryMode) *PutService {
	if layout == "" {
		layout = domain.StreamLayoutFlat
	}
	layout = domain.NormalizeStreamLayout(layout)
	historyMode = domain.NormalizeHistoryMode(historyMode)
	return &PutService{
		store:         store,
		canonicalizer: canonicalizer,
		encoder:       encoder,
		hasher:        hasher,
		clock:         clock,
		idGen:         idGen,
		layout:        layout,
		historyMode:   historyMode,
	}
}

func (s *PutService) Put(ctx context.Context, repoPath, collection, docID string, payload []byte) (PutResult, error) {
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

	if len(payload) == 0 {
		return PutResult{}, ErrPayloadRequired
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return PutResult{}, err
	}

	streamPath := domain.StreamPath(s.layout, collection, docID)
	parentHash := ""
	if s.historyMode != domain.HistoryModeAmend {
		var err error
		parentHash, err = s.store.LoadStreamHead(ctx, absRepoPath, streamPath)
		if err != nil {
			return PutResult{}, err
		}
	}

	canonical, err := s.canonicalizer.Canonicalize(ctx, payload)
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
		Op:         domain.TxOpPut,
		Snapshot:   canonical,
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
	stateTxHash := txHash
	if tx.ParentHash != "" {
		stateEncoded, err = s.encoder.Encode(stateTx)
		if err != nil {
			return PutResult{}, err
		}
		stateTxHash = s.hasher.SumHex(stateEncoded)
	}

	result, err := s.store.PutTx(ctx, TxWrite{
		RepoPath:     absRepoPath,
		StreamPath:   streamPath,
		TxBytes:      encoded,
		TxHash:       txHash,
		Tx:           tx,
		StatePath:    domain.StatePath(s.layout, collection, docID),
		StateTxBytes: stateEncoded,
		StateTxHash:  stateTxHash,
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
