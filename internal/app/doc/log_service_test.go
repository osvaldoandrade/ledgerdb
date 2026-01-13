package doc

import (
	"context"
	"errors"
	"testing"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type fakeLogStore struct {
	head string
	tx   []TxBlob
	err  error
}

func (f fakeLogStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.head, nil
}

func (f fakeLogStore) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error) {
	return TxBlob{}, nil
}

func (f fakeLogStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tx, nil
}

type mapHasher struct {
	values map[string]string
}

func (m mapHasher) SumHex(data []byte) string {
	return m.values[string(data)]
}

type mapDecoder struct {
	values map[string]domain.Transaction
}

func (m mapDecoder) Decode(data []byte) (domain.Transaction, error) {
	tx, ok := m.values[string(data)]
	if !ok {
		return domain.Transaction{}, errors.New("missing tx")
	}
	return tx, nil
}

func TestLogRequiresCollection(t *testing.T) {
	service := NewLogService(fakeLogStore{}, mapDecoder{}, mapHasher{}, domain.StreamLayoutFlat)
	_, err := service.Log(context.Background(), "repo", " ", "doc")
	if !errors.Is(err, ErrCollectionRequired) {
		t.Fatalf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestLogNotFound(t *testing.T) {
	service := NewLogService(fakeLogStore{head: ""}, mapDecoder{}, mapHasher{}, domain.StreamLayoutFlat)
	_, err := service.Log(context.Background(), "repo", "users", "doc")
	if !errors.Is(err, ErrDocNotFound) {
		t.Fatalf("expected ErrDocNotFound, got %v", err)
	}
}

func TestLogOrdersByParentChain(t *testing.T) {
	tx1 := domain.Transaction{TxID: "a", ParentHash: "", Timestamp: 1, Op: domain.TxOpPut}
	tx2 := domain.Transaction{TxID: "b", ParentHash: "hash1", Timestamp: 2, Op: domain.TxOpPatch}
	tx3 := domain.Transaction{TxID: "c", ParentHash: "hash2", Timestamp: 3, Op: domain.TxOpPut}

	decoder := mapDecoder{
		values: map[string]domain.Transaction{
			"tx1": tx1,
			"tx2": tx2,
			"tx3": tx3,
		},
	}
	hasher := mapHasher{
		values: map[string]string{
			"tx1": "hash1",
			"tx2": "hash2",
			"tx3": "hash3",
		},
	}
	store := fakeLogStore{
		head: "hash3",
		tx: []TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
			{Bytes: []byte("tx3")},
		},
	}
	service := NewLogService(store, decoder, hasher, domain.StreamLayoutFlat)

	entries, err := service.Log(context.Background(), "repo", "users", "doc")
	if err != nil {
		t.Fatalf("Log returned error: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].TxHash != "hash3" || entries[1].TxHash != "hash2" || entries[2].TxHash != "hash1" {
		t.Fatalf("unexpected order: %+v", entries)
	}
}
