package doc

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
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

func TestLogPaginatedRoundTrip(t *testing.T) {
	// Build a chain of 5 txs: hash1 -> hash2 -> hash3 -> hash4 -> hash5 (head).
	txs := []domain.Transaction{
		{TxID: "a", ParentHash: "", Timestamp: 1, Op: domain.TxOpPut},
		{TxID: "b", ParentHash: "hash1", Timestamp: 2, Op: domain.TxOpPatch},
		{TxID: "c", ParentHash: "hash2", Timestamp: 3, Op: domain.TxOpPatch},
		{TxID: "d", ParentHash: "hash3", Timestamp: 4, Op: domain.TxOpPatch},
		{TxID: "e", ParentHash: "hash4", Timestamp: 5, Op: domain.TxOpPatch},
	}
	decoderValues := map[string]domain.Transaction{}
	hasherValues := map[string]string{}
	blobs := make([]TxBlob, len(txs))
	for i, tx := range txs {
		key := "tx" + string(rune('1'+i))
		decoderValues[key] = tx
		hasherValues[key] = "hash" + string(rune('1'+i))
		blobs[i] = TxBlob{Bytes: []byte(key)}
	}
	store := fakeLogStore{head: "hash5", tx: blobs}
	service := NewLogService(store, mapDecoder{values: decoderValues}, mapHasher{values: hasherValues}, domain.StreamLayoutFlat)

	// Page 1 of 2 (limit 2, expect 2 entries + next cursor).
	page, err := service.LogPaginated(context.Background(), "repo", "users", "doc", LogOptions{Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("page1: expected 2 entries, got %d", len(page.Entries))
	}
	if page.Entries[0].TxHash != "hash5" || page.Entries[1].TxHash != "hash4" {
		t.Fatalf("page1: unexpected order: %+v", page.Entries)
	}
	if page.NextCursor == "" {
		t.Fatal("page1: expected next cursor")
	}

	// Page 2.
	page2, err := service.LogPaginated(context.Background(), "repo", "users", "doc", LogOptions{Limit: 2, Cursor: page.NextCursor})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.Entries) != 2 {
		t.Fatalf("page2: expected 2 entries, got %d", len(page2.Entries))
	}
	if page2.Entries[0].TxHash != "hash3" || page2.Entries[1].TxHash != "hash2" {
		t.Fatalf("page2: unexpected order: %+v", page2.Entries)
	}
	if page2.NextCursor == "" {
		t.Fatal("page2: expected next cursor")
	}

	// Page 3 (last).
	page3, err := service.LogPaginated(context.Background(), "repo", "users", "doc", LogOptions{Limit: 2, Cursor: page2.NextCursor})
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3.Entries) != 1 {
		t.Fatalf("page3: expected 1 entry, got %d", len(page3.Entries))
	}
	if page3.Entries[0].TxHash != "hash1" {
		t.Fatalf("page3: unexpected entry: %+v", page3.Entries)
	}
	if page3.NextCursor != "" {
		t.Fatalf("page3: expected empty next cursor, got %q", page3.NextCursor)
	}
}

func TestLogPaginatedRejectsInvalidCursor(t *testing.T) {
	tx1 := domain.Transaction{TxID: "a", ParentHash: "", Timestamp: 1, Op: domain.TxOpPut}
	store := fakeLogStore{
		head: "hash1",
		tx:   []TxBlob{{Bytes: []byte("tx1")}},
	}
	service := NewLogService(store,
		mapDecoder{values: map[string]domain.Transaction{"tx1": tx1}},
		mapHasher{values: map[string]string{"tx1": "hash1"}},
		domain.StreamLayoutFlat,
	)

	_, err := service.LogPaginated(context.Background(), "repo", "users", "doc", LogOptions{Cursor: "not-a-cursor!"})
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
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
