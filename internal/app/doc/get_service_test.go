package doc

import (
	"context"
	"errors"
	"testing"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type fakeReadStore struct {
	headHash string
	tx       []TxBlob
	err      error
}

func (f fakeReadStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.headHash, nil
}

func (f fakeReadStore) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error) {
	return TxBlob{}, nil
}

func (f fakeReadStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tx, nil
}

type fakeDecoder struct {
	tx  domain.Transaction
	err error
}

func (f fakeDecoder) Decode(data []byte) (domain.Transaction, error) {
	if f.err != nil {
		return domain.Transaction{}, f.err
	}
	return f.tx, nil
}

type fakePatcher struct {
	out []byte
	err error
}

func (f fakePatcher) Apply(ctx context.Context, doc, patch []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}

func TestGetRequiresCollection(t *testing.T) {
	service := NewGetService(fakeReadStore{}, fakeDecoder{}, fakeHasher{}, fakePatcher{}, domain.StreamLayoutFlat)
	_, err := service.Get(context.Background(), "repo", " ", "doc")
	if !errors.Is(err, ErrCollectionRequired) {
		t.Fatalf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestGetRejectsInvalidCollection(t *testing.T) {
	service := NewGetService(fakeReadStore{}, fakeDecoder{}, fakeHasher{}, fakePatcher{}, domain.StreamLayoutFlat)
	_, err := service.Get(context.Background(), "repo", "users/..", "doc")
	if !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("expected ErrInvalidCollection, got %v", err)
	}
}

func TestGetRequiresDocID(t *testing.T) {
	service := NewGetService(fakeReadStore{}, fakeDecoder{}, fakeHasher{}, fakePatcher{}, domain.StreamLayoutFlat)
	_, err := service.Get(context.Background(), "repo", "users", " ")
	if !errors.Is(err, ErrDocIDRequired) {
		t.Fatalf("expected ErrDocIDRequired, got %v", err)
	}
}

func TestGetReturnsPayload(t *testing.T) {
	store := fakeReadStore{
		headHash: "hash1",
		tx:       []TxBlob{{Bytes: []byte("tx")}},
	}
	decoder := fakeDecoder{
		tx: domain.Transaction{
			TxID:     "01H123",
			Op:       domain.TxOpPut,
			Snapshot: []byte(`{"a":1}`),
		},
	}
	service := NewGetService(store, decoder, fakeHasher{sum: "hash1"}, fakePatcher{}, domain.StreamLayoutFlat)

	result, err := service.Get(context.Background(), "repo", "users", "doc")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(result.Payload) != `{"a":1}` {
		t.Fatalf("unexpected payload: %s", string(result.Payload))
	}
	if result.TxHash != "hash1" || result.TxID != "01H123" || result.Op != domain.TxOpPut {
		t.Fatalf("unexpected metadata: %+v", result)
	}
}

func TestGetDeleteReturnsError(t *testing.T) {
	store := fakeReadStore{
		headHash: "hash1",
		tx:       []TxBlob{{Bytes: []byte("tx")}},
	}
	decoder := fakeDecoder{tx: domain.Transaction{Op: domain.TxOpDelete}}
	service := NewGetService(store, decoder, fakeHasher{sum: "hash1"}, fakePatcher{}, domain.StreamLayoutFlat)

	_, err := service.Get(context.Background(), "repo", "users", "doc")
	if !errors.Is(err, ErrDocDeleted) {
		t.Fatalf("expected ErrDocDeleted, got %v", err)
	}
}
