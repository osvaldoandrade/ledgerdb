package doc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeDeleteStore struct {
	head      TxBlob
	headErr   error
	putErr    error
	putResult PutResult
	received  TxWrite
}

func (f *fakeDeleteStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	return "", nil
}

func (f *fakeDeleteStore) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error) {
	if f.headErr != nil {
		return TxBlob{}, f.headErr
	}
	return f.head, nil
}

func (f *fakeDeleteStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error) {
	return nil, nil
}

func (f *fakeDeleteStore) PutTx(ctx context.Context, write TxWrite) (PutResult, error) {
	f.received = write
	if f.putErr != nil {
		return PutResult{}, f.putErr
	}
	return f.putResult, nil
}

type stubDecoder struct {
	tx  domain.Transaction
	err error
}

func (s stubDecoder) Decode(data []byte) (domain.Transaction, error) {
	if s.err != nil {
		return domain.Transaction{}, s.err
	}
	return s.tx, nil
}

func TestDeleteRequiresCollection(t *testing.T) {
	service := NewDeleteService(&fakeDeleteStore{}, &fakeDeleteStore{}, &fakeEncoder{}, stubDecoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Delete(context.Background(), "repo", " ", "doc")
	if !errors.Is(err, ErrCollectionRequired) {
		t.Fatalf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestDeleteRejectsInvalidCollection(t *testing.T) {
	service := NewDeleteService(&fakeDeleteStore{}, &fakeDeleteStore{}, &fakeEncoder{}, stubDecoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Delete(context.Background(), "repo", "users/..", "doc")
	if !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("expected ErrInvalidCollection, got %v", err)
	}
}

func TestDeleteRequiresDocID(t *testing.T) {
	service := NewDeleteService(&fakeDeleteStore{}, &fakeDeleteStore{}, &fakeEncoder{}, stubDecoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Delete(context.Background(), "repo", "users", " ")
	if !errors.Is(err, ErrDocIDRequired) {
		t.Fatalf("expected ErrDocIDRequired, got %v", err)
	}
}

func TestDeleteReturnsErrDocDeleted(t *testing.T) {
	store := &fakeDeleteStore{head: TxBlob{Bytes: []byte("head")}}
	decoder := stubDecoder{tx: domain.Transaction{Op: domain.TxOpDelete}}
	service := NewDeleteService(store, store, &fakeEncoder{}, decoder, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	_, err := service.Delete(context.Background(), "repo", "users", "doc")
	if !errors.Is(err, ErrDocDeleted) {
		t.Fatalf("expected ErrDocDeleted, got %v", err)
	}
}

func TestDeleteWritesTx(t *testing.T) {
	store := &fakeDeleteStore{head: TxBlob{Bytes: []byte("head")}}
	decoder := stubDecoder{tx: domain.Transaction{Op: domain.TxOpPut}}
	encoder := &fakeEncoder{out: []byte("encoded")}
	hasher := fakeHasher{sum: "hash"}
	clock := fakeClock{now: time.Unix(1, 0).UTC()}
	idGen := fakeIDGen{id: "01H123"}
	service := NewDeleteService(store, store, encoder, decoder, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	result, err := service.Delete(context.Background(), "repo", "users", "doc")
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	if store.received.Tx.Op != domain.TxOpDelete {
		t.Fatalf("expected delete op, got %v", store.received.Tx.Op)
	}
	if result.TxHash != "hash" || result.TxID != "01H123" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestDeleteAmendSkipsParentHash(t *testing.T) {
	store := &fakeDeleteStore{head: TxBlob{Bytes: []byte("head")}}
	decoder := stubDecoder{tx: domain.Transaction{Op: domain.TxOpPut}}
	encoder := &fakeEncoder{out: []byte("encoded")}
	hasher := fakeHasher{sum: "hash"}
	clock := fakeClock{now: time.Unix(1, 0).UTC()}
	idGen := fakeIDGen{id: "01H123"}
	service := NewDeleteService(store, store, encoder, decoder, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAmend)

	_, err := service.Delete(context.Background(), "repo", "users", "doc")
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	if store.received.Tx.ParentHash != "" {
		t.Fatalf("expected empty parent hash, got %q", store.received.Tx.ParentHash)
	}
}
