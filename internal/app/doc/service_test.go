package doc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeStore struct {
	parentHash string
	headErr    error
	putErr     error
	putResult  PutResult
	received   TxWrite
}

func (f *fakeStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.headErr != nil {
		return "", f.headErr
	}
	return f.parentHash, nil
}

func (f *fakeStore) PutTx(ctx context.Context, write TxWrite) (PutResult, error) {
	f.received = write
	if f.putErr != nil {
		return PutResult{}, f.putErr
	}
	return f.putResult, nil
}

type fakeCanonicalizer struct {
	out []byte
	err error
}

func (f fakeCanonicalizer) Canonicalize(ctx context.Context, input []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}

type fakeEncoder struct {
	out []byte
	err error
	tx  domain.Transaction
}

func (f *fakeEncoder) Encode(tx domain.Transaction) ([]byte, error) {
	f.tx = tx
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}

type fakeHasher struct {
	sum string
}

func (f fakeHasher) SumHex(data []byte) string {
	return f.sum
}

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time {
	return f.now
}

type fakeIDGen struct {
	id  string
	err error
}

func (f fakeIDGen) NewID() (string, error) {
	return f.id, f.err
}

func TestPutRequiresCollection(t *testing.T) {
	service := NewPutService(&fakeStore{}, fakeCanonicalizer{}, &fakeEncoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Put(context.Background(), "repo", " ", "doc", []byte(`{}`))
	if !errors.Is(err, ErrCollectionRequired) {
		t.Fatalf("expected ErrCollectionRequired, got %v", err)
	}
}

func TestPutRejectsInvalidCollection(t *testing.T) {
	service := NewPutService(&fakeStore{}, fakeCanonicalizer{}, &fakeEncoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Put(context.Background(), "repo", "users/..", "doc", []byte(`{}`))
	if !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("expected ErrInvalidCollection, got %v", err)
	}
}

func TestPutRequiresDocID(t *testing.T) {
	service := NewPutService(&fakeStore{}, fakeCanonicalizer{}, &fakeEncoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Put(context.Background(), "repo", "users", " ", []byte(`{}`))
	if !errors.Is(err, ErrDocIDRequired) {
		t.Fatalf("expected ErrDocIDRequired, got %v", err)
	}
}

func TestPutRequiresPayload(t *testing.T) {
	service := NewPutService(&fakeStore{}, fakeCanonicalizer{}, &fakeEncoder{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	_, err := service.Put(context.Background(), "repo", "users", "doc", nil)
	if !errors.Is(err, ErrPayloadRequired) {
		t.Fatalf("expected ErrPayloadRequired, got %v", err)
	}
}

func TestPutBuildsTxAndWrites(t *testing.T) {
	store := &fakeStore{parentHash: "parent"}
	canonical := []byte(`{"a":1}`)
	encoder := &fakeEncoder{out: []byte("encoded")}
	hasher := fakeHasher{sum: "hash"}
	clock := fakeClock{now: time.Unix(1, 2).UTC()}
	idGen := fakeIDGen{id: "01H123"}
	service := NewPutService(store, fakeCanonicalizer{out: canonical}, encoder, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	result, err := service.Put(context.Background(), "repo", "users", "doc1", []byte(`{"b":2}`))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if store.received.Tx.ParentHash != "parent" {
		t.Fatalf("expected parent hash %q, got %q", "parent", store.received.Tx.ParentHash)
	}
	if !bytes.Equal(store.received.Tx.Snapshot, canonical) {
		t.Fatalf("expected canonical payload")
	}
	if store.received.Tx.TxID != "01H123" {
		t.Fatalf("expected tx id %q, got %q", "01H123", store.received.Tx.TxID)
	}

	if store.received.TxHash != "hash" {
		t.Fatalf("expected tx hash %q, got %q", "hash", store.received.TxHash)
	}
	if !bytes.Equal(store.received.TxBytes, []byte("encoded")) {
		t.Fatalf("expected encoded tx bytes")
	}
	if result.TxHash != "hash" {
		t.Fatalf("expected result hash %q, got %q", "hash", result.TxHash)
	}
	if result.TxID != "01H123" {
		t.Fatalf("expected result tx id %q, got %q", "01H123", result.TxID)
	}
}

func TestPutAmendSkipsParentHash(t *testing.T) {
	store := &fakeStore{parentHash: "parent"}
	canonical := []byte(`{"a":1}`)
	encoder := &fakeEncoder{out: []byte("encoded")}
	hasher := fakeHasher{sum: "hash"}
	clock := fakeClock{now: time.Unix(1, 2).UTC()}
	idGen := fakeIDGen{id: "01H123"}
	service := NewPutService(store, fakeCanonicalizer{out: canonical}, encoder, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAmend)

	_, err := service.Put(context.Background(), "repo", "users", "doc1", []byte(`{"b":2}`))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if encoder.tx.ParentHash != "" {
		t.Fatalf("expected empty parent hash, got %q", encoder.tx.ParentHash)
	}
}
