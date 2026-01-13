package inspect

import (
	"context"
	"errors"
	"testing"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type fakeReader struct {
	data []byte
	err  error
}

func (f fakeReader) ReadBlob(ctx context.Context, repoPath, objectHash string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.data, nil
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

type fakeHasher struct {
	sum string
}

func (f fakeHasher) SumHex(data []byte) string {
	return f.sum
}

func TestInspectRequiresHash(t *testing.T) {
	service := NewService(fakeReader{}, fakeDecoder{}, fakeHasher{})
	_, err := service.InspectBlob(context.Background(), "repo", " ")
	if !errors.Is(err, ErrHashRequired) {
		t.Fatalf("expected ErrHashRequired, got %v", err)
	}
}

func TestInspectRejectsInvalidHash(t *testing.T) {
	service := NewService(fakeReader{}, fakeDecoder{}, fakeHasher{})
	_, err := service.InspectBlob(context.Background(), "repo", "nope")
	if !errors.Is(err, ErrInvalidHash) {
		t.Fatalf("expected ErrInvalidHash, got %v", err)
	}
}

func TestInspectReadsAndDecodes(t *testing.T) {
	tx := domain.Transaction{TxID: "01HINSPECT", Op: domain.TxOpPut}
	service := NewService(fakeReader{data: []byte("tx")}, fakeDecoder{tx: tx}, fakeHasher{sum: "hash"})

	result, err := service.InspectBlob(context.Background(), "repo", "8ab686eafeb1f44702738c8b0f24f2567c36da6d")
	if err != nil {
		t.Fatalf("InspectBlob returned error: %v", err)
	}
	if result.ObjectHash != "8ab686eafeb1f44702738c8b0f24f2567c36da6d" {
		t.Fatalf("unexpected object hash: %s", result.ObjectHash)
	}
	if result.TxHash != "hash" {
		t.Fatalf("unexpected tx hash: %s", result.TxHash)
	}
	if result.Tx.TxID != "01HINSPECT" {
		t.Fatalf("unexpected tx id: %s", result.Tx.TxID)
	}
}
