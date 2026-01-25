package doc

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type revertStore struct {
	headHash  string
	headBlob  TxBlob
	txBlobs   []TxBlob
	putResult PutResult
	received  TxWrite
}

func (r *revertStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	return r.headHash, nil
}

func (r *revertStore) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error) {
	return r.headBlob, nil
}

func (r *revertStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error) {
	return r.txBlobs, nil
}

func (r *revertStore) PutTx(ctx context.Context, write TxWrite) (PutResult, error) {
	r.received = write
	return r.putResult, nil
}

type passCanonicalizer struct{}

func (passCanonicalizer) Canonicalize(ctx context.Context, input []byte) ([]byte, error) {
	return input, nil
}

type dataHasher struct{}

func (dataHasher) SumHex(data []byte) string {
	return "hash:" + string(data)
}

func TestRevertRequiresReference(t *testing.T) {
	store := &revertStore{}
	service := NewRevertService(store, store, passCanonicalizer{}, &fakeEncoder{}, mapDecoder{}, nil, dataHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	_, err := service.Revert(context.Background(), "repo", "users", "u1", RevertOptions{})
	if !errors.Is(err, ErrTxReferenceRequired) {
		t.Fatalf("expected ErrTxReferenceRequired, got %v", err)
	}
}

func TestRevertUsesTargetSnapshot(t *testing.T) {
	store := &revertStore{
		headHash: "hash:tx2",
		txBlobs: []TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
		},
	}
	decoder := mapDecoder{
		values: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
			"tx2": {
				TxID:       "tx2",
				Timestamp:  2,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPatch,
				Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
				ParentHash: "hash:tx1",
			},
		},
	}
	encoder := &fakeEncoder{out: []byte("encoded")}
	clock := fakeClock{now: time.Unix(1, 0).UTC()}
	idGen := fakeIDGen{id: "01HREV"}

	service := NewRevertService(store, store, passCanonicalizer{}, encoder, decoder, nil, dataHasher{}, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	_, err := service.Revert(context.Background(), "repo", "users", "u1", RevertOptions{TxID: "tx1"})
	if err != nil {
		t.Fatalf("expected revert to succeed: %v", err)
	}
	if !bytes.Equal(store.received.Tx.Snapshot, []byte(`{"a":1}`)) {
		t.Fatalf("expected snapshot to match tx1")
	}
}
