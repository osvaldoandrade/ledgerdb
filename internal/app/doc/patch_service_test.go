package doc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakePatchStore struct {
	headHash  string
	tx        []TxBlob
	headErr   error
	putErr    error
	putResult PutResult
	received  TxWrite
}

func (f *fakePatchStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.headErr != nil {
		return "", f.headErr
	}
	return f.headHash, nil
}

func (f *fakePatchStore) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error) {
	return TxBlob{}, nil
}

func (f *fakePatchStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	return f.tx, nil
}

func (f *fakePatchStore) PutTx(ctx context.Context, write TxWrite) (PutResult, error) {
	f.received = write
	if f.putErr != nil {
		return PutResult{}, f.putErr
	}
	return f.putResult, nil
}

type recordingPatcher struct {
	lastDoc   []byte
	lastPatch []byte
	out       []byte
	err       error
}

func (r *recordingPatcher) Apply(ctx context.Context, doc, patch []byte) ([]byte, error) {
	r.lastDoc = doc
	r.lastPatch = patch
	if r.err != nil {
		return nil, r.err
	}
	return r.out, nil
}

type patchDecoder struct {
	values map[string]domain.Transaction
}

func (m patchDecoder) Decode(data []byte) (domain.Transaction, error) {
	tx, ok := m.values[string(data)]
	if !ok {
		return domain.Transaction{}, errors.New("missing tx")
	}
	return tx, nil
}

type patchHasher struct {
	values map[string]string
}

func (m patchHasher) SumHex(data []byte) string {
	return m.values[string(data)]
}

func TestPatchRequiresPayload(t *testing.T) {
	store := &fakePatchStore{}
	service := NewPatchService(store, store, fakeCanonicalizer{}, &fakeEncoder{}, patchDecoder{}, &recordingPatcher{}, fakeHasher{}, fakeClock{}, fakeIDGen{}, domain.StreamLayoutFlat, domain.HistoryModeAppend)

	_, err := service.Patch(context.Background(), "repo", "users", "doc", nil)
	if !errors.Is(err, ErrPayloadRequired) {
		t.Fatalf("expected ErrPayloadRequired, got %v", err)
	}
}

func TestPatchWritesTx(t *testing.T) {
	store := &fakePatchStore{
		headHash: "hash1",
		tx:       []TxBlob{{Bytes: []byte("tx1")}},
	}
	decoder := patchDecoder{
		values: map[string]domain.Transaction{
			"tx1": {
				TxID:       "01HBASE",
				Timestamp:  1,
				Collection: "users",
				DocID:      "doc",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
		},
	}
	hasher := patchHasher{values: map[string]string{
		"tx1":     "hash1",
		"encoded": "hash2",
	}}
	encoder := &fakeEncoder{out: []byte("encoded")}
	patcher := &recordingPatcher{out: []byte(`{"a":2}`)}
	clock := fakeClock{now: time.Unix(2, 0).UTC()}
	idGen := fakeIDGen{id: "01HPATCH"}
	canonicalizer := fakeCanonicalizer{out: []byte(`[{"op":"replace","path":"/a","value":2}]`)}

	service := NewPatchService(store, store, canonicalizer, encoder, decoder, patcher, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAppend)
	result, err := service.Patch(context.Background(), "repo", "users", "doc", []byte(`[]`))
	if err != nil {
		t.Fatalf("Patch returned error: %v", err)
	}

	if store.received.Tx.Op != domain.TxOpPatch {
		t.Fatalf("expected patch op, got %v", store.received.Tx.Op)
	}
	if string(store.received.Tx.Patch) == "" {
		t.Fatalf("expected patch payload")
	}
	if result.TxHash != "hash2" || result.TxID != "01HPATCH" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(patcher.lastDoc) == 0 {
		t.Fatalf("expected patcher to be invoked")
	}
}

func TestPatchAmendWritesSnapshot(t *testing.T) {
	store := &fakePatchStore{
		headHash: "hash1",
		tx:       []TxBlob{{Bytes: []byte("tx1")}},
	}
	decoder := patchDecoder{
		values: map[string]domain.Transaction{
			"tx1": {
				TxID:       "01HBASE",
				Timestamp:  1,
				Collection: "users",
				DocID:      "doc",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
		},
	}
	hasher := patchHasher{values: map[string]string{
		"tx1":     "hash1",
		"encoded": "hash2",
	}}
	encoder := &fakeEncoder{out: []byte("encoded")}
	patcher := &recordingPatcher{out: []byte(`{"a":2}`)}
	clock := fakeClock{now: time.Unix(2, 0).UTC()}
	idGen := fakeIDGen{id: "01HPATCH"}
	canonicalizer := fakeCanonicalizer{out: []byte(`{"a":2}`)}

	service := NewPatchService(store, store, canonicalizer, encoder, decoder, patcher, hasher, clock, idGen, domain.StreamLayoutFlat, domain.HistoryModeAmend)
	result, err := service.Patch(context.Background(), "repo", "users", "doc", []byte(`[]`))
	if err != nil {
		t.Fatalf("Patch returned error: %v", err)
	}

	if store.received.Tx.Op != domain.TxOpMerge {
		t.Fatalf("expected merge op, got %v", store.received.Tx.Op)
	}
	if store.received.Tx.ParentHash != "" {
		t.Fatalf("expected empty parent hash, got %q", store.received.Tx.ParentHash)
	}
	if len(store.received.Tx.Snapshot) == 0 {
		t.Fatalf("expected snapshot payload")
	}
	if result.TxHash != "hash2" || result.TxID != "01HPATCH" {
		t.Fatalf("unexpected result: %+v", result)
	}
}
