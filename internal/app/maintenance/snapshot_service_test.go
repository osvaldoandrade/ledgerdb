package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codecompany/ledgerdb/internal/app/doc"
	"github.com/codecompany/ledgerdb/internal/domain"
)

const testStream = "documents/users/DOC_deadbeef"

type fakeStreamLister struct {
	streams []string
	err     error
}

func (f fakeStreamLister) ListDocStreams(ctx context.Context, repoPath string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.streams, nil
}

type fakeSnapshotStore struct {
	head     string
	blobs    []doc.TxBlob
	headErr  error
	blobErr  error
	write    doc.TxWrite
	writeErr error
}

func (f *fakeSnapshotStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.headErr != nil {
		return "", f.headErr
	}
	return f.head, nil
}

func (f *fakeSnapshotStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error) {
	if f.blobErr != nil {
		return nil, f.blobErr
	}
	return f.blobs, nil
}

func (f *fakeSnapshotStore) PutTx(ctx context.Context, write doc.TxWrite) (doc.PutResult, error) {
	f.write = write
	if f.writeErr != nil {
		return doc.PutResult{}, f.writeErr
	}
	return doc.PutResult{}, nil
}

type mapDecoder struct {
	txs map[string]domain.Transaction
	err error
}

func (d mapDecoder) Decode(data []byte) (domain.Transaction, error) {
	if d.err != nil {
		return domain.Transaction{}, d.err
	}
	tx, ok := d.txs[string(data)]
	if !ok {
		return domain.Transaction{}, errors.New("missing tx")
	}
	return tx, nil
}

type mapHasher struct {
	values map[string]string
}

func (h mapHasher) SumHex(data []byte) string {
	return h.values[string(data)]
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

type fakePatcher struct {
	out []byte
	err error
}

func (f fakePatcher) Apply(ctx context.Context, docBytes, patch []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
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

func TestSnapshotInvalidThreshold(t *testing.T) {
	service := NewSnapshotService(fakeStreamLister{}, &fakeSnapshotStore{}, &fakeSnapshotStore{}, fakeCanonicalizer{}, &fakeEncoder{}, mapDecoder{}, fakePatcher{}, mapHasher{}, fakeClock{}, fakeIDGen{}, domain.HistoryModeAppend)
	_, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 0})
	if !errors.Is(err, ErrInvalidThreshold) {
		t.Fatalf("expected ErrInvalidThreshold, got %v", err)
	}
}

func TestSnapshotInvalidMax(t *testing.T) {
	service := NewSnapshotService(fakeStreamLister{}, &fakeSnapshotStore{}, &fakeSnapshotStore{}, fakeCanonicalizer{}, &fakeEncoder{}, mapDecoder{}, fakePatcher{}, mapHasher{}, fakeClock{}, fakeIDGen{}, domain.HistoryModeAppend)
	_, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 1, Max: -1})
	if !errors.Is(err, ErrInvalidMax) {
		t.Fatalf("expected ErrInvalidMax, got %v", err)
	}
}

func TestSnapshotCreatesMergeTx(t *testing.T) {
	store := &fakeSnapshotStore{
		head: "h2",
		blobs: []doc.TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
		},
	}
	decoder := mapDecoder{txs: map[string]domain.Transaction{
		"tx1": {
			TxID:       "t1",
			Timestamp:  1,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPut,
			Snapshot:   []byte(`{"a":1}`),
		},
		"tx2": {
			TxID:          "t2",
			Timestamp:     2,
			Collection:    "users",
			DocID:         "doc1",
			Op:            domain.TxOpPatch,
			Patch:         []byte(`[{"op":"replace","path":"/a","value":2}]`),
			ParentHash:    "h1",
			SchemaVersion: "v1",
		},
	}}
	hasher := mapHasher{values: map[string]string{
		"tx1":     "h1",
		"tx2":     "h2",
		"encoded": "h3",
	}}
	encoder := &fakeEncoder{out: []byte("encoded")}

	service := NewSnapshotService(
		fakeStreamLister{streams: []string{testStream}},
		store,
		store,
		fakeCanonicalizer{out: []byte(`{"a":2}`)},
		encoder,
		decoder,
		fakePatcher{out: []byte(`{"a":2}`)},
		hasher,
		fakeClock{now: time.Unix(0, 123)},
		fakeIDGen{id: "01HSNAP"},
		domain.HistoryModeAppend,
	)

	result, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 1})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if result.Processed != 1 || result.Snapshotted != 1 || result.Planned != 0 || result.Skipped != 0 || result.Truncated || result.DryRun || len(result.Issues) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if store.write.StreamPath != testStream {
		t.Fatalf("unexpected stream path: %s", store.write.StreamPath)
	}
	if store.write.Tx.Op != domain.TxOpMerge {
		t.Fatalf("expected merge op, got %v", store.write.Tx.Op)
	}
	if store.write.Tx.ParentHash != "h2" {
		t.Fatalf("expected parent hash h2, got %s", store.write.Tx.ParentHash)
	}
	if store.write.Tx.SchemaVersion != "v1" {
		t.Fatalf("expected schema version v1, got %s", store.write.Tx.SchemaVersion)
	}
	if store.write.Tx.TxID != "01HSNAP" {
		t.Fatalf("expected tx id 01HSNAP, got %s", store.write.Tx.TxID)
	}
}

func TestSnapshotSkipsShortChain(t *testing.T) {
	store := &fakeSnapshotStore{
		head: "h1",
		blobs: []doc.TxBlob{
			{Bytes: []byte("tx1")},
		},
	}
	decoder := mapDecoder{txs: map[string]domain.Transaction{
		"tx1": {
			TxID:       "t1",
			Timestamp:  1,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPut,
			Snapshot:   []byte(`{"a":1}`),
		},
	}}
	hasher := mapHasher{values: map[string]string{
		"tx1": "h1",
	}}

	service := NewSnapshotService(
		fakeStreamLister{streams: []string{testStream}},
		store,
		store,
		fakeCanonicalizer{out: []byte(`{"a":1}`)},
		&fakeEncoder{out: []byte("encoded")},
		decoder,
		fakePatcher{},
		hasher,
		fakeClock{now: time.Unix(0, 123)},
		fakeIDGen{id: "01HSNAP"},
		domain.HistoryModeAppend,
	)

	result, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 2})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if result.Processed != 1 || result.Snapshotted != 0 || result.Planned != 0 || result.Skipped != 1 || result.Truncated || result.DryRun || len(result.Issues) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestSnapshotSkipsDeletedHead(t *testing.T) {
	store := &fakeSnapshotStore{
		head: "h2",
		blobs: []doc.TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
		},
	}
	decoder := mapDecoder{txs: map[string]domain.Transaction{
		"tx1": {
			TxID:       "t1",
			Timestamp:  1,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPut,
			Snapshot:   []byte(`{"a":1}`),
		},
		"tx2": {
			TxID:       "t2",
			Timestamp:  2,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpDelete,
			ParentHash: "h1",
		},
	}}
	hasher := mapHasher{values: map[string]string{
		"tx1": "h1",
		"tx2": "h2",
	}}

	service := NewSnapshotService(
		fakeStreamLister{streams: []string{testStream}},
		store,
		store,
		fakeCanonicalizer{},
		&fakeEncoder{out: []byte("encoded")},
		decoder,
		fakePatcher{},
		hasher,
		fakeClock{now: time.Unix(0, 123)},
		fakeIDGen{id: "01HSNAP"},
		domain.HistoryModeAppend,
	)

	result, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 1})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if result.Processed != 1 || result.Snapshotted != 0 || result.Planned != 0 || result.Skipped != 1 || result.Truncated || result.DryRun || len(result.Issues) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestSnapshotDryRunPlansOnly(t *testing.T) {
	store := &fakeSnapshotStore{
		head: "h2",
		blobs: []doc.TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
		},
	}
	decoder := mapDecoder{txs: map[string]domain.Transaction{
		"tx1": {
			TxID:       "t1",
			Timestamp:  1,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPut,
			Snapshot:   []byte(`{"a":1}`),
		},
		"tx2": {
			TxID:       "t2",
			Timestamp:  2,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPatch,
			Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
			ParentHash: "h1",
		},
	}}
	hasher := mapHasher{values: map[string]string{
		"tx1":     "h1",
		"tx2":     "h2",
		"encoded": "h3",
	}}

	service := NewSnapshotService(
		fakeStreamLister{streams: []string{testStream}},
		store,
		store,
		fakeCanonicalizer{out: []byte(`{"a":2}`)},
		&fakeEncoder{out: []byte("encoded")},
		decoder,
		fakePatcher{out: []byte(`{"a":2}`)},
		hasher,
		fakeClock{now: time.Unix(0, 123)},
		fakeIDGen{id: "01HSNAP"},
		domain.HistoryModeAppend,
	)

	result, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 1, DryRun: true})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if result.Processed != 1 || result.Snapshotted != 0 || result.Planned != 1 || result.Skipped != 0 || !result.DryRun || result.Truncated || len(result.Issues) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if store.write.Tx.TxID != "" {
		t.Fatalf("expected no write in dry-run")
	}
}

func TestSnapshotMaxLimitsProcessing(t *testing.T) {
	store := &fakeSnapshotStore{
		head: "h2",
		blobs: []doc.TxBlob{
			{Bytes: []byte("tx1")},
			{Bytes: []byte("tx2")},
		},
	}
	decoder := mapDecoder{txs: map[string]domain.Transaction{
		"tx1": {
			TxID:       "t1",
			Timestamp:  1,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPut,
			Snapshot:   []byte(`{"a":1}`),
		},
		"tx2": {
			TxID:       "t2",
			Timestamp:  2,
			Collection: "users",
			DocID:      "doc1",
			Op:         domain.TxOpPatch,
			Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
			ParentHash: "h1",
		},
	}}
	hasher := mapHasher{values: map[string]string{
		"tx1":     "h1",
		"tx2":     "h2",
		"encoded": "h3",
	}}

	service := NewSnapshotService(
		fakeStreamLister{streams: []string{testStream, testStream + "_2"}},
		store,
		store,
		fakeCanonicalizer{out: []byte(`{"a":2}`)},
		&fakeEncoder{out: []byte("encoded")},
		decoder,
		fakePatcher{out: []byte(`{"a":2}`)},
		hasher,
		fakeClock{now: time.Unix(0, 123)},
		fakeIDGen{id: "01HSNAP"},
		domain.HistoryModeAppend,
	)

	result, err := service.Snapshot(context.Background(), "repo", SnapshotOptions{Threshold: 1, Max: 1})
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	if result.Processed != 1 || result.Snapshotted != 1 || result.Truncated != true || result.Skipped != 0 || len(result.Issues) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}
