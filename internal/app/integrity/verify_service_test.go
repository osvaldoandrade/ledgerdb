package integrity

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

const testStreamPath = "documents/users/DOC_deadbeef"

type fakeLister struct {
	streams []string
	err     error
}

func (f fakeLister) ListDocStreams(ctx context.Context, repoPath string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.streams, nil
}

type fakeStore struct {
	head    string
	headErr error
	txs     []doc.TxBlob
	txErr   error
}

func (f fakeStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.headErr != nil {
		return "", f.headErr
	}
	return f.head, nil
}

func (f fakeStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error) {
	if f.txErr != nil {
		return nil, f.txErr
	}
	return f.txs, nil
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
	hashes map[string]string
}

func (h mapHasher) SumHex(data []byte) string {
	return h.hashes[string(data)]
}

type fakePatcher struct {
	err error
}

func (f fakePatcher) Apply(ctx context.Context, docBytes, patch []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []byte(`{"ok":true}`), nil
}

func TestVerifyReportsMissingHead(t *testing.T) {
	service := NewVerifyService(
		fakeLister{streams: []string{testStreamPath}},
		fakeStore{head: ""},
		mapDecoder{},
		mapHasher{},
		nil,
	)

	result, err := service.Verify(context.Background(), "repo", VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if len(result.Issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(result.Issues))
	}
	if result.Issues[0].Code != IssueHeadMissing {
		t.Fatalf("expected %s, got %s", IssueHeadMissing, result.Issues[0].Code)
	}
}

func TestVerifyAcceptsValidChain(t *testing.T) {
	tx1 := domain.Transaction{
		TxID:       "t1",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	}
	tx2 := domain.Transaction{
		TxID:       "t2",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
		ParentHash: "h1",
	}

	service := NewVerifyService(
		fakeLister{streams: []string{testStreamPath}},
		fakeStore{
			head: "h2",
			txs: []doc.TxBlob{
				{Bytes: []byte("tx1")},
				{Bytes: []byte("tx2")},
			},
		},
		mapDecoder{txs: map[string]domain.Transaction{
			"tx1": tx1,
			"tx2": tx2,
		}},
		mapHasher{hashes: map[string]string{
			"tx1": "h1",
			"tx2": "h2",
		}},
		nil,
	)

	result, err := service.Verify(context.Background(), "repo", VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if result.Valid != 1 || len(result.Issues) != 0 {
		t.Fatalf("expected no issues, got %+v", result)
	}
}

func TestVerifyDetectsOrphanTxs(t *testing.T) {
	tx1 := domain.Transaction{
		TxID:       "t1",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	}
	tx2 := domain.Transaction{
		TxID:       "t2",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
		ParentHash: "h1",
	}
	tx3 := domain.Transaction{
		TxID:       "t3",
		Timestamp:  3,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":3}`),
	}

	service := NewVerifyService(
		fakeLister{streams: []string{testStreamPath}},
		fakeStore{
			head: "h2",
			txs: []doc.TxBlob{
				{Bytes: []byte("tx1")},
				{Bytes: []byte("tx2")},
				{Bytes: []byte("tx3")},
			},
		},
		mapDecoder{txs: map[string]domain.Transaction{
			"tx1": tx1,
			"tx2": tx2,
			"tx3": tx3,
		}},
		mapHasher{hashes: map[string]string{
			"tx1": "h1",
			"tx2": "h2",
			"tx3": "h3",
		}},
		nil,
	)

	result, err := service.Verify(context.Background(), "repo", VerifyOptions{})
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if result.Valid != 0 || len(result.Issues) != 1 {
		t.Fatalf("expected 1 issue, got %+v", result)
	}
	if result.Issues[0].Code != IssueOrphanTx {
		t.Fatalf("expected %s, got %s", IssueOrphanTx, result.Issues[0].Code)
	}
}

func TestVerifyDeepReportsPatchFailure(t *testing.T) {
	tx1 := domain.Transaction{
		TxID:       "t1",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	}
	tx2 := domain.Transaction{
		TxID:       "t2",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
		ParentHash: "h1",
	}

	patchErr := errors.New("patch failed")
	service := NewVerifyService(
		fakeLister{streams: []string{testStreamPath}},
		fakeStore{
			head: "h2",
			txs: []doc.TxBlob{
				{Bytes: []byte("tx1")},
				{Bytes: []byte("tx2")},
			},
		},
		mapDecoder{txs: map[string]domain.Transaction{
			"tx1": tx1,
			"tx2": tx2,
		}},
		mapHasher{hashes: map[string]string{
			"tx1": "h1",
			"tx2": "h2",
		}},
		fakePatcher{err: patchErr},
	)

	result, err := service.Verify(context.Background(), "repo", VerifyOptions{Deep: true})
	if err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
	if result.Valid != 0 || len(result.Issues) != 1 {
		t.Fatalf("expected 1 issue, got %+v", result)
	}
	if result.Issues[0].Code != IssueRehydrate {
		t.Fatalf("expected %s, got %s", IssueRehydrate, result.Issues[0].Code)
	}
}
