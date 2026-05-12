package dr

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeLister struct{ streams []string }

func (f fakeLister) ListDocStreams(context.Context, string) ([]string, error) {
	return f.streams, nil
}

type fakeReadStore struct {
	head   map[string]string
	blobs  map[string][]doc.TxBlob
}

func (f fakeReadStore) LoadStreamHead(_ context.Context, _, stream string) (string, error) {
	return f.head[stream], nil
}

func (f fakeReadStore) LoadStreamTxs(_ context.Context, _, stream string) ([]doc.TxBlob, error) {
	return f.blobs[stream], nil
}

type fakeDecoder struct {
	byBytes map[string]domain.Transaction
}

func (f fakeDecoder) Decode(b []byte) (domain.Transaction, error) {
	return f.byBytes[string(b)], nil
}

type fakeHasher struct{}

func (fakeHasher) SumHex(b []byte) string { return string(b) }

func TestParseThresholdAcceptsRFC3339(t *testing.T) {
	ts, txID, err := parseThreshold("2026-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if txID != "" {
		t.Fatalf("did not expect tx id, got %q", txID)
	}
	if ts == 0 {
		t.Fatalf("expected non-zero timestamp")
	}
}

func TestParseThresholdAcceptsDateOnly(t *testing.T) {
	ts, _, err := parseThreshold("2026-01-01")
	if err != nil {
		t.Fatal(err)
	}
	if ts == 0 {
		t.Fatalf("expected non-zero timestamp")
	}
}

func TestParseThresholdAcceptsUnixNanos(t *testing.T) {
	ts, _, err := parseThreshold("1700000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	if ts != 1700000000000000000 {
		t.Fatalf("expected unix nanos passthrough, got %d", ts)
	}
}

func TestParseThresholdAcceptsULID(t *testing.T) {
	_, txID, err := parseThreshold("01HZX0000000000000000000AB")
	if err != nil {
		t.Fatalf("expected tx id parse, got %v", err)
	}
	if txID == "" {
		t.Fatalf("expected non-empty tx id")
	}
}

func TestParseThresholdRejectsInvalid(t *testing.T) {
	_, _, err := parseThreshold("not-a-real-threshold")
	if !errors.Is(err, ErrInvalidThreshold) {
		t.Fatalf("expected ErrInvalidThreshold, got %v", err)
	}
}

func TestTruncateRequiresConfirmation(t *testing.T) {
	svc := NewTruncateService(fakeLister{}, fakeReadStore{}, fakeDecoder{}, fakeHasher{}, nil, domain.StreamLayoutFlat)
	_, err := svc.Truncate(context.Background(), t.TempDir(), TruncateOptions{Before: "2026-01-01"})
	if !errors.Is(err, ErrTruncateConfirmation) {
		t.Fatalf("expected ErrTruncateConfirmation, got %v", err)
	}
}

func TestTruncateRequiresThreshold(t *testing.T) {
	svc := NewTruncateService(fakeLister{}, fakeReadStore{}, fakeDecoder{}, fakeHasher{}, nil, domain.StreamLayoutFlat)
	_, err := svc.Truncate(context.Background(), t.TempDir(), TruncateOptions{DryRun: true})
	if !errors.Is(err, ErrTruncateThresholdRequired) {
		t.Fatalf("expected ErrTruncateThresholdRequired, got %v", err)
	}
}

func TestTruncatePlansDropForOlderTxs(t *testing.T) {
	const stream = "documents/users/DOC_abc"
	// Build a chain: tx1 (put, ts=100) <- tx2 (patch, ts=300)
	tx1 := domain.Transaction{
		TxID: "01ULIDONE0000000000000000A", Timestamp: 100, Collection: "users",
		DocID: "alice", Op: domain.TxOpPut, Snapshot: []byte(`{"v":1}`),
	}
	tx2 := domain.Transaction{
		TxID: "01ULIDTWO0000000000000000B", Timestamp: 300, Collection: "users",
		DocID: "alice", Op: domain.TxOpPatch, Patch: []byte(`[]`), ParentHash: "tx1",
	}

	lister := fakeLister{streams: []string{stream}}
	store := fakeReadStore{
		head: map[string]string{stream: "tx2"},
		blobs: map[string][]doc.TxBlob{stream: {
			{Path: "tx1", Bytes: []byte("tx1")},
			{Path: "tx2", Bytes: []byte("tx2")},
		}},
	}
	decoder := fakeDecoder{byBytes: map[string]domain.Transaction{
		"tx1": tx1, "tx2": tx2,
	}}

	svc := NewTruncateService(lister, store, decoder, fakeHasher{}, nil, domain.StreamLayoutFlat)
	result, err := svc.Truncate(context.Background(), t.TempDir(), TruncateOptions{
		Before: "200", DryRun: true,
	})
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if len(result.Plan.Streams) != 1 {
		t.Fatalf("expected one stream in plan, got %d", len(result.Plan.Streams))
	}
	sp := result.Plan.Streams[0]
	if sp.DroppedTx != 1 {
		t.Fatalf("expected to drop 1 tx, got %d", sp.DroppedTx)
	}
	if sp.KeptTx != 1 {
		t.Fatalf("expected to keep 1 tx, got %d", sp.KeptTx)
	}
}
