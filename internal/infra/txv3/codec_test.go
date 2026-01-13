package txv3

import (
	"bytes"
	"testing"

	"github.com/codecompany/ledgerdb/internal/domain"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tx := domain.Transaction{
		TxID:       "01H123",
		Timestamp:  123,
		Collection: "users",
		DocID:      "user_1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"name":"Ada"}`),
		ParentHash: "abc",
	}

	data, err := Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	if decoded.TxID != tx.TxID || decoded.Timestamp != tx.Timestamp || decoded.Collection != tx.Collection || decoded.DocID != tx.DocID {
		t.Fatalf("decoded tx metadata mismatch: %+v", decoded)
	}
	if decoded.Op != tx.Op {
		t.Fatalf("expected op %v, got %v", tx.Op, decoded.Op)
	}
	if !bytes.Equal(decoded.Snapshot, tx.Snapshot) {
		t.Fatalf("snapshot mismatch")
	}
	if decoded.ParentHash != tx.ParentHash {
		t.Fatalf("expected parent hash %q, got %q", tx.ParentHash, decoded.ParentHash)
	}
}

func TestEncodeDeterministic(t *testing.T) {
	tx := domain.Transaction{
		TxID:       "01H123",
		Timestamp:  123,
		Collection: "users",
		DocID:      "user_1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"name":"Ada"}`),
	}

	first, err := Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	second, err := Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	if !bytes.Equal(first, second) {
		t.Fatalf("expected deterministic encoding")
	}
}
