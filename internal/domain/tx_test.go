package domain

import "testing"

func TestTransactionValidatePut(t *testing.T) {
	tx := Transaction{
		TxID:       "01H123",
		Timestamp:  1,
		Collection: "users",
		DocID:      "user_1",
		Op:         TxOpPut,
		Snapshot:   []byte(`{"name":"Ada"}`),
	}

	if err := tx.Validate(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestTransactionValidatePatchMissingPayload(t *testing.T) {
	tx := Transaction{
		TxID:       "01H123",
		Timestamp:  1,
		Collection: "users",
		DocID:      "user_1",
		Op:         TxOpPatch,
	}

	if err := tx.Validate(); err != ErrMissingPayload {
		t.Fatalf("expected ErrMissingPayload, got %v", err)
	}
}

func TestTransactionValidateDeleteRejectsPayload(t *testing.T) {
	tx := Transaction{
		TxID:       "01H123",
		Timestamp:  1,
		Collection: "users",
		DocID:      "user_1",
		Op:         TxOpDelete,
		Snapshot:   []byte(`{"name":"Ada"}`),
	}

	if err := tx.Validate(); err != ErrUnexpectedPayload {
		t.Fatalf("expected ErrUnexpectedPayload, got %v", err)
	}
}

func TestTransactionValidateMergeRequiresPayload(t *testing.T) {
	tx := Transaction{
		TxID:       "01H123",
		Timestamp:  1,
		Collection: "users",
		DocID:      "user_1",
		Op:         TxOpMerge,
	}

	if err := tx.Validate(); err != ErrMissingPayload {
		t.Fatalf("expected ErrMissingPayload, got %v", err)
	}
}

func TestTransactionValidateOp(t *testing.T) {
	tx := Transaction{
		TxID:       "01H123",
		Timestamp:  1,
		Collection: "users",
		DocID:      "user_1",
		Op:         TxOpUnknown,
		Snapshot:   []byte(`{"name":"Ada"}`),
	}

	if err := tx.Validate(); err != ErrInvalidOp {
		t.Fatalf("expected ErrInvalidOp, got %v", err)
	}
}
