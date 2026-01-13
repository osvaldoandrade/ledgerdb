package domain

import "errors"

type TxOp int

const (
	TxOpUnknown TxOp = iota
	TxOpPut
	TxOpPatch
	TxOpDelete
	TxOpMerge
)

var (
	ErrTxIDRequired       = errors.New("tx id is required")
	ErrTimestampRequired  = errors.New("timestamp is required")
	ErrCollectionRequired = errors.New("collection is required")
	ErrDocIDRequired      = errors.New("doc id is required")
	ErrInvalidOp          = errors.New("invalid tx op")
	ErrMissingPayload     = errors.New("payload is required")
	ErrUnexpectedPayload  = errors.New("payload is not allowed")
	ErrMultiplePayloads   = errors.New("multiple payloads provided")
)

type Transaction struct {
	TxID          string
	Timestamp     int64
	Collection    string
	DocID         string
	Op            TxOp
	Snapshot      []byte
	Patch         []byte
	ParentHash    string
	SchemaVersion string
}

func (op TxOp) IsValid() bool {
	return op >= TxOpPut && op <= TxOpMerge
}

func (op TxOp) String() string {
	switch op {
	case TxOpPut:
		return "put"
	case TxOpPatch:
		return "patch"
	case TxOpDelete:
		return "delete"
	case TxOpMerge:
		return "merge"
	default:
		return "unknown"
	}
}

func (t Transaction) Validate() error {
	if t.TxID == "" {
		return ErrTxIDRequired
	}
	if t.Timestamp == 0 {
		return ErrTimestampRequired
	}
	if t.Collection == "" {
		return ErrCollectionRequired
	}
	if t.DocID == "" {
		return ErrDocIDRequired
	}
	if !t.Op.IsValid() {
		return ErrInvalidOp
	}
	if len(t.Snapshot) > 0 && len(t.Patch) > 0 {
		return ErrMultiplePayloads
	}

	switch t.Op {
	case TxOpPut:
		if len(t.Snapshot) == 0 {
			return ErrMissingPayload
		}
	case TxOpPatch:
		if len(t.Patch) == 0 {
			return ErrMissingPayload
		}
	case TxOpDelete:
		if len(t.Snapshot) > 0 || len(t.Patch) > 0 {
			return ErrUnexpectedPayload
		}
	case TxOpMerge:
		if len(t.Snapshot) == 0 && len(t.Patch) == 0 {
			return ErrMissingPayload
		}
	}

	return nil
}
