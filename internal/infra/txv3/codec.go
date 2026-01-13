package txv3

import (
	"fmt"

	"github.com/codecompany/ledgerdb/internal/domain"
	"google.golang.org/protobuf/proto"
)

type Encoder struct{}

func (Encoder) Encode(tx domain.Transaction) ([]byte, error) {
	return Encode(tx)
}

type Decoder struct{}

func (Decoder) Decode(data []byte) (domain.Transaction, error) {
	return Decode(data)
}

func Encode(tx domain.Transaction) ([]byte, error) {
	if err := tx.Validate(); err != nil {
		return nil, err
	}

	pb, err := toProto(tx)
	if err != nil {
		return nil, err
	}

	return proto.MarshalOptions{Deterministic: true}.Marshal(pb)
}

func Decode(data []byte) (domain.Transaction, error) {
	var pb Transaction
	if err := proto.Unmarshal(data, &pb); err != nil {
		return domain.Transaction{}, fmt.Errorf("decode txv3: %w", err)
	}

	return fromProto(&pb)
}

func toProto(tx domain.Transaction) (*Transaction, error) {
	op, err := toProtoOp(tx.Op)
	if err != nil {
		return nil, err
	}

	pb := &Transaction{
		TxId:          tx.TxID,
		Timestamp:     tx.Timestamp,
		Collection:    tx.Collection,
		DocId:         tx.DocID,
		Op:            op,
		ParentHash:    tx.ParentHash,
		SchemaVersion: tx.SchemaVersion,
	}

	if len(tx.Snapshot) > 0 {
		pb.Payload = &Transaction_Snapshot{Snapshot: tx.Snapshot}
	} else if len(tx.Patch) > 0 {
		pb.Payload = &Transaction_Patch{Patch: tx.Patch}
	}

	return pb, nil
}

func fromProto(pb *Transaction) (domain.Transaction, error) {
	op, err := fromProtoOp(pb.Op)
	if err != nil {
		return domain.Transaction{}, err
	}

	tx := domain.Transaction{
		TxID:          pb.TxId,
		Timestamp:     pb.Timestamp,
		Collection:    pb.Collection,
		DocID:         pb.DocId,
		Op:            op,
		ParentHash:    pb.ParentHash,
		SchemaVersion: pb.SchemaVersion,
	}

	switch payload := pb.Payload.(type) {
	case *Transaction_Snapshot:
		tx.Snapshot = payload.Snapshot
	case *Transaction_Patch:
		tx.Patch = payload.Patch
	}

	return tx, nil
}

func toProtoOp(op domain.TxOp) (Transaction_Op, error) {
	switch op {
	case domain.TxOpPut:
		return Transaction_PUT, nil
	case domain.TxOpPatch:
		return Transaction_PATCH, nil
	case domain.TxOpDelete:
		return Transaction_DELETE, nil
	case domain.TxOpMerge:
		return Transaction_MERGE, nil
	default:
		return Transaction_UNKNOWN, fmt.Errorf("invalid tx op: %v", op)
	}
}

func fromProtoOp(op Transaction_Op) (domain.TxOp, error) {
	switch op {
	case Transaction_PUT:
		return domain.TxOpPut, nil
	case Transaction_PATCH:
		return domain.TxOpPatch, nil
	case Transaction_DELETE:
		return domain.TxOpDelete, nil
	case Transaction_MERGE:
		return domain.TxOpMerge, nil
	default:
		return domain.TxOpUnknown, fmt.Errorf("invalid proto op: %v", op)
	}
}
