package doc

import "github.com/codecompany/ledgerdb/internal/domain"

type PutResult struct {
	CommitHash string
	TxHash     string
	TxID       string
}

type GetResult struct {
	Payload []byte
	TxHash  string
	TxID    string
	Op      domain.TxOp
}

type TxWrite struct {
	RepoPath     string
	StreamPath   string
	TxBytes      []byte
	TxHash       string
	Tx           domain.Transaction
	StatePath    string
	StateTxBytes []byte
	StateTxHash  string
	StateTx      domain.Transaction
}

type TxBlob struct {
	Path  string
	Bytes []byte
}

type LogEntry struct {
	TxID       string
	TxHash     string
	ParentHash string
	Timestamp  int64
	Op         domain.TxOp
}

type RevertOptions struct {
	TxID   string
	TxHash string
}
