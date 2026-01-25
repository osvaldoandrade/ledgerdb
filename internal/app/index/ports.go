package index

import (
	"context"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type Fetcher interface {
	Fetch(ctx context.Context, repoPath string) error
}

type CommitSource interface {
	ListCommitHashes(ctx context.Context, repoPath, sinceHash string) ([]string, error)
	CommitTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error)
	CommitStateTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error)
	StateTxsSince(ctx context.Context, repoPath string, state State) (StateTxsResult, error)
}

type Store interface {
	GetState(ctx context.Context) (State, error)
	Begin(ctx context.Context) (StoreTx, error)
	Reset(ctx context.Context) error
}

type StoreTx interface {
	EnsureCollection(ctx context.Context, collection string) (string, error)
	GetDoc(ctx context.Context, collection, docID string) (DocRecord, bool, error)
	UpsertDoc(ctx context.Context, collection string, record DocRecord) error
	SetState(ctx context.Context, state State) error
	Commit() error
	Rollback() error
}

type Canonicalizer interface {
	Canonicalize(ctx context.Context, input []byte) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte) (domain.Transaction, error)
}

type Patcher interface {
	Apply(ctx context.Context, doc, patch []byte) ([]byte, error)
}

type Hasher interface {
	SumHex(data []byte) string
}
