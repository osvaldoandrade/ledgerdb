package stats

import (
	"context"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// StatsStore is the minimal git-backed surface required to compute a stats summary.
type StatsStore interface {
	LoadStatus(ctx context.Context, repoPath string) (domain.RepoStatus, error)
	ListDocStreams(ctx context.Context, repoPath string) ([]string, error)
	LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error)
	LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error)
	CountObjects(ctx context.Context, repoPath string) (CountObjects, error)
	RepoDiskBytes(ctx context.Context, repoPath string) (int64, error)
	AheadBehind(ctx context.Context, repoPath, localRef, remoteRef string) (ahead, behind int, err error)
}

// DiffStore is the minimal git-backed surface required to compute a cross-ref diff.
type DiffStore interface {
	ResolveRefHash(ctx context.Context, repoPath, ref string) (string, error)
	CollectCommitTxs(ctx context.Context, repoPath, commitHash string) ([]TxRecord, error)
	CommitsInRange(ctx context.Context, repoPath, from, to string) ([]string, error)
}

// Decoder decodes a serialized TxV3 payload.
type Decoder interface {
	Decode(data []byte) (domain.Transaction, error)
}
