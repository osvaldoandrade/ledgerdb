package integrity

import (
	"context"

	"github.com/codecompany/ledgerdb/internal/app/doc"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type StreamLister interface {
	ListDocStreams(ctx context.Context, repoPath string) ([]string, error)
}

type ReadStore interface {
	LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error)
	LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error)
}

type Decoder interface {
	Decode(data []byte) (domain.Transaction, error)
}

type Hasher interface {
	SumHex(data []byte) string
}

type Patcher interface {
	Apply(ctx context.Context, doc, patch []byte) ([]byte, error)
}

type VerifyOptions struct {
	Deep bool
}

type VerifyResult struct {
	Streams int
	Valid   int
	Issues  []Issue
}

type Issue struct {
	StreamPath string
	Code       string
	Message    string
}
