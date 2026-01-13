package maintenance

import (
	"context"
	"time"

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

type WriteStore interface {
	PutTx(ctx context.Context, write doc.TxWrite) (doc.PutResult, error)
}

type GCExecutor interface {
	RunGC(ctx context.Context, repoPath, prune string) error
}

type Canonicalizer interface {
	Canonicalize(ctx context.Context, input []byte) ([]byte, error)
}

type Encoder interface {
	Encode(tx domain.Transaction) ([]byte, error)
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

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	NewID() (string, error)
}
