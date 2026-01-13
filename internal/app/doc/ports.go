package doc

import (
	"context"
	"time"

	"github.com/codecompany/ledgerdb/internal/domain"
)

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

type WriteStore interface {
	LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error)
	PutTx(ctx context.Context, write TxWrite) (PutResult, error)
}

type ReadStore interface {
	LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error)
	LoadHeadTx(ctx context.Context, repoPath, streamPath string) (TxBlob, error)
	LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]TxBlob, error)
}
