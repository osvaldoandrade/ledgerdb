package inspect

import (
	"context"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type BlobReader interface {
	ReadBlob(ctx context.Context, repoPath, objectHash string) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte) (domain.Transaction, error)
}

type Hasher interface {
	SumHex(data []byte) string
}
