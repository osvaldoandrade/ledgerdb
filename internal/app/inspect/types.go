package inspect

import "github.com/codecompany/ledgerdb/internal/domain"

type BlobResult struct {
	ObjectHash string
	TxHash     string
	Tx         domain.Transaction
}
