package inspect

import "github.com/osvaldoandrade/ledgerdb/internal/domain"

type BlobResult struct {
	ObjectHash string
	TxHash     string
	Tx         domain.Transaction
}
