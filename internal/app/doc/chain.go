package doc

import (
	"context"
	"fmt"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type txChainEntry struct {
	Hash string
	Tx   domain.Transaction
}

func buildTxIndex(blobs []TxBlob, decoder Decoder, hasher Hasher) (map[string]txChainEntry, error) {
	index := make(map[string]txChainEntry, len(blobs))
	for _, blob := range blobs {
		tx, err := decoder.Decode(blob.Bytes)
		if err != nil {
			return nil, err
		}
		hash := hasher.SumHex(blob.Bytes)
		index[hash] = txChainEntry{
			Hash: hash,
			Tx:   tx,
		}
	}
	return index, nil
}

func buildTxChain(headHash string, index map[string]txChainEntry) ([]txChainEntry, error) {
	var chain []txChainEntry
	visited := make(map[string]struct{})
	current := headHash
	for current != "" {
		if _, ok := visited[current]; ok {
			return nil, fmt.Errorf("cycle detected at %s", current)
		}
		visited[current] = struct{}{}

		entry, ok := index[current]
		if !ok {
			return nil, fmt.Errorf("missing tx %s", current)
		}
		chain = append(chain, entry)
		current = entry.Tx.ParentHash
	}
	return chain, nil
}

func rehydrateChain(ctx context.Context, chain []txChainEntry, patcher Patcher) ([]byte, domain.Transaction, error) {
	var doc []byte
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, domain.Transaction{}, err
		}

		tx := chain[i].Tx
		switch tx.Op {
		case domain.TxOpPut:
			doc = tx.Snapshot
		case domain.TxOpPatch:
			if patcher == nil {
				return nil, domain.Transaction{}, ErrPatchUnsupported
			}
			if doc == nil {
				return nil, domain.Transaction{}, ErrPatchUnsupported
			}
			updated, err := patcher.Apply(ctx, doc, tx.Patch)
			if err != nil {
				return nil, domain.Transaction{}, err
			}
			doc = updated
		case domain.TxOpDelete:
			return nil, domain.Transaction{}, ErrDocDeleted
		case domain.TxOpMerge:
			if len(tx.Snapshot) > 0 {
				doc = tx.Snapshot
				continue
			}
			if len(tx.Patch) == 0 {
				return nil, domain.Transaction{}, ErrPatchUnsupported
			}
			if patcher == nil {
				return nil, domain.Transaction{}, ErrPatchUnsupported
			}
			updated, err := patcher.Apply(ctx, doc, tx.Patch)
			if err != nil {
				return nil, domain.Transaction{}, err
			}
			doc = updated
		default:
			return nil, domain.Transaction{}, ErrPatchUnsupported
		}
	}

	if doc == nil {
		return nil, domain.Transaction{}, ErrDocNotFound
	}

	return doc, chain[0].Tx, nil
}
