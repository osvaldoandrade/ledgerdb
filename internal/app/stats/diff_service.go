package stats

import (
	"context"
	"sort"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// DiffService computes per-collection document differences between two refs.
type DiffService struct {
	store DiffStore
}

// NewDiffService builds a DiffService.
func NewDiffService(store DiffStore) *DiffService {
	return &DiffService{store: store}
}

// DiffOptions controls the diff command.
type DiffOptions struct {
	// Collection, when set, limits results to a single collection.
	Collection string
	// Limit caps the number of entries reported per category, per collection (0 = unlimited).
	Limit int
}

// Diff walks the commit graph between refA and refB and reports per-collection
// added/changed/removed documents.
func (s *DiffService) Diff(ctx context.Context, repoPath, refA, refB string, opts DiffOptions) (DiffResult, error) {
	refA = strings.TrimSpace(refA)
	refB = strings.TrimSpace(refB)
	if refA == "" || refB == "" {
		return DiffResult{}, ErrRefRequired
	}
	if opts.Limit < 0 {
		return DiffResult{}, ErrInvalidLimit
	}

	absPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return DiffResult{}, err
	}

	hashA, err := s.store.ResolveRefHash(ctx, absPath, refA)
	if err != nil {
		return DiffResult{}, err
	}
	hashB, err := s.store.ResolveRefHash(ctx, absPath, refB)
	if err != nil {
		return DiffResult{}, err
	}

	stateA, err := s.collectState(ctx, absPath, hashA)
	if err != nil {
		return DiffResult{}, err
	}
	stateB, err := s.collectState(ctx, absPath, hashB)
	if err != nil {
		return DiffResult{}, err
	}

	filter := strings.TrimSpace(opts.Collection)
	collections := s.buildSummaries(stateA, stateB, filter, opts.Limit)

	return DiffResult{
		RefA:        refA,
		RefB:        refB,
		HashA:       hashA,
		HashB:       hashB,
		Collections: collections,
		Limit:       opts.Limit,
	}, nil
}

// docKey identifies a unique document.
type docKey struct {
	Collection string
	DocID      string
}

// collectState walks all commits reachable from `ref` and folds every TxV3 into a
// per-document head state (latest tx wins by timestamp, then tx_hash).
func (s *DiffService) collectState(ctx context.Context, repoPath, ref string) (map[docKey]TxRecord, error) {
	if ref == "" {
		return map[docKey]TxRecord{}, nil
	}
	commits, err := s.store.CommitsInRange(ctx, repoPath, "", ref)
	if err != nil {
		return nil, err
	}

	state := make(map[docKey]TxRecord)
	for _, commit := range commits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		txs, err := s.store.CollectCommitTxs(ctx, repoPath, commit)
		if err != nil {
			return nil, err
		}
		for _, tx := range txs {
			key := docKey{Collection: tx.Collection, DocID: tx.DocID}
			existing, ok := state[key]
			if !ok || isNewer(tx, existing) {
				state[key] = tx
			}
		}
	}
	return state, nil
}

func isNewer(candidate, current TxRecord) bool {
	if candidate.Timestamp != current.Timestamp {
		return candidate.Timestamp > current.Timestamp
	}
	return candidate.TxHash > current.TxHash
}

func (s *DiffService) buildSummaries(stateA, stateB map[docKey]TxRecord, filter string, limit int) []DiffCollectionSummary {
	byCollection := make(map[string]*DiffCollectionSummary)

	visit := func(collection string) *DiffCollectionSummary {
		if filter != "" && collection != filter {
			return nil
		}
		summary, ok := byCollection[collection]
		if !ok {
			summary = &DiffCollectionSummary{Collection: collection}
			byCollection[collection] = summary
		}
		return summary
	}

	for key, txB := range stateB {
		summary := visit(key.Collection)
		if summary == nil {
			continue
		}
		txA, inA := stateA[key]
		switch {
		case !inA && !isTombstone(txB):
			recordAdded(summary, key, txB, limit)
		case inA && txA.TxHash != txB.TxHash && isTombstone(txB) && !isTombstone(txA):
			recordRemoved(summary, key, txA.TxHash, txB.TxHash, limit)
		case inA && txA.TxHash != txB.TxHash && !isTombstone(txB):
			recordChanged(summary, key, txA.TxHash, txB.TxHash, limit)
		}
	}

	for key, txA := range stateA {
		if _, present := stateB[key]; present {
			continue
		}
		if isTombstone(txA) {
			continue
		}
		summary := visit(key.Collection)
		if summary == nil {
			continue
		}
		recordRemoved(summary, key, txA.TxHash, "", limit)
	}

	out := make([]DiffCollectionSummary, 0, len(byCollection))
	for _, summary := range byCollection {
		sort.Slice(summary.Added, func(i, j int) bool { return summary.Added[i].DocID < summary.Added[j].DocID })
		sort.Slice(summary.Changed, func(i, j int) bool { return summary.Changed[i].DocID < summary.Changed[j].DocID })
		sort.Slice(summary.Removed, func(i, j int) bool { return summary.Removed[i].DocID < summary.Removed[j].DocID })
		out = append(out, *summary)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Collection < out[j].Collection })
	return out
}

func isTombstone(tx TxRecord) bool {
	return tx.Op == domain.TxOpDelete
}

func recordAdded(summary *DiffCollectionSummary, key docKey, tx TxRecord, limit int) {
	summary.AddedTotal++
	if underLimit(limit, len(summary.Added)) {
		summary.Added = append(summary.Added, DiffEntry{
			Collection: key.Collection,
			DocID:      key.DocID,
			ToHash:     tx.TxHash,
		})
	}
}

func recordChanged(summary *DiffCollectionSummary, key docKey, fromHash, toHash string, limit int) {
	summary.ChangedAll++
	if underLimit(limit, len(summary.Changed)) {
		summary.Changed = append(summary.Changed, DiffEntry{
			Collection: key.Collection,
			DocID:      key.DocID,
			FromHash:   fromHash,
			ToHash:     toHash,
		})
	}
}

func recordRemoved(summary *DiffCollectionSummary, key docKey, fromHash, toHash string, limit int) {
	summary.RemovedAll++
	if underLimit(limit, len(summary.Removed)) {
		summary.Removed = append(summary.Removed, DiffEntry{
			Collection: key.Collection,
			DocID:      key.DocID,
			FromHash:   fromHash,
			ToHash:     toHash,
		})
	}
}

func underLimit(limit, current int) bool {
	return limit == 0 || current < limit
}
