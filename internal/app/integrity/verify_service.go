package integrity

import (
	"context"
	"errors"
	"fmt"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

const (
	IssueHeadRead    = "head_read"
	IssueHeadMissing = "head_missing"
	IssueTxRead      = "tx_read"
	IssueTxMissing   = "tx_missing"
	IssueTxDecode    = "tx_decode"
	IssueTxInvalid   = "tx_invalid"
	IssueChain       = "chain_invalid"
	IssueOrphanTx    = "orphan_tx"
	IssueRehydrate   = "rehydrate_failed"
)

var (
	errPatchWithoutBase = errors.New("patch without base document")
	errMergeWithoutBase = errors.New("merge patch without base document")
	errPatchUnsupported = errors.New("patcher not configured")
)

type VerifyService struct {
	lister  StreamLister
	store   ReadStore
	decoder Decoder
	hasher  Hasher
	patcher Patcher
}

func NewVerifyService(lister StreamLister, store ReadStore, decoder Decoder, hasher Hasher, patcher Patcher) *VerifyService {
	return &VerifyService{
		lister:  lister,
		store:   store,
		decoder: decoder,
		hasher:  hasher,
		patcher: patcher,
	}
}

func (s *VerifyService) Verify(ctx context.Context, repoPath string, opts VerifyOptions) (VerifyResult, error) {
	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return VerifyResult{}, err
	}

	streams, err := s.lister.ListDocStreams(ctx, absRepoPath)
	if err != nil {
		return VerifyResult{}, err
	}

	result := VerifyResult{Streams: len(streams)}
	for _, streamPath := range streams {
		if err := ctx.Err(); err != nil {
			return VerifyResult{}, err
		}

		issues := s.verifyStream(ctx, absRepoPath, streamPath, opts)
		if len(issues) == 0 {
			result.Valid++
			continue
		}
		result.Issues = append(result.Issues, issues...)
	}

	return result, nil
}

func (s *VerifyService) verifyStream(ctx context.Context, repoPath, streamPath string, opts VerifyOptions) []Issue {
	headHash, err := s.store.LoadStreamHead(ctx, repoPath, streamPath)
	if err != nil {
		return []Issue{newIssue(streamPath, IssueHeadRead, err)}
	}
	if headHash == "" {
		return []Issue{newIssue(streamPath, IssueHeadMissing, errors.New("HEAD not found"))}
	}

	txBlobs, err := s.store.LoadStreamTxs(ctx, repoPath, streamPath)
	if err != nil {
		return []Issue{newIssue(streamPath, IssueTxRead, err)}
	}
	if len(txBlobs) == 0 {
		return []Issue{newIssue(streamPath, IssueTxMissing, errors.New("no tx blobs found"))}
	}

	index := make(map[string]chainEntry, len(txBlobs))
	for _, blob := range txBlobs {
		tx, err := s.decoder.Decode(blob.Bytes)
		if err != nil {
			return []Issue{newIssue(streamPath, IssueTxDecode, err)}
		}
		if err := tx.Validate(); err != nil {
			return []Issue{newIssue(streamPath, IssueTxInvalid, err)}
		}
		hash := s.hasher.SumHex(blob.Bytes)
		if _, exists := index[hash]; exists {
			return []Issue{newIssue(streamPath, IssueChain, fmt.Errorf("duplicate tx hash %s", hash))}
		}
		index[hash] = chainEntry{Hash: hash, Tx: tx}
	}

	chain, err := buildTxChain(headHash, index)
	if err != nil {
		return []Issue{newIssue(streamPath, IssueChain, err)}
	}

	var issues []Issue
	if len(chain) != len(index) {
		issues = append(issues, newIssue(streamPath, IssueOrphanTx, fmt.Errorf("%d orphan tx(s)", len(index)-len(chain))))
	}

	if opts.Deep {
		if err := verifyRehydrate(ctx, chain, s.patcher); err != nil {
			issues = append(issues, newIssue(streamPath, IssueRehydrate, err))
		}
	}

	return issues
}

type chainEntry struct {
	Hash string
	Tx   domain.Transaction
}

func buildTxChain(headHash string, index map[string]chainEntry) ([]chainEntry, error) {
	var chain []chainEntry
	visited := make(map[string]struct{}, len(index))
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

func verifyRehydrate(ctx context.Context, chain []chainEntry, patcher Patcher) error {
	var doc []byte
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}

		tx := chain[i].Tx
		switch tx.Op {
		case domain.TxOpPut:
			doc = tx.Snapshot
		case domain.TxOpPatch:
			if patcher == nil {
				return errPatchUnsupported
			}
			if doc == nil {
				return errPatchWithoutBase
			}
			updated, err := patcher.Apply(ctx, doc, tx.Patch)
			if err != nil {
				return err
			}
			doc = updated
		case domain.TxOpDelete:
			doc = nil
		case domain.TxOpMerge:
			if len(tx.Snapshot) > 0 {
				doc = tx.Snapshot
				continue
			}
			if len(tx.Patch) == 0 {
				return domain.ErrMissingPayload
			}
			if patcher == nil {
				return errPatchUnsupported
			}
			if doc == nil {
				return errMergeWithoutBase
			}
			updated, err := patcher.Apply(ctx, doc, tx.Patch)
			if err != nil {
				return err
			}
			doc = updated
		default:
			return domain.ErrInvalidOp
		}
	}
	return nil
}

func newIssue(streamPath, code string, err error) Issue {
	return Issue{
		StreamPath: streamPath,
		Code:       code,
		Message:    err.Error(),
	}
}
