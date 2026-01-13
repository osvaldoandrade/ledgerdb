package maintenance

import (
	"context"
	"errors"
	"fmt"

	"github.com/codecompany/ledgerdb/internal/app/doc"
	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

const (
	IssueHeadRead      = "head_read"
	IssueHeadMissing   = "head_missing"
	IssueTxRead        = "tx_read"
	IssueTxMissing     = "tx_missing"
	IssueTxDecode      = "tx_decode"
	IssueTxInvalid     = "tx_invalid"
	IssueChain         = "chain_invalid"
	IssueRehydrate     = "rehydrate_failed"
	IssueCanonicalize  = "canonicalize_failed"
	IssueWriteSnapshot = "write_failed"
)

var (
	errDocDeleted       = errors.New("document deleted")
	errPatchUnsupported = errors.New("patch operations not supported")
	errPatchWithoutBase = errors.New("patch without base document")
	errMergeWithoutBase = errors.New("merge patch without base document")
)

type SnapshotService struct {
	lister        StreamLister
	readStore     ReadStore
	writeStore    WriteStore
	canonicalizer Canonicalizer
	encoder       Encoder
	decoder       Decoder
	patcher       Patcher
	hasher        Hasher
	clock         Clock
	idGen         IDGenerator
	historyMode   domain.HistoryMode
}

func NewSnapshotService(lister StreamLister, readStore ReadStore, writeStore WriteStore, canonicalizer Canonicalizer, encoder Encoder, decoder Decoder, patcher Patcher, hasher Hasher, clock Clock, idGen IDGenerator, historyMode domain.HistoryMode) *SnapshotService {
	historyMode = domain.NormalizeHistoryMode(historyMode)
	return &SnapshotService{
		lister:        lister,
		readStore:     readStore,
		writeStore:    writeStore,
		canonicalizer: canonicalizer,
		encoder:       encoder,
		decoder:       decoder,
		patcher:       patcher,
		hasher:        hasher,
		clock:         clock,
		idGen:         idGen,
		historyMode:   historyMode,
	}
}

func (s *SnapshotService) Snapshot(ctx context.Context, repoPath string, opts SnapshotOptions) (SnapshotResult, error) {
	if opts.Threshold <= 0 {
		return SnapshotResult{}, ErrInvalidThreshold
	}
	if opts.Max < 0 {
		return SnapshotResult{}, ErrInvalidMax
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return SnapshotResult{}, err
	}

	streams, err := s.lister.ListDocStreams(ctx, absRepoPath)
	if err != nil {
		return SnapshotResult{}, err
	}

	result := SnapshotResult{Streams: len(streams), DryRun: opts.DryRun}
	for _, streamPath := range streams {
		if err := ctx.Err(); err != nil {
			return SnapshotResult{}, err
		}

		if opts.Max > 0 && (result.Snapshotted+result.Planned) >= opts.Max {
			result.Truncated = true
			break
		}

		result.Processed++
		action, issues := s.snapshotStream(ctx, absRepoPath, streamPath, opts, !opts.DryRun)
		if len(issues) > 0 {
			result.Issues = append(result.Issues, issues...)
			continue
		}
		switch action {
		case snapshotCreated:
			result.Snapshotted++
		case snapshotPlanned:
			result.Planned++
		case snapshotSkipped:
			result.Skipped++
		}
	}

	return result, nil
}

type snapshotAction int

const (
	snapshotNone snapshotAction = iota
	snapshotSkipped
	snapshotPlanned
	snapshotCreated
)

func (s *SnapshotService) snapshotStream(ctx context.Context, repoPath, streamPath string, opts SnapshotOptions, apply bool) (snapshotAction, []Issue) {
	headHash, err := s.readStore.LoadStreamHead(ctx, repoPath, streamPath)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueHeadRead, err)}
	}
	if headHash == "" {
		return snapshotNone, []Issue{newIssue(streamPath, IssueHeadMissing, errors.New("HEAD not found"))}
	}

	txBlobs, err := s.readStore.LoadStreamTxs(ctx, repoPath, streamPath)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueTxRead, err)}
	}
	if len(txBlobs) == 0 {
		return snapshotNone, []Issue{newIssue(streamPath, IssueTxMissing, errors.New("no tx blobs found"))}
	}

	index, err := buildTxIndex(txBlobs, s.decoder, s.hasher)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueTxDecode, err)}
	}

	chain, err := buildTxChain(headHash, index)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueChain, err)}
	}

	if len(chain) <= opts.Threshold {
		return snapshotSkipped, nil
	}

	docBytes, headTx, err := rehydrateChain(ctx, chain, s.patcher)
	if err != nil {
		if errors.Is(err, errDocDeleted) {
			return snapshotSkipped, nil
		}
		return snapshotNone, []Issue{newIssue(streamPath, IssueRehydrate, err)}
	}

	canonical, err := s.canonicalizer.Canonicalize(ctx, docBytes)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueCanonicalize, err)}
	}

	txID, err := s.idGen.NewID()
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueWriteSnapshot, err)}
	}

	newTx := domain.Transaction{
		TxID:          txID,
		Timestamp:     s.clock.Now().UnixNano(),
		Collection:    headTx.Collection,
		DocID:         headTx.DocID,
		Op:            domain.TxOpMerge,
		Snapshot:      canonical,
		SchemaVersion: headTx.SchemaVersion,
	}
	if s.historyMode != domain.HistoryModeAmend {
		newTx.ParentHash = headHash
	}

	encoded, err := s.encoder.Encode(newTx)
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueWriteSnapshot, err)}
	}

	txHash := s.hasher.SumHex(encoded)
	if !apply {
		return snapshotPlanned, nil
	}

	_, err = s.writeStore.PutTx(ctx, doc.TxWrite{
		RepoPath:   repoPath,
		StreamPath: streamPath,
		TxBytes:    encoded,
		TxHash:     txHash,
		Tx:         newTx,
	})
	if err != nil {
		return snapshotNone, []Issue{newIssue(streamPath, IssueWriteSnapshot, err)}
	}

	return snapshotCreated, nil
}

type chainEntry struct {
	Hash string
	Tx   domain.Transaction
}

func buildTxIndex(blobs []doc.TxBlob, decoder Decoder, hasher Hasher) (map[string]chainEntry, error) {
	index := make(map[string]chainEntry, len(blobs))
	for _, blob := range blobs {
		tx, err := decoder.Decode(blob.Bytes)
		if err != nil {
			return nil, err
		}
		if err := tx.Validate(); err != nil {
			return nil, err
		}
		hash := hasher.SumHex(blob.Bytes)
		if _, exists := index[hash]; exists {
			return nil, fmt.Errorf("duplicate tx hash %s", hash)
		}
		index[hash] = chainEntry{Hash: hash, Tx: tx}
	}
	return index, nil
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

func rehydrateChain(ctx context.Context, chain []chainEntry, patcher Patcher) ([]byte, domain.Transaction, error) {
	var docBytes []byte
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, domain.Transaction{}, err
		}

		tx := chain[i].Tx
		switch tx.Op {
		case domain.TxOpPut:
			docBytes = tx.Snapshot
		case domain.TxOpPatch:
			if patcher == nil {
				return nil, domain.Transaction{}, errPatchUnsupported
			}
			if docBytes == nil {
				return nil, domain.Transaction{}, errPatchWithoutBase
			}
			updated, err := patcher.Apply(ctx, docBytes, tx.Patch)
			if err != nil {
				return nil, domain.Transaction{}, err
			}
			docBytes = updated
		case domain.TxOpDelete:
			return nil, domain.Transaction{}, errDocDeleted
		case domain.TxOpMerge:
			if len(tx.Snapshot) > 0 {
				docBytes = tx.Snapshot
				continue
			}
			if len(tx.Patch) == 0 {
				return nil, domain.Transaction{}, domain.ErrMissingPayload
			}
			if patcher == nil {
				return nil, domain.Transaction{}, errPatchUnsupported
			}
			if docBytes == nil {
				return nil, domain.Transaction{}, errMergeWithoutBase
			}
			updated, err := patcher.Apply(ctx, docBytes, tx.Patch)
			if err != nil {
				return nil, domain.Transaction{}, err
			}
			docBytes = updated
		default:
			return nil, domain.Transaction{}, domain.ErrInvalidOp
		}
	}

	if docBytes == nil {
		return nil, domain.Transaction{}, doc.ErrDocNotFound
	}

	return docBytes, chain[0].Tx, nil
}

func newIssue(streamPath, code string, err error) Issue {
	return Issue{
		StreamPath: streamPath,
		Code:       code,
		Message:    err.Error(),
	}
}
