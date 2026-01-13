package index

import (
	"context"
	"errors"
	"sort"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type SyncService struct {
	fetcher       Fetcher
	source        CommitSource
	store         Store
	canonicalizer Canonicalizer
	decoder       Decoder
	patcher       Patcher
	hasher        Hasher
}

func NewSyncService(fetcher Fetcher, source CommitSource, store Store, canonicalizer Canonicalizer, decoder Decoder, patcher Patcher, hasher Hasher) *SyncService {
	return &SyncService{
		fetcher:       fetcher,
		source:        source,
		store:         store,
		canonicalizer: canonicalizer,
		decoder:       decoder,
		patcher:       patcher,
		hasher:        hasher,
	}
}

func (s *SyncService) Sync(ctx context.Context, repoPath string, opts SyncOptions) (SyncResult, error) {
	if err := s.ensureDeps(); err != nil {
		return SyncResult{}, err
	}

	if opts.Fetch {
		if s.fetcher == nil {
			return SyncResult{}, ErrFetchUnavailable
		}
		if err := s.fetcher.Fetch(ctx, repoPath); err != nil {
			return SyncResult{}, err
		}
	}

	mode := NormalizeMode(opts.Mode)
	if mode == ModeState {
		result, err := s.syncState(ctx, repoPath, opts)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, ErrStateUnavailable) {
			return result, err
		}
	}

	return s.syncHistory(ctx, repoPath, opts)
}

func (s *SyncService) syncHistory(ctx context.Context, repoPath string, opts SyncOptions) (SyncResult, error) {
	state, err := s.store.GetState(ctx)
	if err != nil {
		return SyncResult{}, err
	}

	commitHashes, err := s.source.ListCommitHashes(ctx, repoPath, state.LastCommit)
	reset := false
	if err != nil {
		if errors.Is(err, ErrCommitNotFound) && opts.AllowReset {
			if err := s.store.Reset(ctx); err != nil {
				return SyncResult{}, err
			}
			state = State{}
			reset = true
			commitHashes, err = s.source.ListCommitHashes(ctx, repoPath, "")
		}
		if err != nil {
			return SyncResult{}, err
		}
	}

	result := SyncResult{
		Reset:   reset,
		Fetched: opts.Fetch,
		Commits: len(commitHashes),
	}

	batchSize := opts.BatchCommits
	if batchSize <= 0 {
		batchSize = 1
	}

	collections := make(map[string]struct{})
	for start := 0; start < len(commitHashes); start += batchSize {
		end := start + batchSize
		if end > len(commitHashes) {
			end = len(commitHashes)
		}

		storeTx, err := s.store.Begin(ctx)
		if err != nil {
			return result, err
		}

		for _, commitHash := range commitHashes[start:end] {
			if err := ctx.Err(); err != nil {
				_ = storeTx.Rollback()
				return result, err
			}

			txBlobs, err := s.source.CommitTxs(ctx, repoPath, commitHash)
			if err != nil {
				_ = storeTx.Rollback()
				return result, err
			}

			decoded, err := s.decodeTxs(txBlobs)
			if err != nil {
				_ = storeTx.Rollback()
				return result, err
			}

			if err := s.applyTxs(ctx, storeTx, decoded, collections, &result); err != nil {
				_ = storeTx.Rollback()
				return result, err
			}

			result.LastCommit = commitHash
		}

		if result.LastCommit != "" {
			if err := storeTx.SetState(ctx, State{LastCommit: result.LastCommit}); err != nil {
				_ = storeTx.Rollback()
				return result, err
			}
		}
		if err := storeTx.Commit(); err != nil {
			return result, err
		}
	}

	result.Collections = len(collections)
	if result.LastCommit == "" {
		result.LastCommit = state.LastCommit
	}
	return result, nil
}

func (s *SyncService) syncState(ctx context.Context, repoPath string, opts SyncOptions) (SyncResult, error) {
	state, err := s.store.GetState(ctx)
	if err != nil {
		return SyncResult{}, err
	}

	result := SyncResult{
		Fetched: opts.Fetch,
	}

	stateResult, err := s.source.StateTxsSince(ctx, repoPath, state)
	if err != nil {
		if errors.Is(err, ErrCommitNotFound) && opts.AllowReset {
			if err := s.store.Reset(ctx); err != nil {
				return SyncResult{}, err
			}
			state = State{}
			result.Reset = true
			stateResult, err = s.source.StateTxsSince(ctx, repoPath, state)
		}
		if err != nil {
			return result, err
		}
	}

	if stateResult.StateHash == "" {
		result.LastCommit = state.LastCommit
		return result, nil
	}
	if stateResult.StateHash == state.LastStateTree && len(stateResult.Txs) == 0 && stateResult.HeadHash == state.LastCommit {
		result.LastCommit = state.LastCommit
		return result, nil
	}

	storeTx, err := s.store.Begin(ctx)
	if err != nil {
		return result, err
	}

	collections := make(map[string]struct{})
	if len(stateResult.Txs) > 0 {
		decoded, err := s.decodeTxs(stateResult.Txs)
		if err != nil {
			_ = storeTx.Rollback()
			return result, err
		}

		if err := s.applyTxs(ctx, storeTx, decoded, collections, &result); err != nil {
			_ = storeTx.Rollback()
			return result, err
		}
	}

	if stateResult.StateHash != state.LastStateTree || stateResult.HeadHash != state.LastCommit {
		if err := storeTx.SetState(ctx, State{LastCommit: stateResult.HeadHash, LastStateTree: stateResult.StateHash}); err != nil {
			_ = storeTx.Rollback()
			return result, err
		}
	}
	if err := storeTx.Commit(); err != nil {
		return result, err
	}

	result.Collections = len(collections)
	result.LastCommit = stateResult.HeadHash
	if stateResult.HeadHash != state.LastCommit {
		result.Commits = 1
	}
	return result, nil
}

type decodedTx struct {
	Tx    domain.Transaction
	Bytes []byte
}

func (s *SyncService) decodeTxs(blobs []CommitTx) ([]decodedTx, error) {
	decoded := make([]decodedTx, 0, len(blobs))
	for _, blob := range blobs {
		tx, err := s.decoder.Decode(blob.Bytes)
		if err != nil {
			return nil, err
		}
		if err := tx.Validate(); err != nil {
			return nil, err
		}
		decoded = append(decoded, decodedTx{Tx: tx, Bytes: blob.Bytes})
	}

	sort.Slice(decoded, func(i, j int) bool {
		if decoded[i].Tx.Timestamp == decoded[j].Tx.Timestamp {
			return decoded[i].Tx.TxID < decoded[j].Tx.TxID
		}
		return decoded[i].Tx.Timestamp < decoded[j].Tx.Timestamp
	})

	return decoded, nil
}

func (s *SyncService) applyTxs(ctx context.Context, storeTx StoreTx, txs []decodedTx, collections map[string]struct{}, result *SyncResult) error {
	for _, item := range txs {
		tx := item.Tx
		if _, err := storeTx.EnsureCollection(ctx, tx.Collection); err != nil {
			return err
		}
		collections[tx.Collection] = struct{}{}

		switch tx.Op {
		case domain.TxOpPut:
			payload, err := s.canonicalizer.Canonicalize(ctx, tx.Snapshot)
			if err != nil {
				return err
			}
			if err := storeTx.UpsertDoc(ctx, tx.Collection, s.newRecord(tx, item.Bytes, payload, false)); err != nil {
				return err
			}
			result.TxsApplied++
			result.DocsUpserted++
		case domain.TxOpPatch:
			payload, err := s.applyPatch(ctx, storeTx, tx)
			if err != nil {
				return err
			}
			if err := storeTx.UpsertDoc(ctx, tx.Collection, s.newRecord(tx, item.Bytes, payload, false)); err != nil {
				return err
			}
			result.TxsApplied++
			result.DocsUpserted++
		case domain.TxOpMerge:
			payload, err := s.applyMerge(ctx, storeTx, tx)
			if err != nil {
				return err
			}
			if err := storeTx.UpsertDoc(ctx, tx.Collection, s.newRecord(tx, item.Bytes, payload, false)); err != nil {
				return err
			}
			result.TxsApplied++
			result.DocsUpserted++
		case domain.TxOpDelete:
			if err := storeTx.UpsertDoc(ctx, tx.Collection, s.newRecord(tx, item.Bytes, nil, true)); err != nil {
				return err
			}
			result.TxsApplied++
			result.DocsDeleted++
		default:
			return domain.ErrInvalidOp
		}
	}
	return nil
}

func (s *SyncService) applyPatch(ctx context.Context, storeTx StoreTx, tx domain.Transaction) ([]byte, error) {
	if s.patcher == nil {
		return nil, ErrPatchUnsupported
	}
	record, found, err := storeTx.GetDoc(ctx, tx.Collection, tx.DocID)
	if err != nil {
		return nil, err
	}
	if !found || record.Deleted {
		return nil, ErrMissingDocument
	}
	updated, err := s.patcher.Apply(ctx, record.Payload, tx.Patch)
	if err != nil {
		return nil, err
	}
	return s.canonicalizer.Canonicalize(ctx, updated)
}

func (s *SyncService) applyMerge(ctx context.Context, storeTx StoreTx, tx domain.Transaction) ([]byte, error) {
	if len(tx.Snapshot) > 0 {
		return s.canonicalizer.Canonicalize(ctx, tx.Snapshot)
	}
	if s.patcher == nil {
		return nil, ErrPatchUnsupported
	}
	record, found, err := storeTx.GetDoc(ctx, tx.Collection, tx.DocID)
	if err != nil {
		return nil, err
	}
	if !found || record.Deleted {
		return nil, ErrMissingDocument
	}
	updated, err := s.patcher.Apply(ctx, record.Payload, tx.Patch)
	if err != nil {
		return nil, err
	}
	return s.canonicalizer.Canonicalize(ctx, updated)
}

func (s *SyncService) newRecord(tx domain.Transaction, txBytes []byte, payload []byte, deleted bool) DocRecord {
	return DocRecord{
		DocID:         tx.DocID,
		Payload:       payload,
		TxHash:        s.hasher.SumHex(txBytes),
		TxID:          tx.TxID,
		Op:            tx.Op.String(),
		SchemaVersion: tx.SchemaVersion,
		UpdatedAt:     tx.Timestamp,
		Deleted:       deleted,
	}
}

func (s *SyncService) ensureDeps() error {
	if s.store == nil || s.source == nil {
		return errors.New("missing dependencies")
	}
	if s.decoder == nil || s.canonicalizer == nil || s.hasher == nil {
		return errors.New("missing dependencies")
	}
	return nil
}
