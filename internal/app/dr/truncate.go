package dr

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// TruncateStreamLister enumerates document streams to consider.
type TruncateStreamLister interface {
	ListDocStreams(ctx context.Context, repoPath string) ([]string, error)
}

// TruncateReadStore reads transactions to build per-stream truncation plans.
type TruncateReadStore interface {
	LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error)
	LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error)
}

// TruncateDecoder decodes raw tx bytes into domain transactions.
type TruncateDecoder interface {
	Decode(data []byte) (domain.Transaction, error)
}

// TruncateHasher hashes raw tx bytes (the canonical tx hash).
type TruncateHasher interface {
	SumHex(data []byte) string
}

// TruncateService plans and (optionally) executes history truncation.
// Truncation drops transactions older than the threshold, but keeps the latest
// snapshot per document so document state is preserved. The destructive
// rewrite is delegated to a TruncateExecutor; in the absence of one the
// service can still produce a dry-run plan.
type TruncateService struct {
	lister   TruncateStreamLister
	store    TruncateReadStore
	decoder  TruncateDecoder
	hasher   TruncateHasher
	executor TruncateExecutor
	layout   domain.StreamLayout
}

// NewTruncateService constructs a TruncateService.
func NewTruncateService(
	lister TruncateStreamLister,
	store TruncateReadStore,
	decoder TruncateDecoder,
	hasher TruncateHasher,
	executor TruncateExecutor,
	layout domain.StreamLayout,
) *TruncateService {
	return &TruncateService{
		lister:   lister,
		store:    store,
		decoder:  decoder,
		hasher:   hasher,
		executor: executor,
		layout:   domain.NormalizeStreamLayout(layout),
	}
}

// Truncate evaluates the request and (when --yes is set and not --dry-run)
// invokes the executor to rewrite git history.
func (s *TruncateService) Truncate(ctx context.Context, repoPath string, opts TruncateOptions) (TruncateResult, error) {
	if strings.TrimSpace(opts.Before) == "" {
		return TruncateResult{}, ErrTruncateThresholdRequired
	}
	if !opts.DryRun && !opts.Yes {
		return TruncateResult{}, ErrTruncateConfirmation
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return TruncateResult{}, err
	}

	plan, err := s.buildPlan(ctx, absRepoPath, opts)
	if err != nil {
		return TruncateResult{}, err
	}

	result := TruncateResult{Plan: plan, DryRun: opts.DryRun}
	if opts.DryRun {
		return result, nil
	}
	if s.executor == nil {
		return result, fmt.Errorf("truncate executor not configured")
	}
	exec, err := s.executor.Truncate(ctx, absRepoPath, plan, opts)
	if err != nil {
		return result, err
	}
	result.Execution = &exec
	return result, nil
}

func (s *TruncateService) buildPlan(ctx context.Context, repoPath string, opts TruncateOptions) (TruncatePlan, error) {
	thresholdTs, thresholdTxID, err := parseThreshold(opts.Before)
	if err != nil {
		return TruncatePlan{}, err
	}

	streams, err := s.lister.ListDocStreams(ctx, repoPath)
	if err != nil {
		return TruncatePlan{}, err
	}

	collection := strings.TrimSpace(opts.Collection)
	collectionPrefix := ""
	if collection != "" {
		if !domain.IsValidCollectionName(collection) {
			return TruncatePlan{}, fmt.Errorf("%w: invalid collection name", ErrInvalidThreshold)
		}
		collectionPrefix = path.Join(domain.DocumentsRoot, collection) + "/"
	}

	plan := TruncatePlan{
		ThresholdTimestamp: thresholdTs,
		ThresholdTxID:      thresholdTxID,
		Collection:         collection,
	}

	for _, streamPath := range streams {
		if err := ctx.Err(); err != nil {
			return TruncatePlan{}, err
		}
		if collectionPrefix != "" && !strings.HasPrefix(streamPath+"/", collectionPrefix) {
			continue
		}
		streamPlan, err := s.planStream(ctx, repoPath, streamPath, thresholdTs, thresholdTxID)
		if err != nil {
			return TruncatePlan{}, err
		}
		if streamPlan.DroppedTx == 0 {
			continue
		}
		plan.Streams = append(plan.Streams, streamPlan)
	}

	sort.Slice(plan.Streams, func(i, j int) bool {
		return plan.Streams[i].StreamPath < plan.Streams[j].StreamPath
	})
	return plan, nil
}

func (s *TruncateService) planStream(ctx context.Context, repoPath, streamPath string, thresholdTs int64, thresholdTxID string) (TruncateStreamPlan, error) {
	head, err := s.store.LoadStreamHead(ctx, repoPath, streamPath)
	if err != nil {
		return TruncateStreamPlan{}, err
	}
	if head == "" {
		return TruncateStreamPlan{StreamPath: streamPath}, nil
	}

	txBlobs, err := s.store.LoadStreamTxs(ctx, repoPath, streamPath)
	if err != nil {
		return TruncateStreamPlan{}, err
	}

	type entry struct {
		hash string
		tx   domain.Transaction
	}
	index := make(map[string]entry, len(txBlobs))
	for _, blob := range txBlobs {
		tx, err := s.decoder.Decode(blob.Bytes)
		if err != nil {
			return TruncateStreamPlan{}, fmt.Errorf("decode tx in %s: %w", streamPath, err)
		}
		hash := s.hasher.SumHex(blob.Bytes)
		index[hash] = entry{hash: hash, tx: tx}
	}

	// Walk the chain from HEAD back to the root.
	var chain []entry
	current := head
	visited := map[string]struct{}{}
	for current != "" {
		if _, seen := visited[current]; seen {
			return TruncateStreamPlan{}, fmt.Errorf("cycle in %s at %s", streamPath, current)
		}
		visited[current] = struct{}{}
		e, ok := index[current]
		if !ok {
			return TruncateStreamPlan{}, fmt.Errorf("missing tx %s in %s", current, streamPath)
		}
		chain = append(chain, e)
		current = e.tx.ParentHash
	}

	// chain is newest-first; reverse to oldest-first to make threshold logic
	// easier to read.
	oldestFirst := make([]entry, len(chain))
	for i := range chain {
		oldestFirst[len(chain)-1-i] = chain[i]
	}

	keepStart := -1
	for i, e := range oldestFirst {
		if isAtOrAfterThreshold(e.tx, thresholdTs, thresholdTxID) {
			keepStart = i
			break
		}
	}
	if keepStart < 0 {
		// every tx is older than threshold — keep only the latest snapshot.
		keepStart = len(oldestFirst) - 1
	}
	if keepStart <= 0 {
		// nothing to drop.
		return TruncateStreamPlan{StreamPath: streamPath, KeptTx: len(oldestFirst)}, nil
	}

	dropped := keepStart
	kept := len(oldestFirst) - dropped
	plan := TruncateStreamPlan{
		StreamPath: streamPath,
		KeptTx:     kept,
		DroppedTx:  dropped,
	}
	// The synthesised base must be a self-contained snapshot. Use the hash of
	// the kept first tx if it is already a snapshot, otherwise leave it empty
	// for the executor to materialise.
	if oldestFirst[keepStart].tx.Op == domain.TxOpPut {
		plan.NewBaseHash = oldestFirst[keepStart].hash
	}
	return plan, nil
}

func parseThreshold(before string) (int64, string, error) {
	before = strings.TrimSpace(before)
	if before == "" {
		return 0, "", ErrTruncateThresholdRequired
	}

	if ts, err := strconv.ParseInt(before, 10, 64); err == nil && ts > 0 {
		// Accept unix nanos or seconds — anything below year 2200 in nanos
		// (~7.25e18) is fine.
		return ts, "", nil
	}

	if t, err := time.Parse(time.RFC3339Nano, before); err == nil {
		return t.UTC().UnixNano(), "", nil
	}
	if t, err := time.Parse(time.RFC3339, before); err == nil {
		return t.UTC().UnixNano(), "", nil
	}
	if t, err := time.Parse("2006-01-02", before); err == nil {
		return t.UTC().UnixNano(), "", nil
	}

	// Fall through: treat as a tx id.
	if looksLikeULID(before) {
		return 0, before, nil
	}
	return 0, "", fmt.Errorf("%w: %q", ErrInvalidThreshold, before)
}

func looksLikeULID(value string) bool {
	if len(value) != 26 {
		return false
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z')) {
			return false
		}
	}
	return true
}

func isAtOrAfterThreshold(tx domain.Transaction, thresholdTs int64, thresholdTxID string) bool {
	if thresholdTxID != "" {
		return tx.TxID >= thresholdTxID
	}
	return tx.Timestamp >= thresholdTs
}
