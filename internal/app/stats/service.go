package stats

import (
	"context"
	"sort"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// Service computes a high-level operational summary for a ledger repository.
type Service struct {
	store   StatsStore
	decoder Decoder
}

// NewService builds a stats Service.
func NewService(store StatsStore, decoder Decoder) *Service {
	return &Service{store: store, decoder: decoder}
}

// Options controls scope of a Stats() call.
type Options struct {
	// Collection, when set, limits collection-level aggregation to that name.
	Collection string
	// SampleStreams caps how many streams per collection are inspected for chain depth (0 = no cap).
	SampleStreams int
}

// Stats produces a snapshot of repository metrics.
func (s *Service) Stats(ctx context.Context, repoPath string, opts Options) (Result, error) {
	absPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return Result{}, err
	}

	status, err := s.store.LoadStatus(ctx, absPath)
	if err != nil {
		return Result{}, err
	}

	disk, err := s.store.RepoDiskBytes(ctx, absPath)
	if err != nil {
		return Result{}, err
	}

	counts, err := s.store.CountObjects(ctx, absPath)
	if err != nil {
		return Result{}, err
	}

	collections, err := s.collectStats(ctx, absPath, opts)
	if err != nil {
		return Result{}, err
	}

	ahead, behind, hasRemote, err := s.replication(ctx, absPath)
	if err != nil {
		return Result{}, err
	}

	return Result{
		RepoPath:     absPath,
		Manifest:     status.Manifest,
		HasManifest:  status.HasManifest,
		HasHead:      status.HasHead,
		HeadHash:     status.HeadHash,
		DiskBytes:    disk,
		CountObjects: counts,
		Collections:  collections,
		Ahead:        ahead,
		Behind:       behind,
		HasRemote:    hasRemote,
	}, nil
}

func (s *Service) collectStats(ctx context.Context, repoPath string, opts Options) ([]CollectionStat, error) {
	streams, err := s.store.ListDocStreams(ctx, repoPath)
	if err != nil {
		return nil, err
	}

	filter := strings.TrimSpace(opts.Collection)
	type accum struct {
		streams        []string
		docCount       int
		lastTimestamp  int64
		chainTotal     int
		chainMax       int
		sampledStreams int
	}
	byCollection := make(map[string]*accum)

	for _, streamPath := range streams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		collection := collectionFromStreamPath(streamPath)
		if collection == "" {
			continue
		}
		if filter != "" && collection != filter {
			continue
		}
		bucket, ok := byCollection[collection]
		if !ok {
			bucket = &accum{}
			byCollection[collection] = bucket
		}
		bucket.streams = append(bucket.streams, streamPath)
		bucket.docCount++
	}

	for _, bucket := range byCollection {
		for i, streamPath := range bucket.streams {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if opts.SampleStreams > 0 && i >= opts.SampleStreams {
				break
			}
			depth, ts, err := s.streamChainDepth(ctx, repoPath, streamPath)
			if err != nil {
				return nil, err
			}
			if depth == 0 {
				continue
			}
			bucket.sampledStreams++
			bucket.chainTotal += depth
			if depth > bucket.chainMax {
				bucket.chainMax = depth
			}
			if ts > bucket.lastTimestamp {
				bucket.lastTimestamp = ts
			}
		}
	}

	out := make([]CollectionStat, 0, len(byCollection))
	for name, bucket := range byCollection {
		stat := CollectionStat{
			Name:           name,
			DocCount:       bucket.docCount,
			LastTimestamp:  bucket.lastTimestamp,
			MaxChainDepth:  bucket.chainMax,
			SampledStreams: bucket.sampledStreams,
		}
		if bucket.sampledStreams > 0 {
			stat.AvgChainDepth = float64(bucket.chainTotal) / float64(bucket.sampledStreams)
		}
		out = append(out, stat)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Service) streamChainDepth(ctx context.Context, repoPath, streamPath string) (int, int64, error) {
	blobs, err := s.store.LoadStreamTxs(ctx, repoPath, streamPath)
	if err != nil {
		return 0, 0, err
	}
	if len(blobs) == 0 {
		return 0, 0, nil
	}
	var maxTimestamp int64
	for _, blob := range blobs {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		tx, decodeErr := s.decoder.Decode(blob.Bytes)
		if decodeErr != nil {
			// Skip undecodable blobs but keep them in the depth tally.
			continue
		}
		if tx.Timestamp > maxTimestamp {
			maxTimestamp = tx.Timestamp
		}
	}
	return len(blobs), maxTimestamp, nil
}

func (s *Service) replication(ctx context.Context, repoPath string) (int, int, bool, error) {
	// AheadBehind reports zero counts both when the remote is missing and when
	// branches match; we surface HasRemote=true unconditionally and let the
	// renderer present zeros.
	ahead, behind, err := s.store.AheadBehind(ctx, repoPath, "refs/heads/main", "refs/remotes/origin/main")
	if err != nil {
		return 0, 0, false, err
	}
	return ahead, behind, true, nil
}

func collectionFromStreamPath(streamPath string) string {
	parts := strings.Split(streamPath, "/")
	if len(parts) < 3 {
		return ""
	}
	if parts[0] != domain.DocumentsRoot {
		return ""
	}
	return parts[1]
}
