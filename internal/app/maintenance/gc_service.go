package maintenance

import (
	"context"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
)

type GCService struct {
	executor GCExecutor
}

func NewGCService(executor GCExecutor) *GCService {
	return &GCService{executor: executor}
}

func (s *GCService) GC(ctx context.Context, repoPath string, opts GCOptions) error {
	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return err
	}

	prune := strings.TrimSpace(opts.Prune)
	return s.executor.RunGC(ctx, absRepoPath, prune)
}
