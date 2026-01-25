package repo

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
)

type CloneService struct {
	cloner Cloner
}

func NewCloneService(cloner Cloner) *CloneService {
	return &CloneService{cloner: cloner}
}

func (s *CloneService) Clone(ctx context.Context, url, path string) error {
	url = strings.TrimSpace(url)
	if url == "" {
		return ErrRepoURLRequired
	}

	path = strings.TrimSpace(path)
	if path == "" {
		defaultDir, err := defaultCloneDir(url)
		if err != nil {
			return err
		}
		path = defaultDir
	}

	absPath, err := paths.NormalizeRepoPath(path)
	if err != nil {
		return err
	}

	return s.cloner.Clone(ctx, url, absPath)
}

func defaultCloneDir(url string) (string, error) {
	trimmed := strings.TrimSpace(url)
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "", ErrClonePathRequired
	}

	last := trimmed
	if idx := strings.LastIndexAny(trimmed, "/:"); idx >= 0 && idx < len(trimmed)-1 {
		last = trimmed[idx+1:]
	}

	last = strings.TrimSuffix(last, ".git")
	last = strings.TrimSpace(last)
	if last == "" {
		return "", ErrClonePathRequired
	}

	if filepath.Base(last) != last {
		return "", fmt.Errorf("invalid clone dir %q: %w", last, ErrClonePathRequired)
	}

	return last, nil
}
