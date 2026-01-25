package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func (s *Store) LoadStatus(ctx context.Context, path string) (domain.RepoStatus, error) {
	if err := ctx.Err(); err != nil {
		return domain.RepoStatus{}, err
	}

	repo, err := git.PlainOpen(path)
	if err != nil {
		return domain.RepoStatus{}, fmt.Errorf("open git repo: %w", err)
	}

	isBare := false
	if _, err := repo.Worktree(); err != nil {
		if errors.Is(err, git.ErrIsBareRepository) {
			isBare = true
		} else {
			return domain.RepoStatus{}, fmt.Errorf("open worktree: %w", err)
		}
	}

	status := domain.RepoStatus{IsBare: isBare}

	ref, err := repo.Head()
	if err == nil {
		status.HasHead = true
		status.HeadHash = ref.Hash().String()
	} else if !errors.Is(err, plumbing.ErrReferenceNotFound) && !errors.Is(err, plumbing.ErrObjectNotFound) {
		return domain.RepoStatus{}, fmt.Errorf("read HEAD: %w", err)
	}

	manifestPath := filepath.Join(path, "db.yaml")
	data, err := os.ReadFile(manifestPath)
	if err == nil {
		manifest, err := parseManifest(data)
		if err != nil {
			return domain.RepoStatus{}, err
		}
		status.HasManifest = true
		status.Manifest = manifest
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.RepoStatus{}, fmt.Errorf("read manifest: %w", err)
	}

	return status, nil
}
