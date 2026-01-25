package repo

import (
	"context"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type StatusService struct {
	store StatusStore
}

func NewStatusService(store StatusStore) *StatusService {
	return &StatusService{store: store}
}

func (s *StatusService) Status(ctx context.Context, path string) (domain.RepoStatus, error) {
	absPath, err := paths.NormalizeRepoPath(path)
	if err != nil {
		return domain.RepoStatus{}, err
	}

	status, err := s.store.LoadStatus(ctx, absPath)
	if err != nil {
		return domain.RepoStatus{}, err
	}

	status.Path = absPath
	return status, nil
}
