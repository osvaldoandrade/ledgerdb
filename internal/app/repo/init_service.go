package repo

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type InitService struct {
	store Store
	clock Clock
}

type InitOptions struct {
	Name         string
	StreamLayout domain.StreamLayout
	HistoryMode  domain.HistoryMode
	RemoteURL    string
}

func NewInitService(store Store, clock Clock) *InitService {
	return &InitService{
		store: store,
		clock: clock,
	}
}

func (s *InitService) Init(ctx context.Context, path string, opts InitOptions) error {
	absPath, err := paths.NormalizeRepoPath(path)
	if err != nil {
		return err
	}

	manifestName := strings.TrimSpace(opts.Name)
	if manifestName == "" {
		manifestName = filepath.Base(absPath)
	}

	if err := s.store.Init(ctx, absPath); err != nil {
		return err
	}

	manifest := domain.NewManifest(manifestName, s.clock.Now())
	if opts.StreamLayout != "" {
		manifest.StreamLayout = opts.StreamLayout
	}
	if opts.HistoryMode != "" {
		manifest.HistoryMode = opts.HistoryMode
	}
	manifest = manifest.WithDefaults()
	if err := s.store.WriteManifest(ctx, absPath, manifest); err != nil {
		return err
	}

	if strings.TrimSpace(opts.RemoteURL) != "" {
		if err := s.store.SetRemote(ctx, absPath, "origin", opts.RemoteURL); err != nil {
			return err
		}
	}

	return nil
}
