package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
)

type Store struct {
	options StoreOptions
}

type StoreOptions struct {
	SignCommits bool
	SignKey     string
	HistoryMode domain.HistoryMode
}

func NewStore() *Store {
	return &Store{}
}

func NewStoreWithOptions(options StoreOptions) *Store {
	return &Store{options: options}
}

func (s *Store) Init(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create repo dir: %w", err)
	}

	_, err := git.PlainInitWithOptions(path, &git.PlainInitOptions{Bare: true})
	if err != nil {
		if errors.Is(err, git.ErrRepositoryAlreadyExists) {
			return fmt.Errorf("repository already exists: %w", err)
		}
		return fmt.Errorf("init git repo: %w", err)
	}

	return nil
}

func (s *Store) WriteManifest(ctx context.Context, path string, manifest domain.Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	manifestPath := filepath.Join(path, "db.yaml")
	payload := renderManifest(manifest)
	if err := os.WriteFile(manifestPath, []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}
