package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
)

func (s *Store) Clone(ctx context.Context, url, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := ensureClonePath(path); err != nil {
		return err
	}

	auth, err := authForURL(url)
	if err != nil {
		return err
	}

	_, err = git.PlainCloneContext(ctx, path, true, &git.CloneOptions{URL: url, Auth: auth})
	if err != nil {
		return fmt.Errorf("clone git repo: %w", err)
	}

	return nil
}

func ensureClonePath(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return fmt.Errorf("clone path already exists: %w", os.ErrExist)
		}
		return fmt.Errorf("clone path is a file: %w", os.ErrExist)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check clone path: %w", err)
	}

	parent := filepath.Dir(path)
	if parent != "" && parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create parent directory: %w", err)
		}
	}

	return nil
}
