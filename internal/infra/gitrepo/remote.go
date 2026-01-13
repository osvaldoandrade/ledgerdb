package gitrepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
)

func (s *Store) SetRemote(ctx context.Context, repoPath, name, url string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		name = "origin"
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return nil
	}
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		if !strings.HasSuffix(url, ".git") {
			url += ".git"
		}
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return fmt.Errorf("open git repo: %w", err)
	}

	cfg, err := repo.Config()
	if err != nil {
		return fmt.Errorf("read git config: %w", err)
	}

	if existing, ok := cfg.Remotes[name]; ok {
		existing.URLs = []string{url}
		cfg.Remotes[name] = existing
	} else {
		cfg.Remotes[name] = &config.RemoteConfig{
			Name: name,
			URLs: []string{url},
		}
	}

	if err := repo.SetConfig(cfg); err != nil {
		return fmt.Errorf("write git config: %w", err)
	}
	return nil
}
