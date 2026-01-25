package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
)

func (s *Store) Push(ctx context.Context, repoPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return fmt.Errorf("open git repo: %w", err)
	}

	remote, err := repo.Remote("origin")
	if err != nil {
		if errors.Is(err, git.ErrRemoteNotFound) {
			return nil
		}
		return fmt.Errorf("read git remote: %w", err)
	}
	remoteURL := ""
	if cfg := remote.Config(); cfg != nil && len(cfg.URLs) > 0 {
		remoteURL = cfg.URLs[0]
	}
	auth, err := authForURL(remoteURL)
	if err != nil {
		return err
	}

	err = repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []config.RefSpec{
			config.RefSpec("refs/heads/main:refs/heads/main"),
		},
		Auth: auth,
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		return nil
	}
	if isNonFastForward(err) {
		return domain.ErrSyncConflict
	}
	if isAuthFailure(err) {
		if fallbackErr := pushWithSystemGit(ctx, repoPath); fallbackErr == nil {
			return nil
		}
	}
	return fmt.Errorf("push git repo: %w", err)
}

func isNonFastForward(err error) bool {
	return err != nil && strings.Contains(err.Error(), "non-fast-forward update")
}

func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "authentication required") ||
		strings.Contains(msg, "repository not found") ||
		strings.Contains(msg, "authorization failed") ||
		strings.Contains(msg, "permission denied")
}

func pushWithSystemGit(ctx context.Context, repoPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "push", "-u", "origin", "main")
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git push: %w", err)
	}
	return nil
}
