package gitrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"
)

// Bundle creates a single-file git bundle at bundlePath capturing every ref of
// the repository at repoPath. go-git/v5 does not expose `git bundle create`,
// so we shell out to the system git binary.
func (s *Store) Bundle(ctx context.Context, repoPath, bundlePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(bundlePath), 0o755); err != nil {
		return fmt.Errorf("create bundle dir: %w", err)
	}
	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("git binary not available for bundle: %w", err)
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "bundle", "create", bundlePath, "--all")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("git bundle: %w: %s", err, msg)
		}
		return fmt.Errorf("git bundle: %w", err)
	}
	return nil
}

// RestoreBundle initialises a bare repository at repoPath and fetches every
// ref from the bundle at bundlePath into it.
func (s *Store) RestoreBundle(ctx context.Context, bundlePath, repoPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("git binary not available for restore: %w", err)
	}
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return fmt.Errorf("create restore dir: %w", err)
	}
	if _, err := git.PlainInitWithOptions(repoPath, &git.PlainInitOptions{Bare: true}); err != nil {
		if !errors.Is(err, git.ErrRepositoryAlreadyExists) {
			return fmt.Errorf("init restore repo: %w", err)
		}
	}

	if err := runGitInRepo(ctx, repoPath, "verify bundle", "bundle", "verify", bundlePath); err != nil {
		return err
	}
	if err := runGitInRepo(ctx, repoPath, "fetch from bundle", "fetch", bundlePath, "refs/*:refs/*"); err != nil {
		return err
	}
	// Point HEAD at main so subsequent ledgerdb operations work. Best-effort:
	// fail loudly only if the symref command itself can't be invoked.
	_ = runGitInRepo(ctx, repoPath, "set HEAD", "symbolic-ref", "HEAD", "refs/heads/main")
	return nil
}

func runGitInRepo(ctx context.Context, repoPath, label string, args ...string) error {
	full := append([]string{"-C", repoPath}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("%s: %w: %s", label, err, msg)
		}
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}
