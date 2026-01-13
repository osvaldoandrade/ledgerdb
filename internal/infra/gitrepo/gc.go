package gitrepo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

func (s *Store) RunGC(ctx context.Context, repoPath, prune string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	args := []string{"-C", repoPath, "gc"}
	if prune != "" {
		args = append(args, "--prune="+prune)
	}

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Stdout = io.Discard

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("git gc: %w: %s", err, msg)
		}
		return fmt.Errorf("git gc: %w", err)
	}

	return nil
}
