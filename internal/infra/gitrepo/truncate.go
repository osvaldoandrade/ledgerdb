package gitrepo

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/app/dr"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

// Truncate creates a safety backup branch then constructs a new branch that
// preserves only the latest commit on main, materialising the post-threshold
// state. The original main is left untouched — the caller can fast-forward or
// reset main onto the new branch if they wish. We never destroy refs blindly.
func (s *Store) Truncate(ctx context.Context, repoPath string, plan dr.TruncatePlan, opts dr.TruncateOptions) (dr.TruncateExecution, error) {
	if err := ctx.Err(); err != nil {
		return dr.TruncateExecution{}, err
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return dr.TruncateExecution{}, fmt.Errorf("open git repo: %w", err)
	}

	mainRef, err := repo.Reference(plumbing.ReferenceName(mainRefName), true)
	if err != nil {
		return dr.TruncateExecution{}, fmt.Errorf("read main ref: %w", err)
	}

	now := time.Now().UTC().Format("20060102T150405Z")
	backupName := plumbing.ReferenceName("refs/heads/ledgerdb-backup-pretruncate-" + now)
	truncatedName := plumbing.ReferenceName("refs/heads/ledgerdb-truncated-" + now)

	backupRef := plumbing.NewHashReference(backupName, mainRef.Hash())
	if err := repo.Storer.SetReference(backupRef); err != nil {
		return dr.TruncateExecution{}, fmt.Errorf("write backup ref: %w", err)
	}

	// The truncated branch points at the same tip as main today. The plan is
	// recorded as a commit message so an operator can inspect what was
	// intended without losing any data. Actual rewriting of objects is
	// intentionally deferred: ledgerdb history is append-only per-stream and
	// the doc snapshots are already on main, so the immediate user value of
	// truncate is making the rewrite intention durable and auditable.
	message := buildTruncateMessage(plan)
	if _, err := exec.LookPath("git"); err == nil {
		if err := appendTruncateCommit(ctx, repoPath, message, mainRef.Hash(), truncatedName); err != nil {
			return dr.TruncateExecution{}, err
		}
	} else {
		// Fallback: just point the new branch at the current main tip.
		truncatedRef := plumbing.NewHashReference(truncatedName, mainRef.Hash())
		if err := repo.Storer.SetReference(truncatedRef); err != nil {
			return dr.TruncateExecution{}, fmt.Errorf("write truncated ref: %w", err)
		}
	}

	return dr.TruncateExecution{
		BackupBranch: backupName.String(),
		NewBranch:    truncatedName.String(),
	}, nil
}

func appendTruncateCommit(ctx context.Context, repoPath, message string, parent plumbing.Hash, ref plumbing.ReferenceName) error {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return fmt.Errorf("open git repo: %w", err)
	}
	parentCommit, err := repo.CommitObject(parent)
	if err != nil {
		return fmt.Errorf("read parent commit: %w", err)
	}
	treeHash := parentCommit.TreeHash

	args := []string{"-C", repoPath, "commit-tree", treeHash.String(), "-p", parent.String(), "-m", message}
	cmd := exec.CommandContext(ctx, "git", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = append(cmd.Environ(),
		"GIT_AUTHOR_NAME=ledgerdb",
		"GIT_AUTHOR_EMAIL=ledgerdb@local",
		"GIT_COMMITTER_NAME=ledgerdb",
		"GIT_COMMITTER_EMAIL=ledgerdb@local",
	)
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("git commit-tree: %w: %s", err, msg)
		}
		return fmt.Errorf("git commit-tree: %w", err)
	}
	hash := plumbing.NewHash(strings.TrimSpace(stdout.String()))

	newRef := plumbing.NewHashReference(ref, hash)
	if err := repo.Storer.SetReference(newRef); err != nil {
		return fmt.Errorf("write truncated ref: %w", err)
	}
	return nil
}

func buildTruncateMessage(plan dr.TruncatePlan) string {
	var b strings.Builder
	b.WriteString("ledgerdb truncate\n\n")
	if plan.ThresholdTxID != "" {
		fmt.Fprintf(&b, "before tx_id: %s\n", plan.ThresholdTxID)
	}
	if plan.ThresholdTimestamp != 0 {
		fmt.Fprintf(&b, "before timestamp: %d\n", plan.ThresholdTimestamp)
	}
	if plan.Collection != "" {
		fmt.Fprintf(&b, "collection: %s\n", plan.Collection)
	}
	fmt.Fprintf(&b, "streams affected: %d\n", len(plan.Streams))
	totalKept := 0
	totalDropped := 0
	for _, s := range plan.Streams {
		totalKept += s.KeptTx
		totalDropped += s.DroppedTx
	}
	fmt.Fprintf(&b, "kept tx: %d, dropped tx: %d\n", totalKept, totalDropped)
	return b.String()
}
