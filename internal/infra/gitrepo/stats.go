package gitrepo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	statsapp "github.com/osvaldoandrade/ledgerdb/internal/app/stats"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/utils/merkletrie"
)

// RepoDiskBytes returns the byte size of the on-disk git directory.
func (s *Store) RepoDiskBytes(ctx context.Context, repoPath string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	candidates := []string{
		filepath.Join(repoPath, ".git"),
		repoPath,
	}
	var total int64
	for _, root := range candidates {
		info, err := os.Stat(root)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, fmt.Errorf("stat %s: %w", root, err)
		}
		if !info.IsDir() {
			continue
		}
		size, err := dirSize(ctx, root)
		if err != nil {
			return 0, err
		}
		total = size
		break
	}
	return total, nil
}

func dirSize(ctx context.Context, root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk %s: %w", root, err)
	}
	return size, nil
}

// CountObjects shells out to `git count-objects -v` and parses the relevant fields.
func (s *Store) CountObjects(ctx context.Context, repoPath string) (statsapp.CountObjects, error) {
	if err := ctx.Err(); err != nil {
		return statsapp.CountObjects{}, err
	}

	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "count-objects", "-v")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return statsapp.CountObjects{}, fmt.Errorf("git count-objects: %w: %s", err, msg)
		}
		return statsapp.CountObjects{}, fmt.Errorf("git count-objects: %w", err)
	}

	out := statsapp.CountObjects{}
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "count":
			out.LooseObjects = parseInt64(value)
		case "size":
			out.SizeBytes = parseInt64(value) * 1024
		case "in-pack":
			// ignored: total object count inside packs.
		case "packs":
			out.PackCount = parseInt64(value)
		case "size-pack":
			out.SizePackKB = parseInt64(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return statsapp.CountObjects{}, fmt.Errorf("parse count-objects: %w", err)
	}
	return out, nil
}

func parseInt64(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

// AheadBehind returns how many commits localRef has that remoteRef does not, and vice versa.
// When either ref is missing this returns zeros with a nil error.
func (s *Store) AheadBehind(ctx context.Context, repoPath, localRef, remoteRef string) (int, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return 0, 0, fmt.Errorf("open git repo: %w", err)
	}

	localHash, err := repo.ResolveRevision(plumbing.Revision(localRef))
	if err != nil {
		if isMissingRef(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("resolve %s: %w", localRef, err)
	}
	remoteHash, err := repo.ResolveRevision(plumbing.Revision(remoteRef))
	if err != nil {
		if isMissingRef(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("resolve %s: %w", remoteRef, err)
	}

	localCommit, err := repo.CommitObject(*localHash)
	if err != nil {
		return 0, 0, fmt.Errorf("read local commit: %w", err)
	}
	remoteCommit, err := repo.CommitObject(*remoteHash)
	if err != nil {
		return 0, 0, fmt.Errorf("read remote commit: %w", err)
	}

	ahead, err := countCommitsExcluding(ctx, localCommit, remoteCommit)
	if err != nil {
		return 0, 0, err
	}
	behind, err := countCommitsExcluding(ctx, remoteCommit, localCommit)
	if err != nil {
		return 0, 0, err
	}
	return ahead, behind, nil
}

func countCommitsExcluding(ctx context.Context, from, exclude *object.Commit) (int, error) {
	excluded := make(map[plumbing.Hash]struct{})
	if exclude != nil {
		iter := object.NewCommitPreorderIter(exclude, nil, nil)
		err := iter.ForEach(func(c *object.Commit) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			excluded[c.Hash] = struct{}{}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("walk exclude: %w", err)
		}
	}

	count := 0
	iter := object.NewCommitPreorderIter(from, nil, nil)
	err := iter.ForEach(func(c *object.Commit) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, skip := excluded[c.Hash]; skip {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk from: %w", err)
	}
	return count, nil
}

func isMissingRef(err error) bool {
	return errors.Is(err, plumbing.ErrReferenceNotFound) ||
		errors.Is(err, plumbing.ErrObjectNotFound) ||
		errors.Is(err, git.ErrRemoteNotFound)
}

// ResolveRefHash converts a user-supplied ref (HEAD, refs/heads/main, branch name, sha) into a commit hash.
func (s *Store) ResolveRefHash(ctx context.Context, repoPath, ref string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return "", fmt.Errorf("open git repo: %w", err)
	}
	resolved, err := repo.ResolveRevision(plumbing.Revision(ref))
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", ref, err)
	}
	return resolved.String(), nil
}

// CommitsInRange returns commits reachable from `to` but not from `from`. When
// `from` is empty, all ancestors of `to` are returned. Results are ordered
// newest-first.
func (s *Store) CommitsInRange(ctx context.Context, repoPath, from, to string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("open git repo: %w", err)
	}

	excluded := make(map[plumbing.Hash]struct{})
	if from != "" {
		fromCommit, err := repo.CommitObject(plumbing.NewHash(from))
		if err != nil {
			return nil, fmt.Errorf("read from commit: %w", err)
		}
		iter := object.NewCommitPreorderIter(fromCommit, nil, nil)
		err = iter.ForEach(func(c *object.Commit) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			excluded[c.Hash] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk from: %w", err)
		}
	}

	toCommit, err := repo.CommitObject(plumbing.NewHash(to))
	if err != nil {
		return nil, fmt.Errorf("read to commit: %w", err)
	}
	iter := object.NewCommitPreorderIter(toCommit, nil, nil)
	var commits []string
	err = iter.ForEach(func(c *object.Commit) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, skip := excluded[c.Hash]; skip {
			return nil
		}
		commits = append(commits, c.Hash.String())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk to: %w", err)
	}
	return commits, nil
}

// CollectCommitTxs decodes every tx blob touched (added/modified) by `commitHash`
// into a TxRecord, using the txv3 decoder bundled in this package.
func (s *Store) CollectCommitTxs(ctx context.Context, repoPath, commitHash string) ([]statsapp.TxRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("open git repo: %w", err)
	}

	commit, err := repo.CommitObject(plumbing.NewHash(commitHash))
	if err != nil {
		return nil, fmt.Errorf("read commit: %w", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("read commit tree: %w", err)
	}

	if commit.NumParents() == 0 {
		return collectTxsFromTree(ctx, tree)
	}
	if commit.NumParents() > 1 {
		// For merge commits, treat as new state across all files reachable from this tree.
		return collectTxsFromTree(ctx, tree)
	}

	parent, err := commit.Parent(0)
	if err != nil {
		return nil, fmt.Errorf("read commit parent: %w", err)
	}
	parentTree, err := parent.Tree()
	if err != nil {
		return nil, fmt.Errorf("read parent tree: %w", err)
	}

	changes, err := object.DiffTreeContext(ctx, parentTree, tree)
	if err != nil {
		return nil, fmt.Errorf("diff trees: %w", err)
	}

	var records []statsapp.TxRecord
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		action, err := change.Action()
		if err != nil {
			return nil, fmt.Errorf("read change action: %w", err)
		}
		if action == merkletrie.Delete {
			continue
		}
		filePath := change.To.Name
		if !isTxPath(filePath) {
			continue
		}
		data, err := readTreeFile(tree, filePath)
		if err != nil {
			return nil, err
		}
		rec, err := decodeTxRecord(filePath, data)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func collectTxsFromTree(ctx context.Context, tree *object.Tree) ([]statsapp.TxRecord, error) {
	var records []statsapp.TxRecord
	iter := tree.Files()
	err := iter.ForEach(func(file *object.File) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isTxPath(file.Name) {
			return nil
		}
		reader, err := file.Reader()
		if err != nil {
			return fmt.Errorf("read tx file %s: %w", file.Name, err)
		}
		defer func() {
			_ = reader.Close()
		}()
		data, err := io.ReadAll(reader)
		if err != nil {
			return fmt.Errorf("read tx file %s: %w", file.Name, err)
		}
		rec, err := decodeTxRecord(file.Name, data)
		if err != nil {
			return err
		}
		records = append(records, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// decodeTxRecord wraps the package-local txv3 decoder + sha256 hash so the
// stats layer doesn't need to know the on-disk format.
func decodeTxRecord(filePath string, data []byte) (statsapp.TxRecord, error) {
	tx, err := txv3.Decode(data)
	if err != nil {
		return statsapp.TxRecord{}, fmt.Errorf("decode tx %s: %w", filePath, err)
	}
	collection := tx.Collection
	if collection == "" {
		collection = collectionFromTxPath(filePath)
	}
	return statsapp.TxRecord{
		Collection: collection,
		DocID:      tx.DocID,
		Op:         tx.Op,
		TxHash:     hashBytes(data),
		Timestamp:  tx.Timestamp,
	}, nil
}

func collectionFromTxPath(filePath string) string {
	parts := strings.Split(filePath, "/")
	if len(parts) < 3 {
		return ""
	}
	if parts[0] != domain.DocumentsRoot && parts[0] != domain.StateRoot {
		return ""
	}
	return parts[1]
}

