package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	indexapp "github.com/codecompany/ledgerdb/internal/app/index"
	"github.com/codecompany/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/utils/merkletrie"
)

func (s *Store) Fetch(ctx context.Context, repoPath string) error {
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

	err = repo.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin",
		RefSpecs: []config.RefSpec{
			"+refs/heads/*:refs/heads/*",
		},
		Auth: auth,
	})
	if err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			return nil
		}
		return fmt.Errorf("fetch git repo: %w", err)
	}
	return nil
}

func (s *Store) ListCommitHashes(ctx context.Context, repoPath, sinceHash string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("open git repo: %w", err)
	}

	ref, err := repo.Reference(plumbing.ReferenceName(mainRefName), true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read main ref: %w", err)
	}

	iter, err := repo.Log(&git.LogOptions{From: ref.Hash()})
	if err != nil {
		return nil, fmt.Errorf("read git log: %w", err)
	}
	defer iter.Close()

	var commits []string
	found := sinceHash == ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		commit, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read git log: %w", err)
		}
		if sinceHash != "" && commit.Hash.String() == sinceHash {
			found = true
			break
		}
		commits = append(commits, commit.Hash.String())
	}

	if !found {
		return nil, indexapp.ErrCommitNotFound
	}

	reverseStrings(commits)
	return commits, nil
}

func (s *Store) CommitTxs(ctx context.Context, repoPath, commitHash string) ([]indexapp.CommitTx, error) {
	return s.commitTxsForRoot(ctx, repoPath, commitHash, domain.DocumentsRoot)
}

func (s *Store) CommitStateTxs(ctx context.Context, repoPath, commitHash string) ([]indexapp.CommitTx, error) {
	return s.commitTxsForRoot(ctx, repoPath, commitHash, domain.StateRoot)
}

func (s *Store) StateTxsSince(ctx context.Context, repoPath string, state indexapp.State) (indexapp.StateTxsResult, error) {
	if err := ctx.Err(); err != nil {
		return indexapp.StateTxsResult{}, err
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return indexapp.StateTxsResult{}, fmt.Errorf("open git repo: %w", err)
	}

	ref, err := repo.Reference(plumbing.ReferenceName(mainRefName), true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return indexapp.StateTxsResult{}, nil
		}
		return indexapp.StateTxsResult{}, fmt.Errorf("read main ref: %w", err)
	}

	headCommit, err := repo.CommitObject(ref.Hash())
	if err != nil {
		return indexapp.StateTxsResult{}, fmt.Errorf("read main commit: %w", err)
	}
	headTree, err := headCommit.Tree()
	if err != nil {
		return indexapp.StateTxsResult{}, fmt.Errorf("read main tree: %w", err)
	}

	headStateTree, err := headTree.Tree(domain.StateRoot)
	if err != nil {
		if errors.Is(err, object.ErrDirectoryNotFound) {
			return indexapp.StateTxsResult{}, indexapp.ErrStateUnavailable
		}
		return indexapp.StateTxsResult{}, fmt.Errorf("read state tree: %w", err)
	}

	result := indexapp.StateTxsResult{
		HeadHash:  headCommit.Hash.String(),
		StateHash: headStateTree.Hash.String(),
	}
	if state.LastCommit == "" && state.LastStateTree == "" {
		txs, err := listAllTxsInTree(ctx, headStateTree, domain.StateRoot)
		if err != nil {
			return indexapp.StateTxsResult{}, err
		}
		result.Txs = txs
		return result, nil
	}
	if state.LastStateTree != "" && state.LastStateTree == result.StateHash {
		return result, nil
	}

	var sinceStateTree *object.Tree
	if state.LastStateTree != "" {
		sinceStateTree, err = repo.TreeObject(plumbing.NewHash(state.LastStateTree))
		if err != nil {
			if !errors.Is(err, plumbing.ErrObjectNotFound) {
				return indexapp.StateTxsResult{}, fmt.Errorf("read state tree: %w", err)
			}
			sinceStateTree = nil
		}
	}

	if sinceStateTree == nil && state.LastCommit != "" {
		sinceCommit, err := repo.CommitObject(plumbing.NewHash(state.LastCommit))
		if err != nil {
			if errors.Is(err, plumbing.ErrObjectNotFound) {
				return indexapp.StateTxsResult{}, indexapp.ErrCommitNotFound
			}
			return indexapp.StateTxsResult{}, fmt.Errorf("read commit: %w", err)
		}

		sinceTree, err := sinceCommit.Tree()
		if err != nil {
			return indexapp.StateTxsResult{}, fmt.Errorf("read commit tree: %w", err)
		}
		sinceStateTree, err = sinceTree.Tree(domain.StateRoot)
		if err != nil {
			if errors.Is(err, object.ErrDirectoryNotFound) {
				txs, err := listAllTxsInTree(ctx, headStateTree, domain.StateRoot)
				if err != nil {
					return indexapp.StateTxsResult{}, err
				}
				result.Txs = txs
				return result, nil
			}
			return indexapp.StateTxsResult{}, fmt.Errorf("read state tree: %w", err)
		}
	}

	if sinceStateTree == nil {
		txs, err := listAllTxsInTree(ctx, headStateTree, domain.StateRoot)
		if err != nil {
			return indexapp.StateTxsResult{}, err
		}
		result.Txs = txs
		return result, nil
	}

	changes, err := object.DiffTreeContext(ctx, sinceStateTree, headStateTree)
	if err != nil {
		return indexapp.StateTxsResult{}, fmt.Errorf("diff state trees: %w", err)
	}

	var txs []indexapp.CommitTx
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return indexapp.StateTxsResult{}, err
		}
		action, err := change.Action()
		if err != nil {
			return indexapp.StateTxsResult{}, fmt.Errorf("read change action: %w", err)
		}
		if action == merkletrie.Delete {
			continue
		}
		if change.To.Name == "" || !isTxPath(change.To.Name) {
			continue
		}
		txBytes, err := readTreeFile(headStateTree, change.To.Name)
		if err != nil {
			return indexapp.StateTxsResult{}, err
		}
		txs = append(txs, indexapp.CommitTx{
			Path:  path.Join(domain.StateRoot, change.To.Name),
			Bytes: txBytes,
		})
	}

	result.Txs = txs
	return result, nil
}

func (s *Store) commitTxsForRoot(ctx context.Context, repoPath, commitHash, root string) ([]indexapp.CommitTx, error) {
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
	if commit.NumParents() > 1 {
		return nil, indexapp.ErrMergeCommitUnsupported
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("read commit tree: %w", err)
	}

	if commit.NumParents() == 0 {
		return listAllTxs(ctx, tree, root)
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

	var txs []indexapp.CommitTx
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
		path := change.To.Name
		if !isTxPathForRoot(path, root) {
			continue
		}
		txBytes, err := readTreeFile(tree, path)
		if err != nil {
			return nil, err
		}
		txs = append(txs, indexapp.CommitTx{Path: path, Bytes: txBytes})
	}

	return txs, nil
}

func listAllTxs(ctx context.Context, tree *object.Tree, root string) ([]indexapp.CommitTx, error) {
	iter := tree.Files()
	var txs []indexapp.CommitTx
	err := iter.ForEach(func(file *object.File) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !isTxPathForRoot(file.Name, root) {
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
		txs = append(txs, indexapp.CommitTx{Path: file.Name, Bytes: data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return txs, nil
}

func listAllTxsInTree(ctx context.Context, tree *object.Tree, prefix string) ([]indexapp.CommitTx, error) {
	iter := tree.Files()
	var txs []indexapp.CommitTx
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
		txPath := file.Name
		if prefix != "" {
			txPath = path.Join(prefix, file.Name)
		}
		txs = append(txs, indexapp.CommitTx{Path: txPath, Bytes: data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return txs, nil
}

func isTxPath(filePath string) bool {
	return strings.Contains(filePath, "/"+domain.TxDirName+"/") && strings.HasSuffix(filePath, domain.TxFileExt)
}

func isTxPathForRoot(filePath, root string) bool {
	if !strings.HasPrefix(filePath, root+"/") {
		return false
	}
	return isTxPath(filePath)
}

func reverseStrings(items []string) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}
