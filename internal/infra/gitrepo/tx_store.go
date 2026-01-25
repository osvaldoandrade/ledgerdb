package gitrepo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"
)

const (
	mainRefName    = "refs/heads/main"
	casMaxRetries  = 5
	casBackoffBase = 25 * time.Millisecond
)

func (s *Store) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return "", fmt.Errorf("open git repo: %w", err)
	}

	ref, err := repo.Reference(plumbing.ReferenceName(mainRefName), true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("read main ref: %w", err)
	}

	commit, err := repo.CommitObject(ref.Hash())
	if err != nil {
		return "", fmt.Errorf("read main commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return "", fmt.Errorf("read main tree: %w", err)
	}

	streamPath = normalizeTreePath(streamPath)
	headPath := path.Join(streamPath, domain.StreamHeadFile)
	headContent, err := readTreeFile(tree, headPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return "", nil
		}
		return "", err
	}

	relPath := strings.TrimSpace(string(headContent))
	if relPath == "" {
		return "", nil
	}

	txPath := path.Join(streamPath, relPath)
	txBytes, err := readTreeFile(tree, txPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return "", fmt.Errorf("stream tx missing at %s", txPath)
		}
		return "", err
	}

	return hashBytes(txBytes), nil
}

func (s *Store) PutTx(ctx context.Context, write doc.TxWrite) (doc.PutResult, error) {
	if err := ctx.Err(); err != nil {
		return doc.PutResult{}, err
	}

	repo, err := git.PlainOpen(write.RepoPath)
	if err != nil {
		return doc.PutResult{}, fmt.Errorf("open git repo: %w", err)
	}

	streamPath := normalizeTreePath(write.StreamPath)
	txFileName := txFileName(write.Tx)
	if s.historyMode() == domain.HistoryModeAmend {
		txFileName = domain.TxCompactFile
	}
	relTxPath := path.Join(domain.TxDirName, txFileName)

	txBlobHash, err := writeBlob(repo.Storer, write.TxBytes)
	if err != nil {
		return doc.PutResult{}, err
	}

	headBlobHash, err := writeBlob(repo.Storer, []byte(relTxPath+"\n"))
	if err != nil {
		return doc.PutResult{}, err
	}

	statePath := ""
	var stateTxBlobHash plumbing.Hash
	var stateHeadBlobHash plumbing.Hash
	if write.StatePath != "" && len(write.StateTxBytes) > 0 {
		statePath = normalizeTreePath(write.StatePath)
		stateTxBlobHash, err = writeBlob(repo.Storer, write.StateTxBytes)
		if err != nil {
			return doc.PutResult{}, err
		}
		relStateTxPath := path.Join(domain.TxDirName, domain.TxCompactFile)
		stateHeadBlobHash, err = writeBlob(repo.Storer, []byte(relStateTxPath+"\n"))
		if err != nil {
			return doc.PutResult{}, err
		}
	}

	refName := plumbing.ReferenceName(mainRefName)
	for attempt := 0; attempt < casMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return doc.PutResult{}, err
		}

		baseRef, baseTree, baseTreeHash, err := loadBaseTree(repo, refName)
		if err != nil {
			return doc.PutResult{}, err
		}

		currentHead, err := loadStreamHeadHash(baseTree, streamPath)
		if err != nil {
			return doc.PutResult{}, err
		}
		if s.historyMode() != domain.HistoryModeAmend {
			if currentHead != write.Tx.ParentHash {
				return doc.PutResult{}, domain.ErrHeadChanged
			}
		}

		treeHash := baseTreeHash
		treeHash, err = updateTree(repo.Storer, treeHash, path.Join(streamPath, relTxPath), txBlobHash, filemode.Regular)
		if err != nil {
			return doc.PutResult{}, err
		}
		treeHash, err = updateTree(repo.Storer, treeHash, path.Join(streamPath, domain.StreamHeadFile), headBlobHash, filemode.Regular)
		if err != nil {
			return doc.PutResult{}, err
		}
		if statePath != "" {
			relStateTxPath := path.Join(domain.TxDirName, domain.TxCompactFile)
			treeHash, err = updateTree(repo.Storer, treeHash, path.Join(statePath, relStateTxPath), stateTxBlobHash, filemode.Regular)
			if err != nil {
				return doc.PutResult{}, err
			}
			treeHash, err = updateTree(repo.Storer, treeHash, path.Join(statePath, domain.StreamHeadFile), stateHeadBlobHash, filemode.Regular)
			if err != nil {
				return doc.PutResult{}, err
			}
		}

		commitHash, err := s.writeCommit(ctx, write.RepoPath, repo, treeHash, baseRef, write.Tx.TxID)
		if err != nil {
			return doc.PutResult{}, err
		}

		newRef := plumbing.NewHashReference(refName, commitHash)
		if err := repo.Storer.CheckAndSetReference(newRef, baseRef); err != nil {
			if errors.Is(err, storage.ErrReferenceHasChanged) {
				if attempt == casMaxRetries-1 {
					return doc.PutResult{}, domain.ErrHeadChanged
				}
				if err := sleepWithBackoff(ctx, attempt); err != nil {
					return doc.PutResult{}, err
				}
				continue
			}
			return doc.PutResult{}, fmt.Errorf("update main ref: %w", err)
		}

		return doc.PutResult{
			CommitHash: commitHash.String(),
			TxHash:     write.TxHash,
			TxID:       write.Tx.TxID,
		}, nil
	}

	return doc.PutResult{}, domain.ErrHeadChanged
}

func loadStreamHeadHash(tree *object.Tree, streamPath string) (string, error) {
	if tree == nil {
		return "", nil
	}

	headPath := path.Join(streamPath, domain.StreamHeadFile)
	headContent, err := readTreeFile(tree, headPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return "", nil
		}
		return "", err
	}

	relPath := strings.TrimSpace(string(headContent))
	if relPath == "" {
		return "", nil
	}

	txPath := path.Join(streamPath, relPath)
	txBytes, err := readTreeFile(tree, txPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return "", fmt.Errorf("stream tx missing at %s", txPath)
		}
		return "", err
	}

	return hashBytes(txBytes), nil
}

func loadBaseTree(repo *git.Repository, refName plumbing.ReferenceName) (*plumbing.Reference, *object.Tree, plumbing.Hash, error) {
	baseRef, err := repo.Reference(refName, true)
	if err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return nil, nil, plumbing.ZeroHash, fmt.Errorf("read main ref: %w", err)
	}
	if baseRef == nil {
		return nil, nil, plumbing.ZeroHash, nil
	}

	commit, err := repo.CommitObject(baseRef.Hash())
	if err != nil {
		return nil, nil, plumbing.ZeroHash, fmt.Errorf("read main commit: %w", err)
	}
	baseTree, err := commit.Tree()
	if err != nil {
		return nil, nil, plumbing.ZeroHash, fmt.Errorf("read main tree: %w", err)
	}
	return baseRef, baseTree, commit.TreeHash, nil
}

func sleepWithBackoff(ctx context.Context, attempt int) error {
	delay := casBackoffBase * time.Duration(1<<attempt)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readTreeFile(tree *object.Tree, filePath string) ([]byte, error) {
	file, err := tree.File(filePath)
	if err != nil {
		return nil, err
	}

	reader, err := file.Reader()
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}
	defer func() {
		_ = reader.Close()
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}

	return data, nil
}

func writeBlob(s storer.EncodedObjectStorer, data []byte) (plumbing.Hash, error) {
	obj := s.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	writer, err := obj.Writer()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("write blob: %w", err)
	}
	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		_ = writer.Close()
		return plumbing.ZeroHash, fmt.Errorf("write blob: %w", err)
	}
	if err := writer.Close(); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("write blob: %w", err)
	}
	return s.SetEncodedObject(obj)
}

func updateTree(s storer.EncodedObjectStorer, baseHash plumbing.Hash, filePath string, fileHash plumbing.Hash, mode filemode.FileMode) (plumbing.Hash, error) {
	var baseTree *object.Tree
	if !baseHash.IsZero() {
		var err error
		baseTree, err = object.GetTree(s, baseHash)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("load tree: %w", err)
		}
	} else {
		baseTree = &object.Tree{}
	}

	parts := strings.Split(strings.Trim(filePath, "/"), "/")
	updatedTree, err := updateTreeRecursive(s, baseTree, parts, fileHash, mode)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return writeTree(s, updatedTree)
}

func updateTreeRecursive(s storer.EncodedObjectStorer, tree *object.Tree, parts []string, fileHash plumbing.Hash, mode filemode.FileMode) (*object.Tree, error) {
	if len(parts) == 0 {
		return tree, nil
	}

	name := parts[0]
	entries := make([]object.TreeEntry, 0, len(tree.Entries)+1)
	var existing *object.TreeEntry
	for _, entry := range tree.Entries {
		if entry.Name == name {
			e := entry
			existing = &e
			continue
		}
		entries = append(entries, entry)
	}

	if len(parts) == 1 {
		entries = append(entries, object.TreeEntry{Name: name, Mode: mode, Hash: fileHash})
	} else {
		var childTree *object.Tree
		if existing != nil && existing.Mode == filemode.Dir {
			var err error
			childTree, err = object.GetTree(s, existing.Hash)
			if err != nil {
				return nil, fmt.Errorf("load tree %s: %w", name, err)
			}
		} else {
			childTree = &object.Tree{}
		}

		updatedChild, err := updateTreeRecursive(s, childTree, parts[1:], fileHash, mode)
		if err != nil {
			return nil, err
		}

		childHash, err := writeTree(s, updatedChild)
		if err != nil {
			return nil, err
		}

		entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: childHash})
	}

	sort.Sort(object.TreeEntrySorter(entries))
	return &object.Tree{Entries: entries}, nil
}

func writeTree(s storer.EncodedObjectStorer, tree *object.Tree) (plumbing.Hash, error) {
	obj := s.NewEncodedObject()
	if err := tree.Encode(obj); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("encode tree: %w", err)
	}
	return s.SetEncodedObject(obj)
}

func (s *Store) writeCommit(ctx context.Context, repoPath string, repo *git.Repository, treeHash plumbing.Hash, baseRef *plumbing.Reference, txID string) (plumbing.Hash, error) {
	parentRef := baseRef
	if s.historyMode() == domain.HistoryModeAmend {
		parentRef = nil
	}
	if s.options.SignCommits {
		return s.writeSignedCommit(ctx, repoPath, treeHash, parentRef, txID)
	}
	return writeUnsignedCommit(repo.Storer, treeHash, parentRef, txID)
}

func writeUnsignedCommit(s storer.EncodedObjectStorer, treeHash plumbing.Hash, baseRef *plumbing.Reference, txID string) (plumbing.Hash, error) {
	author := object.Signature{
		Name:  "ledgerdb",
		Email: "ledgerdb@local",
		When:  time.Now().UTC(),
	}

	commit := &object.Commit{
		Author:       author,
		Committer:    author,
		Message:      fmt.Sprintf("ledgerdb tx %s", txID),
		TreeHash:     treeHash,
		ParentHashes: nil,
	}

	if baseRef != nil {
		commit.ParentHashes = []plumbing.Hash{baseRef.Hash()}
	}

	obj := s.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("encode commit: %w", err)
	}
	return s.SetEncodedObject(obj)
}

func (s *Store) historyMode() domain.HistoryMode {
	return domain.NormalizeHistoryMode(s.options.HistoryMode)
}

func (s *Store) writeSignedCommit(ctx context.Context, repoPath string, treeHash plumbing.Hash, baseRef *plumbing.Reference, txID string) (plumbing.Hash, error) {
	if err := ctx.Err(); err != nil {
		return plumbing.ZeroHash, err
	}

	message := fmt.Sprintf("ledgerdb tx %s", txID)
	args := []string{"-C", repoPath, "commit-tree", treeHash.String(), "-m", message}
	if baseRef != nil {
		args = append(args, "-p", baseRef.Hash().String())
	}
	if s.options.SignKey != "" {
		args = append(args, "-S"+s.options.SignKey)
	} else {
		args = append(args, "-S")
	}

	cmd := exec.CommandContext(ctx, "git", args...)
	now := time.Now().UTC()
	date := fmt.Sprintf("%d +0000", now.Unix())
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=ledgerdb",
		"GIT_AUTHOR_EMAIL=ledgerdb@local",
		"GIT_AUTHOR_DATE="+date,
		"GIT_COMMITTER_NAME=ledgerdb",
		"GIT_COMMITTER_EMAIL=ledgerdb@local",
		"GIT_COMMITTER_DATE="+date,
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return plumbing.ZeroHash, fmt.Errorf("git commit-tree: %w: %s", err, msg)
		}
		return plumbing.ZeroHash, fmt.Errorf("git commit-tree: %w", err)
	}

	hash := strings.TrimSpace(stdout.String())
	if hash == "" {
		return plumbing.ZeroHash, errors.New("git commit-tree returned empty hash")
	}
	return plumbing.NewHash(hash), nil
}

func normalizeTreePath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	return strings.TrimPrefix(p, "./")
}

func txFileName(tx domain.Transaction) string {
	op := "unknown"
	switch tx.Op {
	case domain.TxOpPut:
		op = "put"
	case domain.TxOpPatch:
		op = "patch"
	case domain.TxOpDelete:
		op = "delete"
	case domain.TxOpMerge:
		op = "merge"
	}
	return fmt.Sprintf("%d_%s%s", tx.Timestamp, op, domain.TxFileExt)
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
