package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func (s *Store) LoadHeadTx(ctx context.Context, repoPath, streamPath string) (doc.TxBlob, error) {
	if err := ctx.Err(); err != nil {
		return doc.TxBlob{}, err
	}

	tree, err := loadMainTree(repoPath)
	if err != nil {
		if errors.Is(err, doc.ErrDocNotFound) {
			return doc.TxBlob{}, doc.ErrDocNotFound
		}
		return doc.TxBlob{}, err
	}

	streamPath = normalizeTreePath(streamPath)
	headPath := path.Join(streamPath, domain.StreamHeadFile)
	headContent, err := readTreeFile(tree, headPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return doc.TxBlob{}, doc.ErrDocNotFound
		}
		return doc.TxBlob{}, err
	}

	relPath := strings.TrimSpace(string(headContent))
	if relPath == "" {
		return doc.TxBlob{}, doc.ErrDocNotFound
	}

	txPath := path.Join(streamPath, relPath)
	txBytes, err := readTreeFile(tree, txPath)
	if err != nil {
		if errors.Is(err, object.ErrFileNotFound) {
			return doc.TxBlob{}, doc.ErrDocNotFound
		}
		return doc.TxBlob{}, err
	}

	return doc.TxBlob{Path: txPath, Bytes: txBytes}, nil
}

func (s *Store) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tree, err := loadMainTree(repoPath)
	if err != nil {
		if errors.Is(err, doc.ErrDocNotFound) {
			return nil, doc.ErrDocNotFound
		}
		return nil, err
	}

	streamPath = normalizeTreePath(streamPath)
	streamTree, err := tree.Tree(streamPath)
	if err != nil {
		if errors.Is(err, object.ErrDirectoryNotFound) {
			return nil, doc.ErrDocNotFound
		}
		return nil, fmt.Errorf("read stream tree: %w", err)
	}

	txTree, err := streamTree.Tree(domain.TxDirName)
	if err != nil {
		if errors.Is(err, object.ErrDirectoryNotFound) {
			return nil, doc.ErrDocNotFound
		}
		return nil, fmt.Errorf("read tx tree: %w", err)
	}

	var blobs []doc.TxBlob
	for _, entry := range txTree.Entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Mode == filemode.Dir {
			continue
		}
		if !strings.HasSuffix(entry.Name, domain.TxFileExt) {
			continue
		}
		blobBytes, err := readBlob(txTree, entry)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, doc.TxBlob{
			Path:  path.Join(streamPath, domain.TxDirName, entry.Name),
			Bytes: blobBytes,
		})
	}

	return blobs, nil
}

func loadMainTree(repoPath string) (*object.Tree, error) {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("open git repo: %w", err)
	}

	ref, err := repo.Reference(plumbing.ReferenceName(mainRefName), true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, doc.ErrDocNotFound
		}
		return nil, fmt.Errorf("read main ref: %w", err)
	}

	commit, err := repo.CommitObject(ref.Hash())
	if err != nil {
		return nil, fmt.Errorf("read main commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("read main tree: %w", err)
	}

	return tree, nil
}

func readBlob(tree *object.Tree, entry object.TreeEntry) ([]byte, error) {
	file, err := tree.TreeEntryFile(&entry)
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}

	reader, err := file.Reader()
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}

	return data, nil
}
