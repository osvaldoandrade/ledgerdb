package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func (s *Store) ListDocStreams(ctx context.Context, repoPath string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tree, err := loadMainTree(repoPath)
	if err != nil {
		if errors.Is(err, doc.ErrDocNotFound) {
			return nil, nil
		}
		return nil, err
	}

	docsTree, err := tree.Tree(domain.DocumentsRoot)
	if err != nil {
		if errors.Is(err, object.ErrDirectoryNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read documents tree: %w", err)
	}

	var streams []string
	for _, collectionEntry := range docsTree.Entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if collectionEntry.Mode != filemode.Dir {
			continue
		}
		collectionName := collectionEntry.Name
		collectionTree, err := docsTree.Tree(collectionName)
		if err != nil {
			return nil, fmt.Errorf("read collection tree %s: %w", collectionName, err)
		}
		collectionBase := path.Join(domain.DocumentsRoot, collectionName)
		colStreams, err := collectDocStreams(ctx, collectionTree, collectionBase)
		if err != nil {
			return nil, err
		}
		streams = append(streams, colStreams...)
	}

	sort.Strings(streams)
	return streams, nil
}

func collectDocStreams(ctx context.Context, tree *object.Tree, basePath string) ([]string, error) {
	var streams []string
	for _, entry := range tree.Entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Mode != filemode.Dir {
			continue
		}
		fullPath := path.Join(basePath, entry.Name)
		if strings.HasPrefix(entry.Name, "DOC_") {
			streams = append(streams, fullPath)
			continue
		}

		childTree, err := tree.Tree(entry.Name)
		if err != nil {
			return nil, fmt.Errorf("read stream tree %s: %w", fullPath, err)
		}
		nested, err := collectDocStreams(ctx, childTree, fullPath)
		if err != nil {
			return nil, err
		}
		streams = append(streams, nested...)
	}
	return streams, nil
}
