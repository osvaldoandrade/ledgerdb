package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"io"

	inspectapp "github.com/osvaldoandrade/ledgerdb/internal/app/inspect"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func (s *Store) ReadBlob(ctx context.Context, repoPath, objectHash string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !plumbing.IsHash(objectHash) {
		return nil, inspectapp.ErrInvalidHash
	}

	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("open git repo: %w", err)
	}

	hash := plumbing.NewHash(objectHash)
	blob, err := repo.BlobObject(hash)
	if err != nil {
		if errors.Is(err, plumbing.ErrObjectNotFound) || errors.Is(err, object.ErrFileNotFound) {
			return nil, inspectapp.ErrBlobNotFound
		}
		return nil, fmt.Errorf("read blob: %w", err)
	}

	reader, err := blob.Reader()
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
