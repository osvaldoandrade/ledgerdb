package gitrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	collectionapp "github.com/osvaldoandrade/ledgerdb/internal/app/collection"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

const collectionIndexesFile = "indexes.json"

func (s *Store) WriteSchema(ctx context.Context, repoPath, collection string, schema []byte, indexes []domain.IndexSpec) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	collectionDir := filepath.Join(repoPath, "collections", collection)
	if err := os.MkdirAll(collectionDir, 0o755); err != nil {
		return fmt.Errorf("create collection dir: %w", err)
	}

	schemaPath := filepath.Join(collectionDir, "schema.json")
	if err := os.WriteFile(schemaPath, schema, 0o644); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}

	indexPath := filepath.Join(collectionDir, collectionIndexesFile)
	if len(indexes) == 0 {
		if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove indexes: %w", err)
		}
		return nil
	}

	payload, err := json.MarshalIndent(indexes, "", "  ")
	if err != nil {
		return fmt.Errorf("encode indexes: %w", err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(indexPath, payload, 0o644); err != nil {
		return fmt.Errorf("write indexes: %w", err)
	}
	return nil
}

// ReadCollectionIndexes loads the index specs for a collection from disk.
// It accepts both the legacy JSON form (an array of strings, each treated
// as a single-column index) and the current rich form ({name, fields[],
// unique?}). Returns (nil, nil) if no indexes are defined.
func (s *Store) ReadCollectionIndexes(ctx context.Context, repoPath, collection string) ([]domain.IndexSpec, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(repoPath, "collections", collection, collectionIndexesFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read indexes: %w", err)
	}
	return collectionapp.DecodeIndexesJSON(data)
}
