package gitrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (s *Store) WriteSchema(ctx context.Context, repoPath, collection string, schema []byte, indexes []string) error {
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

	if len(indexes) > 0 {
		indexPath := filepath.Join(collectionDir, "indexes.json")
		payload, err := json.MarshalIndent(indexes, "", "  ")
		if err != nil {
			return fmt.Errorf("encode indexes: %w", err)
		}
		payload = append(payload, '\n')
		if err := os.WriteFile(indexPath, payload, 0o644); err != nil {
			return fmt.Errorf("write indexes: %w", err)
		}
	} else {
		indexPath := filepath.Join(collectionDir, "indexes.json")
		if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove indexes: %w", err)
		}
	}

	return nil
}
