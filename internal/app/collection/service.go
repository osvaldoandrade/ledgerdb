package collection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type Service struct {
	store     Store
	source    SchemaSource
	validator SchemaValidator
}

func NewService(store Store, source SchemaSource, validator SchemaValidator) *Service {
	return &Service{
		store:     store,
		source:    source,
		validator: validator,
	}
}

func (s *Service) Apply(ctx context.Context, repoPath, collection, schemaPath string, indexes []string) error {
	collection = strings.TrimSpace(collection)
	if collection == "" {
		return ErrCollectionRequired
	}
	if !domain.IsValidCollectionName(collection) {
		return ErrInvalidCollectionName
	}

	schemaPath = strings.TrimSpace(schemaPath)
	if schemaPath == "" {
		return ErrSchemaPathRequired
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return err
	}

	absSchemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		return fmt.Errorf("resolve schema path: %w", err)
	}

	schema, err := s.source.ReadSchema(ctx, absSchemaPath)
	if err != nil {
		return err
	}

	schema = bytes.TrimSpace(schema)
	if len(schema) == 0 || !json.Valid(schema) {
		return ErrSchemaInvalidJSON
	}

	if s.validator != nil {
		if err := s.validator.Validate(ctx, schema); err != nil {
			return err
		}
	}

	indexes = normalizeIndexes(indexes)

	return s.store.WriteSchema(ctx, absRepoPath, collection, schema, indexes)
}

func normalizeIndexes(indexes []string) []string {
	seen := make(map[string]struct{})
	var normalized []string
	for _, item := range indexes {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	sort.Strings(normalized)
	return normalized
}
