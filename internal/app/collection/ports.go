package collection

import "context"

type SchemaSource interface {
	ReadSchema(ctx context.Context, path string) ([]byte, error)
}

type SchemaValidator interface {
	Validate(ctx context.Context, schema []byte) error
}

type Store interface {
	WriteSchema(ctx context.Context, repoPath, collection string, schema []byte, indexes []string) error
}
