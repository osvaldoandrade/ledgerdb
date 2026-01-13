package filesystem

import (
	"context"
	"fmt"
	"os"
)

type SchemaSource struct{}

func (SchemaSource) ReadSchema(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	return data, nil
}
