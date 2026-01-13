package canonicaljson

import (
	"context"
	"fmt"

	"github.com/go-json-experiment/json/jsontext"
)

type Canonicalizer struct{}

func (Canonicalizer) Canonicalize(ctx context.Context, input []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value := jsontext.Value(append([]byte(nil), input...))
	if err := value.Canonicalize(); err != nil {
		return nil, fmt.Errorf("canonicalize json: %w", err)
	}

	return []byte(value), nil
}
