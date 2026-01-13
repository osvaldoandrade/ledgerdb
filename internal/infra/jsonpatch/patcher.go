package jsonpatch

import (
	"context"
	"fmt"

	"github.com/evanphx/json-patch/v5"
)

type Patcher struct{}

func (Patcher) Apply(ctx context.Context, doc, patch []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, fmt.Errorf("decode patch: %w", err)
	}

	out, err := decoded.Apply(doc)
	if err != nil {
		return nil, fmt.Errorf("apply patch: %w", err)
	}
	return out, nil
}
