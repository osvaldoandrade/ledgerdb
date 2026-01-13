package schema

import (
	"bytes"
	"context"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type JSONSchemaValidator struct{}

func (JSONSchemaValidator) Validate(ctx context.Context, schema []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", bytes.NewReader(schema)); err != nil {
		return fmt.Errorf("load schema: %w", err)
	}

	if _, err := compiler.Compile("schema.json"); err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}

	return nil
}
