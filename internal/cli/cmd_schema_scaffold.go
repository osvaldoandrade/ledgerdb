package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/spf13/cobra"
)

// ErrScaffoldFileExists is returned when the scaffold target file already
// exists and --force was not supplied.
var ErrScaffoldFileExists = errors.New("scaffold file already exists")

// ErrInvalidScaffoldName is returned when the collection name is invalid.
var ErrInvalidScaffoldName = errors.New("invalid scaffold collection name")

func newSchemaCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Manage JSON Schema files",
		RunE:  runHelp,
	}
	cmd.AddCommand(newSchemaScaffoldCmd(opts))
	return cmd
}

func newSchemaScaffoldCmd(_ *RootOptions) *cobra.Command {
	var output string
	var force bool
	var minimal bool
	cmd := &cobra.Command{
		Use:   "scaffold <name>",
		Short: "Emit a starter JSON Schema for a collection",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := strings.TrimSpace(args[0])
			if name == "" {
				return fmt.Errorf("%w: name is empty", ErrInvalidScaffoldName)
			}
			if !domain.IsValidCollectionName(name) {
				return fmt.Errorf("%w: %q", ErrInvalidScaffoldName, name)
			}

			body := scaffoldSchema(name, minimal)

			target := strings.TrimSpace(output)
			if target == "" {
				target = filepath.Join("schemas", name+".json")
			}

			if target == "-" {
				_, err := io.WriteString(cmd.OutOrStdout(), body)
				return err
			}

			return writeScaffoldFile(target, body, force)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output path (use '-' for stdout; default schemas/<name>.json)")
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing file")
	cmd.Flags().BoolVar(&minimal, "minimal", false, "Emit a minimal schema (just type: object)")
	return cmd
}

func writeScaffoldFile(path, body string, force bool) error {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%w: %s (use --force to overwrite)", ErrScaffoldFileExists, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat output: %w", err)
		}
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
	}

	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	return nil
}

func scaffoldSchema(name string, minimal bool) string {
	// json.Marshal escapes the name to prevent injection if it contains quotes.
	titleJSON, _ := json.Marshal(name)
	idJSON, _ := json.Marshal("https://ledgerdb.local/schemas/" + name + ".json")

	if minimal {
		return fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": %s,
  "title": %s,
  "type": "object"
}
`, idJSON, titleJSON)
	}
	return fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": %s,
  "title": %s,
  "type": "object",
  "additionalProperties": false,
  "required": ["id", "created_at", "updated_at"],
  "properties": {
    "id": {
      "type": "string",
      "description": "ULID identifier",
      "pattern": "^[0-9A-HJKMNP-TV-Z]{26}$"
    },
    "created_at": {
      "type": "string",
      "format": "date-time",
      "description": "RFC 3339 creation timestamp"
    },
    "updated_at": {
      "type": "string",
      "format": "date-time",
      "description": "RFC 3339 last-update timestamp"
    },
    "name": {
      "type": "string",
      "description": "Example optional field; replace with your domain fields (string, integer, number, boolean, object, array)."
    }
  }
}
`, idJSON, titleJSON)
}
