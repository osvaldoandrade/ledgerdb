package cli

import (
	"context"
	"encoding/json"
	"fmt"

	collectionapp "github.com/osvaldoandrade/ledgerdb/internal/app/collection"
	migrateapp "github.com/osvaldoandrade/ledgerdb/internal/app/migrate"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/filesystem"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/gitrepo"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/schema"
	"github.com/spf13/cobra"
)

// manifestStoreAdapter bridges gitrepo's package-level LoadManifest and
// *Store.WriteManifest into the migrate.ManifestStore port.
type manifestStoreAdapter struct {
	store *gitrepo.Store
}

func (a manifestStoreAdapter) LoadManifest(repoPath string) (domain.Manifest, error) {
	return gitrepo.LoadManifest(repoPath)
}

func (a manifestStoreAdapter) WriteManifest(ctx context.Context, repoPath string, manifest domain.Manifest) error {
	return a.store.WriteManifest(ctx, repoPath, manifest)
}

func newMigrateCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run ordered schema migrations",
		RunE:  runHelp,
	}
	cmd.AddCommand(newMigrateApplyCmd(opts), newMigrateStatusCmd(opts))
	return cmd
}

func buildMigrateService(opts *RootOptions) (*migrateapp.Service, *gitrepo.Store) {
	store := newGitStore(opts)
	collectionSvc := collectionapp.NewService(store, filesystem.SchemaSource{}, schema.JSONSchemaValidator{})
	migrateSvc := migrateapp.NewService(collectionSvc, manifestStoreAdapter{store: store})
	return migrateSvc, store
}

func newMigrateApplyCmd(opts *RootOptions) *cobra.Command {
	var target int
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply pending migrations in order",
		RunE: func(cmd *cobra.Command, _ []string) error {
			service, store := buildMigrateService(opts)
			return runWithAutoSync(cmd, opts, store, func() error {
				result, err := service.Apply(cmd.Context(), opts.RepoPath, migrateapp.ApplyOptions{
					Target: target,
					DryRun: dryRun,
				})
				if err != nil {
					return err
				}
				return writeMigrateApplyResult(cmd, result, opts.JSONOutput)
			})
		},
	}
	cmd.Flags().IntVar(&target, "target", 0, "Apply up to this migration number (0 = apply all)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview migrations that would be applied")
	return cmd
}

func newMigrateStatusCmd(opts *RootOptions) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report applied and pending migrations",
		RunE: func(cmd *cobra.Command, _ []string) error {
			service, _ := buildMigrateService(opts)
			result, err := service.Status(cmd.Context(), opts.RepoPath)
			if err != nil {
				return err
			}
			return writeMigrateStatusResult(cmd, result, opts.JSONOutput || asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit JSON output")
	return cmd
}

type migrateApplyOutput struct {
	Applied []string `json:"applied"`
	Pending []string `json:"pending"`
	DryRun  bool     `json:"dry_run"`
}

type migrateStatusOutput struct {
	Applied []string `json:"applied"`
	Pending []string `json:"pending"`
}

func writeMigrateApplyResult(cmd *cobra.Command, result migrateapp.ApplyResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := migrateApplyOutput{
			Applied: result.Applied,
			Pending: result.Pending,
			DryRun:  result.DryRun,
		}
		if payload.Applied == nil {
			payload.Applied = []string{}
		}
		if payload.Pending == nil {
			payload.Pending = []string{}
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if result.DryRun {
		if _, err := fmt.Fprintf(out, "%s: %d pending migration(s)\n", ui.warn("Dry run"), len(result.Pending)); err != nil {
			return err
		}
		for _, name := range result.Pending {
			if _, err := fmt.Fprintf(out, "- %s\n", name); err != nil {
				return err
			}
		}
		return nil
	}

	if len(result.Applied) == 0 {
		_, err := fmt.Fprintf(out, "%s: nothing to apply\n", ui.ok("OK"))
		return err
	}
	if _, err := fmt.Fprintf(out, "%s: applied %d migration(s)\n", ui.ok("OK"), len(result.Applied)); err != nil {
		return err
	}
	for _, name := range result.Applied {
		if _, err := fmt.Fprintf(out, "- %s\n", name); err != nil {
			return err
		}
	}
	return nil
}

func writeMigrateStatusResult(cmd *cobra.Command, result migrateapp.StatusResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := migrateStatusOutput{
			Applied: result.Applied,
			Pending: result.Pending,
		}
		if payload.Applied == nil {
			payload.Applied = []string{}
		}
		if payload.Pending == nil {
			payload.Pending = []string{}
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Applied", fmt.Sprintf("%d", len(result.Applied))); err != nil {
		return err
	}
	for _, name := range result.Applied {
		if _, err := fmt.Fprintf(out, "  + %s\n", name); err != nil {
			return err
		}
	}
	if err := writeKV(out, ui, "Pending", fmt.Sprintf("%d", len(result.Pending))); err != nil {
		return err
	}
	for _, name := range result.Pending {
		if _, err := fmt.Fprintf(out, "  - %s\n", name); err != nil {
			return err
		}
	}
	return nil
}
