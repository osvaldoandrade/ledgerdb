package cli

import (
	"encoding/json"
	"fmt"

	drapp "github.com/osvaldoandrade/ledgerdb/internal/app/dr"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
	"github.com/spf13/cobra"
)

func newBackupCmd(opts *RootOptions) *cobra.Command {
	var output string
	var includeSidecar bool
	var sidecarPath string
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Create a sealed tar.gz backup of the repository",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := newGitStore(opts)
			service := drapp.NewBackupService(store, platform.RealClock{})

			var result drapp.BackupResult
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Creating backup")
			err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				var err error
				result, err = service.Backup(cmd.Context(), opts.RepoPath, drapp.BackupOptions{
					OutputPath:     output,
					IncludeSidecar: includeSidecar,
					SidecarPath:    sidecarPath,
				})
				return err
			})
			if err != nil {
				return err
			}
			return writeBackupResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output tarball path (defaults to ledgerdb-backup-<utc>.tar.gz)")
	cmd.Flags().BoolVar(&includeSidecar, "include-sidecar", false, "Include the SQLite sidecar (off by default; it is rebuildable)")
	cmd.Flags().StringVar(&sidecarPath, "sidecar-path", "", "Path to the SQLite sidecar to embed (requires --include-sidecar)")
	return cmd
}

type backupOutput struct {
	OutputPath      string `json:"output_path"`
	BundleSize      int64  `json:"bundle_size"`
	BundleHash      string `json:"bundle_hash"`
	CreatedAt       string `json:"created_at"`
	IncludesSidecar bool   `json:"includes_sidecar"`
}

func writeBackupResult(cmd *cobra.Command, result drapp.BackupResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := backupOutput{
			OutputPath:      result.OutputPath,
			BundleSize:      result.BundleSize,
			BundleHash:      result.BundleHash,
			CreatedAt:       result.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
			IncludesSidecar: result.IncludesSidecar,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Output", result.OutputPath); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Bundle Hash", result.BundleHash); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Bundle Size", fmt.Sprintf("%d", result.BundleSize)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Created At", result.CreatedAt.Format("2006-01-02T15:04:05Z07:00")); err != nil {
		return err
	}
	return writeKV(out, ui, "Sidecar", fmt.Sprintf("%t", result.IncludesSidecar))
}
