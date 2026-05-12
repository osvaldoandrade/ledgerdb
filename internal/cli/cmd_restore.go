package cli

import (
	"encoding/json"
	"fmt"

	drapp "github.com/osvaldoandrade/ledgerdb/internal/app/dr"
	integrityapp "github.com/osvaldoandrade/ledgerdb/internal/app/integrity"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/jsonpatch"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
	"github.com/spf13/cobra"
)

func newRestoreCmd(opts *RootOptions) *cobra.Command {
	var input string
	var target string
	var skipVerify bool
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a repository from a sealed tar.gz backup",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := newGitStore(opts)
			verifier := integrityapp.NewVerifyService(store, store, txv3.Decoder{}, hash.SHA256{}, jsonpatch.Patcher{})
			service := drapp.NewRestoreService(store, verifier, platform.RealClock{})

			if skipVerify {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: --skip-verify disables post-restore integrity verification")
			}

			var result drapp.RestoreResult
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Restoring backup")
			err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				var err error
				result, err = service.Restore(cmd.Context(), drapp.RestoreOptions{
					InputPath:  input,
					TargetPath: target,
					SkipVerify: skipVerify,
				})
				return err
			})
			if err != nil {
				return err
			}
			return writeRestoreResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "Backup tarball path")
	cmd.Flags().StringVar(&target, "target", "", "Target directory (default: ./restored-<utc>)")
	cmd.Flags().BoolVar(&skipVerify, "skip-verify", false, "Skip post-restore integrity verification (not recommended)")
	if err := cmd.MarkFlagRequired("input"); err != nil {
		return cmd
	}
	return cmd
}

type restoreOutput struct {
	TargetPath       string `json:"target_path"`
	BundleHash       string `json:"bundle_hash"`
	Streams          int    `json:"streams"`
	IntegrityValid   int    `json:"integrity_valid"`
	IntegritySkipped bool   `json:"integrity_skipped"`
}

func writeRestoreResult(cmd *cobra.Command, result drapp.RestoreResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := restoreOutput{
			TargetPath:       result.TargetPath,
			BundleHash:       result.BundleHash,
			Streams:          result.Streams,
			IntegrityValid:   result.IntegrityValid,
			IntegritySkipped: result.IntegritySkipped,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Target", result.TargetPath); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Bundle Hash", result.BundleHash); err != nil {
		return err
	}
	if result.IntegritySkipped {
		return writeKV(out, ui, "Integrity", ui.warn("skipped"))
	}
	return writeKV(out, ui, "Integrity", fmt.Sprintf("%d/%d valid", result.IntegrityValid, result.Streams))
}
