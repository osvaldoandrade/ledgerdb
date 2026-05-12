package cli

import (
	"encoding/json"
	"fmt"

	drapp "github.com/osvaldoandrade/ledgerdb/internal/app/dr"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/spf13/cobra"
)

func newTruncateCmd(opts *RootOptions) *cobra.Command {
	var before string
	var collection string
	var dryRun bool
	var yes bool
	cmd := &cobra.Command{
		Use:   "truncate",
		Short: "Drop history older than a threshold (keeps latest snapshot per doc)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := newGitStore(opts)
			service := drapp.NewTruncateService(
				store,
				store,
				txv3.Decoder{},
				hash.SHA256{},
				store,
				opts.StreamLayout,
			)

			var result drapp.TruncateResult
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			labelText := "Truncating history"
			if dryRun {
				labelText = "Planning truncate"
			}
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent(labelText)
			err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				var err error
				result, err = service.Truncate(cmd.Context(), opts.RepoPath, drapp.TruncateOptions{
					Before:     before,
					Collection: collection,
					DryRun:     dryRun,
					Yes:        yes,
				})
				return err
			})
			if err != nil {
				return err
			}
			return writeTruncateResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&before, "before", "", "ISO-8601 timestamp, unix-nanos, or tx_id threshold")
	cmd.Flags().StringVar(&collection, "collection", "", "Limit truncation to a single collection")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Report the plan without modifying refs")
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm the destructive truncation (required outside --dry-run)")
	return cmd
}

type truncateStreamOutput struct {
	StreamPath  string `json:"stream_path"`
	KeptTx      int    `json:"kept_tx"`
	DroppedTx   int    `json:"dropped_tx"`
	NewBaseHash string `json:"new_base_hash,omitempty"`
}

type truncatePlanOutput struct {
	ThresholdTimestamp int64                  `json:"threshold_timestamp,omitempty"`
	ThresholdTxID      string                 `json:"threshold_tx_id,omitempty"`
	Collection         string                 `json:"collection,omitempty"`
	Streams            []truncateStreamOutput `json:"streams"`
}

type truncateExecutionOutput struct {
	BackupBranch string `json:"backup_branch"`
	NewBranch    string `json:"new_branch"`
}

type truncateOutput struct {
	DryRun    bool                     `json:"dry_run"`
	Plan      truncatePlanOutput       `json:"plan"`
	Execution *truncateExecutionOutput `json:"execution,omitempty"`
}

func writeTruncateResult(cmd *cobra.Command, result drapp.TruncateResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := truncateOutput{
			DryRun: result.DryRun,
			Plan: truncatePlanOutput{
				ThresholdTimestamp: result.Plan.ThresholdTimestamp,
				ThresholdTxID:      result.Plan.ThresholdTxID,
				Collection:         result.Plan.Collection,
				Streams:            make([]truncateStreamOutput, 0, len(result.Plan.Streams)),
			},
		}
		for _, s := range result.Plan.Streams {
			payload.Plan.Streams = append(payload.Plan.Streams, truncateStreamOutput{
				StreamPath:  s.StreamPath,
				KeptTx:      s.KeptTx,
				DroppedTx:   s.DroppedTx,
				NewBaseHash: s.NewBaseHash,
			})
		}
		if result.Execution != nil {
			payload.Execution = &truncateExecutionOutput{
				BackupBranch: result.Execution.BackupBranch,
				NewBranch:    result.Execution.NewBranch,
			}
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	totalKept, totalDropped := 0, 0
	for _, s := range result.Plan.Streams {
		totalKept += s.KeptTx
		totalDropped += s.DroppedTx
	}
	if err := writeKV(out, ui, "Streams", fmt.Sprintf("%d", len(result.Plan.Streams))); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Kept Tx", fmt.Sprintf("%d", totalKept)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Dropped Tx", fmt.Sprintf("%d", totalDropped)); err != nil {
		return err
	}
	if result.DryRun {
		return writeKV(out, ui, "Dry Run", ui.warn("true"))
	}
	if result.Execution != nil {
		if err := writeKV(out, ui, "Backup Branch", result.Execution.BackupBranch); err != nil {
			return err
		}
		return writeKV(out, ui, "New Branch", result.Execution.NewBranch)
	}
	return nil
}
