package cli

import (
	"encoding/json"
	"fmt"
	"time"

	statsapp "github.com/osvaldoandrade/ledgerdb/internal/app/stats"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/spf13/cobra"
)

const statsSampleStreams = 256

func newStatsCmd(opts *RootOptions) *cobra.Command {
	var collection string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show repository operational summary",
		RunE: func(cmd *cobra.Command, _ []string) error {
			service := statsapp.NewService(newGitStore(opts), txv3.Decoder{})
			result, err := service.Stats(cmd.Context(), opts.RepoPath, statsapp.Options{
				Collection:    collection,
				SampleStreams: statsSampleStreams,
			})
			if err != nil {
				return err
			}
			return writeStatsResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&collection, "collection", "", "Limit collection-level stats to one collection")
	return cmd
}

type statsCollectionOutput struct {
	Name           string  `json:"name"`
	DocCount       int     `json:"doc_count"`
	LastTimestamp  int64   `json:"last_timestamp,omitempty"`
	AvgChainDepth  float64 `json:"avg_chain_depth"`
	MaxChainDepth  int     `json:"max_chain_depth"`
	SampledStreams int     `json:"sampled_streams"`
}

type statsCountObjectsOutput struct {
	LooseObjects int64 `json:"loose_objects"`
	PackCount    int64 `json:"pack_count"`
	SizeBytes    int64 `json:"size_bytes"`
	SizePackKB   int64 `json:"size_pack_kb"`
}

type statsManifestOutput struct {
	Version      int    `json:"version"`
	Name         string `json:"name"`
	StreamLayout string `json:"stream_layout,omitempty"`
	HistoryMode  string `json:"history_mode,omitempty"`
}

type statsOutput struct {
	Path        string                  `json:"path"`
	Head        string                  `json:"head,omitempty"`
	Manifest    *statsManifestOutput    `json:"manifest,omitempty"`
	DiskBytes   int64                   `json:"disk_bytes"`
	Objects     statsCountObjectsOutput `json:"objects"`
	Collections []statsCollectionOutput `json:"collections"`
	Replication struct {
		HasRemote bool `json:"has_remote"`
		Ahead     int  `json:"ahead"`
		Behind    int  `json:"behind"`
	} `json:"replication"`
}

func writeStatsResult(cmd *cobra.Command, result statsapp.Result, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := statsOutput{
			Path:      result.RepoPath,
			Head:      result.HeadHash,
			DiskBytes: result.DiskBytes,
			Objects: statsCountObjectsOutput{
				LooseObjects: result.CountObjects.LooseObjects,
				PackCount:    result.CountObjects.PackCount,
				SizeBytes:    result.CountObjects.SizeBytes,
				SizePackKB:   result.CountObjects.SizePackKB,
			},
			Collections: make([]statsCollectionOutput, 0, len(result.Collections)),
		}
		if result.HasManifest {
			payload.Manifest = &statsManifestOutput{
				Version:      result.Manifest.Version,
				Name:         result.Manifest.Name,
				StreamLayout: string(result.Manifest.StreamLayout),
				HistoryMode:  string(result.Manifest.HistoryMode),
			}
		}
		for _, col := range result.Collections {
			payload.Collections = append(payload.Collections, statsCollectionOutput{
				Name:           col.Name,
				DocCount:       col.DocCount,
				LastTimestamp:  col.LastTimestamp,
				AvgChainDepth:  col.AvgChainDepth,
				MaxChainDepth:  col.MaxChainDepth,
				SampledStreams: col.SampledStreams,
			})
		}
		payload.Replication.HasRemote = result.HasRemote
		payload.Replication.Ahead = result.Ahead
		payload.Replication.Behind = result.Behind

		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if _, err := fmt.Fprintln(out, ui.accent("Repo")); err != nil {
		return err
	}
	if err := writeKV(out, ui, "  Path", result.RepoPath); err != nil {
		return err
	}
	if result.HasManifest {
		if err := writeKV(out, ui, "  Layout", string(result.Manifest.StreamLayout)); err != nil {
			return err
		}
		if err := writeKV(out, ui, "  History", string(result.Manifest.HistoryMode)); err != nil {
			return err
		}
		if err := writeKV(out, ui, "  Version", fmt.Sprintf("%d", result.Manifest.Version)); err != nil {
			return err
		}
	} else {
		if err := writeKV(out, ui, "  Manifest", ui.dim("(missing)")); err != nil {
			return err
		}
	}
	if result.HasHead {
		if err := writeKV(out, ui, "  Head", result.HeadHash); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(out, ui.accent("Storage")); err != nil {
		return err
	}
	if err := writeKV(out, ui, "  Size On Disk", humanBytes(result.DiskBytes)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "  Loose Objects", fmt.Sprintf("%d", result.CountObjects.LooseObjects)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "  Pack Count", fmt.Sprintf("%d", result.CountObjects.PackCount)); err != nil {
		return err
	}
	if result.CountObjects.SizePackKB > 0 {
		if err := writeKV(out, ui, "  Pack Size", humanBytes(result.CountObjects.SizePackKB*1024)); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(out, ui.accent("Collections")); err != nil {
		return err
	}
	if len(result.Collections) == 0 {
		if _, err := fmt.Fprintln(out, "  "+ui.dim("(none)")); err != nil {
			return err
		}
	} else {
		header := fmt.Sprintf("  %-24s %-10s %-12s %-12s %s", "Name", "Docs", "Avg Chain", "Max Chain", "Last Tx")
		if _, err := fmt.Fprintln(out, ui.dim(header)); err != nil {
			return err
		}
		for _, col := range result.Collections {
			row := fmt.Sprintf("  %-24s %-10d %-12.2f %-12d %s",
				col.Name, col.DocCount, col.AvgChainDepth, col.MaxChainDepth, formatTimestamp(col.LastTimestamp))
			if _, err := fmt.Fprintln(out, row); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(out, ui.accent("Replication")); err != nil {
		return err
	}
	if !result.HasRemote {
		if _, err := fmt.Fprintln(out, "  "+ui.dim("(no origin/main)")); err != nil {
			return err
		}
		return nil
	}
	if err := writeKV(out, ui, "  Ahead origin/main", fmt.Sprintf("%d", result.Ahead)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "  Behind origin/main", fmt.Sprintf("%d", result.Behind)); err != nil {
		return err
	}
	return nil
}

func humanBytes(size int64) string {
	if size <= 0 {
		return "0 B"
	}
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div := int64(unit)
	exp := 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(size)/float64(div), "KMGTPE"[exp])
}

func formatTimestamp(nanos int64) string {
	if nanos <= 0 {
		return "(never)"
	}
	t := time.Unix(0, nanos).UTC()
	return t.Format("2006-01-02T15:04:05Z")
}

