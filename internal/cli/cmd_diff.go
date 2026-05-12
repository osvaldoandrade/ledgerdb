package cli

import (
	"encoding/json"
	"fmt"
	"io"

	statsapp "github.com/osvaldoandrade/ledgerdb/internal/app/stats"
	"github.com/spf13/cobra"
)

func newDiffCmd(opts *RootOptions) *cobra.Command {
	var collection string
	var limit int
	cmd := &cobra.Command{
		Use:   "diff <refA> <refB>",
		Short: "Compare per-collection document state between two refs",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			service := statsapp.NewDiffService(newGitStore(opts))
			result, err := service.Diff(cmd.Context(), opts.RepoPath, args[0], args[1], statsapp.DiffOptions{
				Collection: collection,
				Limit:      limit,
			})
			if err != nil {
				return err
			}
			return writeDiffResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&collection, "collection", "", "Limit results to one collection")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum entries per category, per collection (0 = unlimited)")
	return cmd
}

type diffEntryOutput struct {
	DocID    string `json:"doc_id"`
	FromHash string `json:"from_hash,omitempty"`
	ToHash   string `json:"to_hash,omitempty"`
}

type diffCollectionOutput struct {
	Collection   string            `json:"collection"`
	AddedCount   int               `json:"added_count"`
	ChangedCount int               `json:"changed_count"`
	RemovedCount int               `json:"removed_count"`
	Added        []diffEntryOutput `json:"added,omitempty"`
	Changed      []diffEntryOutput `json:"changed,omitempty"`
	Removed      []diffEntryOutput `json:"removed,omitempty"`
}

type diffOutput struct {
	RefA        string                 `json:"ref_a"`
	RefB        string                 `json:"ref_b"`
	HashA       string                 `json:"hash_a,omitempty"`
	HashB       string                 `json:"hash_b,omitempty"`
	Limit       int                    `json:"limit"`
	Collections []diffCollectionOutput `json:"collections"`
}

func writeDiffResult(cmd *cobra.Command, result statsapp.DiffResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := diffOutput{
			RefA:        result.RefA,
			RefB:        result.RefB,
			HashA:       result.HashA,
			HashB:       result.HashB,
			Limit:       result.Limit,
			Collections: make([]diffCollectionOutput, 0, len(result.Collections)),
		}
		for _, col := range result.Collections {
			payload.Collections = append(payload.Collections, diffCollectionOutput{
				Collection:   col.Collection,
				AddedCount:   col.AddedTotal,
				ChangedCount: col.ChangedAll,
				RemovedCount: col.RemovedAll,
				Added:        diffEntriesOutput(col.Added),
				Changed:      diffEntriesOutput(col.Changed),
				Removed:      diffEntriesOutput(col.Removed),
			})
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if _, err := fmt.Fprintf(out, "%s %s..%s\n", ui.key("Diff"), shortHash(result.HashA, result.RefA), shortHash(result.HashB, result.RefB)); err != nil {
		return err
	}
	changed := filterChangedCollections(result.Collections)
	if len(changed) == 0 {
		_, err := fmt.Fprintln(out, ui.dim("  (no changes)"))
		return err
	}

	header := fmt.Sprintf("  %-24s %-8s %-8s %-8s", "Collection", "Added", "Changed", "Removed")
	if _, err := fmt.Fprintln(out, ui.dim(header)); err != nil {
		return err
	}
	for _, col := range changed {
		row := fmt.Sprintf("  %-24s %-8d %-8d %-8d", col.Collection, col.AddedTotal, col.ChangedAll, col.RemovedAll)
		if _, err := fmt.Fprintln(out, row); err != nil {
			return err
		}
	}

	for _, col := range changed {
		if len(col.Added)+len(col.Changed)+len(col.Removed) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(out, "\n%s %s\n", ui.accent("Collection"), col.Collection); err != nil {
			return err
		}
		if err := writeDiffSection(out, ui, "Added", col.Added, col.AddedTotal); err != nil {
			return err
		}
		if err := writeDiffSection(out, ui, "Changed", col.Changed, col.ChangedAll); err != nil {
			return err
		}
		if err := writeDiffSection(out, ui, "Removed", col.Removed, col.RemovedAll); err != nil {
			return err
		}
	}
	return nil
}

func writeDiffSection(out io.Writer, ui renderer, label string, entries []statsapp.DiffEntry, total int) error {
	if len(entries) == 0 {
		return nil
	}
	prefix := ui.dim(fmt.Sprintf("  %s (%d/%d)", label, len(entries), total))
	if _, err := fmt.Fprintln(out, prefix); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(out, "    - %s\n", entry.DocID); err != nil {
			return err
		}
	}
	return nil
}

func diffEntriesOutput(entries []statsapp.DiffEntry) []diffEntryOutput {
	if len(entries) == 0 {
		return nil
	}
	out := make([]diffEntryOutput, 0, len(entries))
	for _, entry := range entries {
		out = append(out, diffEntryOutput{
			DocID:    entry.DocID,
			FromHash: entry.FromHash,
			ToHash:   entry.ToHash,
		})
	}
	return out
}

func filterChangedCollections(in []statsapp.DiffCollectionSummary) []statsapp.DiffCollectionSummary {
	out := make([]statsapp.DiffCollectionSummary, 0, len(in))
	for _, col := range in {
		if col.AddedTotal+col.ChangedAll+col.RemovedAll == 0 {
			continue
		}
		out = append(out, col)
	}
	return out
}

func shortHash(hash, fallback string) string {
	if hash == "" {
		return fallback
	}
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}
