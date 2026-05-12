package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

func newQueryCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query the SQLite sidecar index",
		RunE:  runHelp,
	}
	cmd.AddCommand(newQueryExplainCmd(opts))
	return cmd
}

func newQueryExplainCmd(opts *RootOptions) *cobra.Command {
	var dbPath string
	cmd := &cobra.Command{
		Use:   "explain <sql>",
		Short: "Run EXPLAIN QUERY PLAN against the SQLite sidecar",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := resolveSidecarPath(dbPath, opts.RepoPath)
			plan, err := explainQueryPlan(cmd.Context(), path, args[0])
			if err != nil {
				return err
			}
			return writeExplainResult(cmd, plan, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "Path to SQLite index database (defaults to <repo>/index.db)")
	return cmd
}

// explainPlanRow mirrors the EXPLAIN QUERY PLAN output columns.
type explainPlanRow struct {
	ID     int    `json:"id"`
	Parent int    `json:"parent"`
	Detail string `json:"detail"`
}

func resolveSidecarPath(flag, repoPath string) string {
	if strings.TrimSpace(flag) != "" {
		return flag
	}
	if strings.TrimSpace(repoPath) == "" {
		return "index.db"
	}
	return filepath.Join(repoPath, "index.db")
}

func explainQueryPlan(ctx context.Context, dbPath, query string) ([]explainPlanRow, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("sql query is required")
	}
	if strings.TrimSpace(dbPath) == "" {
		return nil, fmt.Errorf("sqlite path required")
	}

	// Open read-only to avoid mutating the sidecar.
	dsn := "file:" + dbPath + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	stmt := "EXPLAIN QUERY PLAN " + query
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("explain query plan: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var plan []explainPlanRow
	for rows.Next() {
		var row explainPlanRow
		var notused int
		if err := rows.Scan(&row.ID, &row.Parent, &notused, &row.Detail); err != nil {
			return nil, fmt.Errorf("scan plan row: %w", err)
		}
		plan = append(plan, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate plan rows: %w", err)
	}
	return plan, nil
}

func writeExplainResult(cmd *cobra.Command, plan []explainPlanRow, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if plan == nil {
			plan = []explainPlanRow{}
		}
		return encoder.Encode(plan)
	}

	if len(plan) == 0 {
		_, err := fmt.Fprintln(out, "(empty plan)")
		return err
	}

	ui := newRenderer(out, asJSON)
	indent := childIndentByParent(plan)
	if _, err := fmt.Fprintf(out, "%s\n", ui.key("QUERY PLAN")); err != nil {
		return err
	}
	for _, row := range plan {
		prefix := strings.Repeat("  ", indent[row.ID])
		if _, err := fmt.Fprintf(out, "%s- %s\n", prefix, row.Detail); err != nil {
			return err
		}
	}
	return nil
}

// childIndentByParent computes nesting depth for each row id based on parent links.
func childIndentByParent(plan []explainPlanRow) map[int]int {
	depths := make(map[int]int, len(plan))
	for _, row := range plan {
		if row.Parent == 0 {
			depths[row.ID] = 0
			continue
		}
		parentDepth, ok := depths[row.Parent]
		if !ok {
			parentDepth = 0
		}
		depths[row.ID] = parentDepth + 1
	}
	return depths
}
