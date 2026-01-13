package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	collectionapp "github.com/codecompany/ledgerdb/internal/app/collection"
	docapp "github.com/codecompany/ledgerdb/internal/app/doc"
	indexapp "github.com/codecompany/ledgerdb/internal/app/index"
	inspectapp "github.com/codecompany/ledgerdb/internal/app/inspect"
	integrityapp "github.com/codecompany/ledgerdb/internal/app/integrity"
	maintenanceapp "github.com/codecompany/ledgerdb/internal/app/maintenance"
	repoapp "github.com/codecompany/ledgerdb/internal/app/repo"
	"github.com/codecompany/ledgerdb/internal/domain"
	"github.com/codecompany/ledgerdb/internal/infra/canonicaljson"
	"github.com/codecompany/ledgerdb/internal/infra/filesystem"
	"github.com/codecompany/ledgerdb/internal/infra/gitrepo"
	"github.com/codecompany/ledgerdb/internal/infra/hash"
	"github.com/codecompany/ledgerdb/internal/infra/ident"
	"github.com/codecompany/ledgerdb/internal/infra/jsonpatch"
	"github.com/codecompany/ledgerdb/internal/infra/schema"
	"github.com/codecompany/ledgerdb/internal/infra/sqliteindex"
	"github.com/codecompany/ledgerdb/internal/infra/txv3"
	"github.com/codecompany/ledgerdb/internal/platform"
	"github.com/spf13/cobra"
)

func newInitCmd(opts *RootOptions) *cobra.Command {
	var name string
	var layout string
	var historyMode string
	var remote string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a LedgerDB repository",
		RunE: func(cmd *cobra.Command, _ []string) error {
			parsedLayout, err := domain.ParseStreamLayout(layout)
			if err != nil {
				return err
			}
			parsedHistory, err := domain.ParseHistoryMode(historyMode)
			if err != nil {
				return err
			}
			service := repoapp.NewInitService(newGitStore(opts), platform.RealClock{})
			return service.Init(cmd.Context(), opts.RepoPath, repoapp.InitOptions{
				Name:         name,
				StreamLayout: parsedLayout,
				HistoryMode:  parsedHistory,
				RemoteURL:    remote,
			})
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Database name")
	cmd.Flags().StringVar(&layout, "layout", string(domain.DefaultStreamLayout), "Stream layout (flat, sharded)")
	cmd.Flags().StringVar(&historyMode, "history-mode", string(domain.DefaultHistoryMode), "History mode (append, amend)")
	cmd.Flags().StringVar(&remote, "remote", "", "Remote URL to configure as origin")
	return cmd
}

func newCloneCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clone <url> [path]",
		Short: "Clone a LedgerDB repository",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := ""
			if len(args) == 2 {
				path = args[1]
			}
			service := repoapp.NewCloneService(newGitStore(opts))
			return service.Clone(cmd.Context(), args[0], path)
		},
	}
	return cmd
}

func newStatusCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show repository status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			service := repoapp.NewStatusService(newGitStore(opts))
			status, err := service.Status(cmd.Context(), opts.RepoPath)
			if err != nil {
				return err
			}
			return writeStatus(cmd, status, opts.JSONOutput)
		},
	}
}

func newPushCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "push",
		Short: "Push local commits to origin",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := newGitStore(opts)
			return autoPush(cmd, opts, store)
		},
	}
}

func newCollectionCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "collection",
		Short: "Manage collections and schemas",
		RunE:  runHelp,
	}
	cmd.AddCommand(newCollectionApplyCmd(opts))
	return cmd
}

func newCollectionApplyCmd(opts *RootOptions) *cobra.Command {
	var schemaPath string
	var indexes string
	cmd := &cobra.Command{
		Use:   "apply <name>",
		Short: "Create or update a collection schema",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store := newGitStore(opts)
			service := collectionapp.NewService(store, filesystem.SchemaSource{}, schema.JSONSchemaValidator{})
			parsedIndexes := parseCommaList(indexes)
			return runWithAutoSync(cmd, opts, store, func() error {
				return service.Apply(cmd.Context(), opts.RepoPath, args[0], schemaPath, parsedIndexes)
			})
		},
	}
	cmd.Flags().StringVar(&schemaPath, "schema", "", "Path to JSON schema")
	cmd.Flags().StringVar(&indexes, "indexes", "", "Comma-separated index fields")
	if err := cmd.MarkFlagRequired("schema"); err != nil {
		return cmd
	}
	return cmd
}

func newDocCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doc",
		Short: "Operate on documents",
		RunE:  runHelp,
	}
	cmd.AddCommand(
		newDocPutCmd(opts),
		newDocGetCmd(opts),
		newDocPatchCmd(opts),
		newDocDeleteCmd(opts),
		newDocRevertCmd(opts),
		newDocLogCmd(opts),
	)
	return cmd
}

func newDocPutCmd(opts *RootOptions) *cobra.Command {
	var payload string
	var payloadFile string
	cmd := &cobra.Command{
		Use:   "put <collection> <doc_id>",
		Short: "Write a document snapshot",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := readJSONInput("payload", payload, payloadFile)
			if err != nil {
				return err
			}

			idGen := ident.NewULIDGenerator()
			store := newGitStore(opts)
			service := docapp.NewPutService(
				store,
				canonicaljson.Canonicalizer{},
				txv3.Encoder{},
				hash.SHA256{},
				platform.RealClock{},
				idGen,
				opts.StreamLayout,
				opts.HistoryMode,
			)

			return runWithAutoSync(cmd, opts, store, func() error {
				result, err := service.Put(cmd.Context(), opts.RepoPath, args[0], args[1], data)
				if err != nil {
					return err
				}
				return writePutResult(cmd, result, opts.JSONOutput)
			})
		},
	}

	cmd.Flags().StringVar(&payload, "payload", "", "Inline JSON document payload")
	cmd.Flags().StringVar(&payloadFile, "file", "", "Path to JSON document payload")
	return cmd
}

func newDocGetCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "get <collection> <doc_id>",
		Short: "Read a document state",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			service := docapp.NewGetService(newGitStore(opts), txv3.Decoder{}, hash.SHA256{}, jsonpatch.Patcher{}, opts.StreamLayout)
			result, err := service.Get(cmd.Context(), opts.RepoPath, args[0], args[1])
			if err != nil {
				return err
			}
			return writeGetResult(cmd, result, opts.JSONOutput)
		},
	}
}

func newDocPatchCmd(opts *RootOptions) *cobra.Command {
	var ops string
	var opsFile string
	cmd := &cobra.Command{
		Use:   "patch <collection> <doc_id>",
		Short: "Apply a JSON Patch delta",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := readJSONInput("ops", ops, opsFile)
			if err != nil {
				return err
			}

			idGen := ident.NewULIDGenerator()
			store := newGitStore(opts)
			service := docapp.NewPatchService(
				store,
				store,
				canonicaljson.Canonicalizer{},
				txv3.Encoder{},
				txv3.Decoder{},
				jsonpatch.Patcher{},
				hash.SHA256{},
				platform.RealClock{},
				idGen,
				opts.StreamLayout,
				opts.HistoryMode,
			)
			return runWithAutoSync(cmd, opts, store, func() error {
				result, err := service.Patch(cmd.Context(), opts.RepoPath, args[0], args[1], data)
				if err != nil {
					return err
				}
				return writePutResult(cmd, result, opts.JSONOutput)
			})
		},
	}
	cmd.Flags().StringVar(&ops, "ops", "", "Inline JSON Patch operations")
	cmd.Flags().StringVar(&opsFile, "file", "", "Path to JSON Patch operations")
	return cmd
}

func newDocDeleteCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <collection> <doc_id>",
		Short: "Delete a document (tombstone)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			idGen := ident.NewULIDGenerator()
			store := newGitStore(opts)
			service := docapp.NewDeleteService(
				store,
				store,
				txv3.Encoder{},
				txv3.Decoder{},
				hash.SHA256{},
				platform.RealClock{},
				idGen,
				opts.StreamLayout,
				opts.HistoryMode,
			)
			return runWithAutoSync(cmd, opts, store, func() error {
				result, err := service.Delete(cmd.Context(), opts.RepoPath, args[0], args[1])
				if err != nil {
					return err
				}
				return writePutResult(cmd, result, opts.JSONOutput)
			})
		},
	}
}

func newDocRevertCmd(opts *RootOptions) *cobra.Command {
	var txID string
	var txHash string
	cmd := &cobra.Command{
		Use:   "revert <collection> <doc_id>",
		Short: "Revert a document to a previous transaction",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			idGen := ident.NewULIDGenerator()
			store := newGitStore(opts)
			service := docapp.NewRevertService(
				store,
				store,
				canonicaljson.Canonicalizer{},
				txv3.Encoder{},
				txv3.Decoder{},
				jsonpatch.Patcher{},
				hash.SHA256{},
				platform.RealClock{},
				idGen,
				opts.StreamLayout,
				opts.HistoryMode,
			)
			return runWithAutoSync(cmd, opts, store, func() error {
				result, err := service.Revert(cmd.Context(), opts.RepoPath, args[0], args[1], docapp.RevertOptions{
					TxID:   txID,
					TxHash: txHash,
				})
				if err != nil {
					return err
				}
				return writePutResult(cmd, result, opts.JSONOutput)
			})
		},
	}
	cmd.Flags().StringVar(&txID, "tx-id", "", "Target transaction ID from doc log")
	cmd.Flags().StringVar(&txHash, "tx-hash", "", "Target transaction hash from doc log")
	return cmd
}

func newDocLogCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "log <collection> <doc_id>",
		Short: "Show document history",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			service := docapp.NewLogService(newGitStore(opts), txv3.Decoder{}, hash.SHA256{}, opts.StreamLayout)
			entries, err := service.Log(cmd.Context(), opts.RepoPath, args[0], args[1])
			if err != nil {
				return err
			}
			return writeLogResult(cmd, entries, opts.JSONOutput)
		},
	}
}

func newIndexCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "External index operations",
		RunE:  runHelp,
	}
	cmd.AddCommand(newIndexSyncCmd(opts), newIndexWatchCmd(opts))
	return cmd
}

func newIndexSyncCmd(opts *RootOptions) *cobra.Command {
	var dbPath string
	var fetch bool
	var batchCommits int
	var fast bool
	var mode string
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync the SQLite index from the ledger",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if batchCommits <= 0 {
				return indexapp.ErrInvalidBatchCommits
			}
			parsedMode, err := indexapp.ParseMode(mode)
			if err != nil {
				return err
			}
			store, err := sqliteindex.OpenWithOptions(dbPath, sqliteindex.OpenOptions{Fast: fast})
			if err != nil {
				return err
			}
			defer func() {
				_ = store.Close()
			}()

			gitStore := newGitStore(opts)
			service := indexapp.NewSyncService(
				gitStore,
				gitStore,
				store,
				canonicaljson.Canonicalizer{},
				txv3.Decoder{},
				jsonpatch.Patcher{},
				hash.SHA256{},
			)

			var result indexapp.SyncResult
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Syncing index")
			err = withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				var err error
				result, err = service.Sync(cmd.Context(), opts.RepoPath, indexapp.SyncOptions{
					Fetch:        fetch,
					AllowReset:   opts.HistoryMode == domain.HistoryModeAmend,
					BatchCommits: batchCommits,
					Mode:         parsedMode,
				})
				return err
			})
			if err != nil {
				return err
			}
			return writeIndexSyncResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "Path to SQLite index database")
	cmd.Flags().BoolVar(&fetch, "fetch", true, "Fetch remote updates before syncing")
	cmd.Flags().IntVar(&batchCommits, "batch-commits", 1, "Commits per SQLite transaction (>=1)")
	cmd.Flags().BoolVar(&fast, "fast", false, "Relax SQLite durability for faster indexing")
	cmd.Flags().StringVar(&mode, "mode", string(indexapp.ModeState), "Index source (history, state)")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		return cmd
	}
	return cmd
}

func newIndexWatchCmd(opts *RootOptions) *cobra.Command {
	var dbPath string
	var fetch bool
	var interval time.Duration
	var onlyChanges bool
	var once bool
	var jitter time.Duration
	var quiet bool
	var batchCommits int
	var fast bool
	var mode string
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Continuously sync the SQLite index",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !once && interval <= 0 {
				return indexapp.ErrInvalidInterval
			}
			if jitter < 0 {
				return indexapp.ErrInvalidJitter
			}
			if batchCommits <= 0 {
				return indexapp.ErrInvalidBatchCommits
			}
			parsedMode, err := indexapp.ParseMode(mode)
			if err != nil {
				return err
			}

			store, err := sqliteindex.OpenWithOptions(dbPath, sqliteindex.OpenOptions{Fast: fast})
			if err != nil {
				return err
			}
			defer func() {
				_ = store.Close()
			}()

			gitStore := newGitStore(opts)
			service := indexapp.NewSyncService(
				gitStore,
				gitStore,
				store,
				canonicaljson.Canonicalizer{},
				txv3.Decoder{},
				jsonpatch.Patcher{},
				hash.SHA256{},
			)

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput) && !quiet
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Syncing index")

			for {
				var result indexapp.SyncResult
				err = withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
					var err error
					result, err = service.Sync(cmd.Context(), opts.RepoPath, indexapp.SyncOptions{
						Fetch:        fetch,
						AllowReset:   opts.HistoryMode == domain.HistoryModeAmend,
						BatchCommits: batchCommits,
						Mode:         parsedMode,
					})
					return err
				})
				if err != nil {
					return err
				}
				if !quiet && (!onlyChanges || hasIndexChanges(result)) {
					if err := writeIndexSyncResult(cmd, result, opts.JSONOutput); err != nil {
						return err
					}
				}
				if once {
					return nil
				}

				wait := interval
				if jitter > 0 {
					wait += time.Duration(rng.Int63n(int64(jitter)))
				}
				timer := time.NewTimer(wait)
				select {
				case <-cmd.Context().Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "Path to SQLite index database")
	cmd.Flags().BoolVar(&fetch, "fetch", true, "Fetch remote updates before syncing")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Polling interval for sync")
	cmd.Flags().BoolVar(&onlyChanges, "only-changes", false, "Only emit output when new data is applied")
	cmd.Flags().BoolVar(&once, "once", false, "Run a single sync and exit")
	cmd.Flags().DurationVar(&jitter, "jitter", 0, "Add random jitter to the polling interval")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "Suppress output (errors are still reported)")
	cmd.Flags().IntVar(&batchCommits, "batch-commits", 1, "Commits per SQLite transaction (>=1)")
	cmd.Flags().BoolVar(&fast, "fast", false, "Relax SQLite durability for faster indexing")
	cmd.Flags().StringVar(&mode, "mode", string(indexapp.ModeState), "Index source (history, state)")
	if err := cmd.MarkFlagRequired("db"); err != nil {
		return cmd
	}
	return cmd
}

func newMaintenanceCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Repository maintenance operations",
		RunE:  runHelp,
	}
	cmd.AddCommand(newMaintenanceGCCmd(opts), newMaintenanceSnapshotCmd(opts))
	return cmd
}

func newMaintenanceGCCmd(opts *RootOptions) *cobra.Command {
	var prune string
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Run git garbage collection",
		RunE: func(cmd *cobra.Command, _ []string) error {
			service := maintenanceapp.NewGCService(newGitStore(opts))
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Running git gc")
			err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				return service.GC(cmd.Context(), opts.RepoPath, maintenanceapp.GCOptions{Prune: prune})
			})
			if err != nil {
				return err
			}
			return writeGCResult(cmd, prune, opts.JSONOutput)
		},
	}
	cmd.Flags().StringVar(&prune, "prune", "now", "Prune unreachable objects (git gc --prune)")
	return cmd
}

func newMaintenanceSnapshotCmd(opts *RootOptions) *cobra.Command {
	var threshold int
	var max int
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Create snapshot txs for long patch chains",
		RunE: func(cmd *cobra.Command, _ []string) error {
			idGen := ident.NewULIDGenerator()
			store := newGitStore(opts)
			service := maintenanceapp.NewSnapshotService(
				store,
				store,
				store,
				canonicaljson.Canonicalizer{},
				txv3.Encoder{},
				txv3.Decoder{},
				jsonpatch.Patcher{},
				hash.SHA256{},
				platform.RealClock{},
				idGen,
				opts.HistoryMode,
			)
			return runWithAutoSync(cmd, opts, store, func() error {
				var result maintenanceapp.SnapshotResult
				spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
				label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Creating snapshots")
				err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
					var err error
					result, err = service.Snapshot(cmd.Context(), opts.RepoPath, maintenanceapp.SnapshotOptions{
						Threshold: threshold,
						Max:       max,
						DryRun:    dryRun,
					})
					return err
				})
				if err != nil {
					return err
				}
				return writeSnapshotResult(cmd, result, opts.JSONOutput)
			})
		},
	}
	cmd.Flags().IntVar(&threshold, "threshold", 50, "Minimum tx chain length to snapshot")
	cmd.Flags().IntVar(&max, "max", 0, "Maximum snapshots to create (0 = unlimited)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Report snapshot candidates without writing")
	return cmd
}

func newIntegrityCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "integrity",
		Short: "Verify ledger integrity",
		RunE:  runHelp,
	}
	cmd.AddCommand(newIntegrityVerifyCmd(opts))
	return cmd
}

func newInspectCmd(opts *RootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Decode internal blobs",
		RunE:  runHelp,
	}
	cmd.AddCommand(newInspectBlobCmd(opts))
	return cmd
}

func newInspectBlobCmd(opts *RootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "blob <hash>",
		Short: "Decode a transaction blob by git object hash",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			service := inspectapp.NewService(newGitStore(opts), txv3.Decoder{}, hash.SHA256{})
			result, err := service.InspectBlob(cmd.Context(), opts.RepoPath, args[0])
			if err != nil {
				return err
			}
			return writeInspectResult(cmd, result, opts.JSONOutput)
		},
	}
}

func newIntegrityVerifyCmd(opts *RootOptions) *cobra.Command {
	var deep bool
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify hash chains and report corruption",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store := newGitStore(opts)
			service := integrityapp.NewVerifyService(
				store,
				store,
				txv3.Decoder{},
				hash.SHA256{},
				jsonpatch.Patcher{},
			)
			var result integrityapp.VerifyResult
			spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
			label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).accent("Verifying integrity")
			err := withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
				var err error
				result, err = service.Verify(cmd.Context(), opts.RepoPath, integrityapp.VerifyOptions{Deep: deep})
				return err
			})
			if err != nil {
				return err
			}
			return writeIntegrityResult(cmd, result, opts.JSONOutput)
		},
	}
	cmd.Flags().BoolVar(&deep, "deep", false, "Rebuild documents by applying patches")
	return cmd
}

type putOutput struct {
	Commit string `json:"commit"`
	TxHash string `json:"tx_hash"`
	TxID   string `json:"tx_id"`
}

type getOutput struct {
	Doc    json.RawMessage `json:"doc"`
	TxHash string          `json:"tx_hash,omitempty"`
	TxID   string          `json:"tx_id,omitempty"`
	Op     string          `json:"op,omitempty"`
}

type logOutput struct {
	Entries []logEntryOutput `json:"entries"`
}

type logEntryOutput struct {
	TxHash     string `json:"tx_hash"`
	TxID       string `json:"tx_id"`
	ParentHash string `json:"parent_hash,omitempty"`
	Timestamp  int64  `json:"timestamp"`
	Op         string `json:"op"`
}

type integrityOutput struct {
	Streams int                    `json:"streams"`
	Valid   int                    `json:"valid"`
	Issues  []integrityIssueOutput `json:"issues,omitempty"`
}

type integrityIssueOutput struct {
	StreamPath string `json:"stream_path"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type snapshotOutput struct {
	Streams     int                   `json:"streams"`
	Processed   int                   `json:"processed"`
	Snapshotted int                   `json:"snapshotted"`
	Planned     int                   `json:"planned"`
	Skipped     int                   `json:"skipped"`
	Truncated   bool                  `json:"truncated"`
	DryRun      bool                  `json:"dry_run"`
	Issues      []snapshotIssueOutput `json:"issues,omitempty"`
}

type snapshotIssueOutput struct {
	StreamPath string `json:"stream_path"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type gcOutput struct {
	Status string `json:"status"`
	Prune  string `json:"prune,omitempty"`
}

type inspectOutput struct {
	ObjectHash    string          `json:"object_hash"`
	TxHash        string          `json:"tx_hash"`
	TxID          string          `json:"tx_id"`
	Timestamp     int64           `json:"timestamp"`
	Collection    string          `json:"collection"`
	DocID         string          `json:"doc_id"`
	Op            string          `json:"op"`
	ParentHash    string          `json:"parent_hash,omitempty"`
	SchemaVersion string          `json:"schema_version,omitempty"`
	Snapshot      json.RawMessage `json:"snapshot,omitempty"`
	Patch         json.RawMessage `json:"patch,omitempty"`
}

type indexSyncOutput struct {
	Reset        bool   `json:"reset"`
	Fetched      bool   `json:"fetched"`
	Commits      int    `json:"commits"`
	TxsApplied   int    `json:"txs_applied"`
	DocsUpserted int    `json:"docs_upserted"`
	DocsDeleted  int    `json:"docs_deleted"`
	Collections  int    `json:"collections"`
	LastCommit   string `json:"last_commit,omitempty"`
}

type statusOutput struct {
	Path     string          `json:"path"`
	Bare     bool            `json:"bare"`
	Head     string          `json:"head,omitempty"`
	Manifest *manifestOutput `json:"manifest,omitempty"`
}

type manifestOutput struct {
	Version      int    `json:"version"`
	Name         string `json:"name"`
	StreamLayout string `json:"stream_layout,omitempty"`
	HistoryMode  string `json:"history_mode,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
}

func writeStatus(cmd *cobra.Command, status domain.RepoStatus, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		output := statusOutput{
			Path: status.Path,
			Bare: status.IsBare,
		}
		if status.HasHead {
			output.Head = status.HeadHash
		}
		if status.HasManifest {
			manifest := manifestOutput{
				Version:      status.Manifest.Version,
				Name:         status.Manifest.Name,
				StreamLayout: string(status.Manifest.StreamLayout),
				HistoryMode:  string(status.Manifest.HistoryMode),
			}
			if !status.Manifest.CreatedAt.IsZero() {
				manifest.CreatedAt = status.Manifest.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00")
			}
			output.Manifest = &manifest
		}

		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Path", status.Path); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Bare", fmt.Sprintf("%t", status.IsBare)); err != nil {
		return err
	}
	if status.HasHead {
		if err := writeKV(out, ui, "Head", status.HeadHash); err != nil {
			return err
		}
	} else {
		if err := writeKV(out, ui, "Head", ui.dim("(none)")); err != nil {
			return err
		}
	}
	if status.HasManifest {
		manifest := fmt.Sprintf("%s (v%d)", status.Manifest.Name, status.Manifest.Version)
		if err := writeKV(out, ui, "Manifest", manifest); err != nil {
			return err
		}
		if !status.Manifest.CreatedAt.IsZero() {
			if err := writeKV(out, ui, "Manifest Created", status.Manifest.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00")); err != nil {
				return err
			}
		}
		if status.Manifest.StreamLayout != "" {
			if err := writeKV(out, ui, "Stream Layout", string(status.Manifest.StreamLayout)); err != nil {
				return err
			}
		}
		if status.Manifest.HistoryMode != "" {
			if err := writeKV(out, ui, "History Mode", string(status.Manifest.HistoryMode)); err != nil {
				return err
			}
		}
	} else {
		if err := writeKV(out, ui, "Manifest", ui.dim("(missing)")); err != nil {
			return err
		}
	}
	return nil
}

func writePutResult(cmd *cobra.Command, result docapp.PutResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := putOutput{
			Commit: result.CommitHash,
			TxHash: result.TxHash,
			TxID:   result.TxID,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Commit", result.CommitHash); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Tx Hash", result.TxHash); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Tx ID", result.TxID); err != nil {
		return err
	}
	return nil
}

func writeGetResult(cmd *cobra.Command, result docapp.GetResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := getOutput{
			Doc:    json.RawMessage(result.Payload),
			TxHash: result.TxHash,
			TxID:   result.TxID,
			Op:     result.Op.String(),
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	if _, err := out.Write(result.Payload); err != nil {
		return err
	}
	if len(result.Payload) > 0 && result.Payload[len(result.Payload)-1] != '\n' {
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	return nil
}

func writeLogResult(cmd *cobra.Command, entries []docapp.LogEntry, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := logOutput{Entries: make([]logEntryOutput, 0, len(entries))}
		for _, entry := range entries {
			payload.Entries = append(payload.Entries, logEntryOutput{
				TxHash:     entry.TxHash,
				TxID:       entry.TxID,
				ParentHash: entry.ParentHash,
				Timestamp:  entry.Timestamp,
				Op:         entry.Op.String(),
			})
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	for _, entry := range entries {
		op := colorOp(ui, entry.Op.String())
		if _, err := fmt.Fprintf(out, "%s %s %s %d %s\n", entry.TxHash, entry.TxID, entry.ParentHash, entry.Timestamp, op); err != nil {
			return err
		}
	}
	return nil
}

func writeIntegrityResult(cmd *cobra.Command, result integrityapp.VerifyResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := integrityOutput{
			Streams: result.Streams,
			Valid:   result.Valid,
			Issues:  make([]integrityIssueOutput, 0, len(result.Issues)),
		}
		for _, issue := range result.Issues {
			payload.Issues = append(payload.Issues, integrityIssueOutput{
				StreamPath: issue.StreamPath,
				Code:       issue.Code,
				Message:    issue.Message,
			})
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if result.Streams > 0 {
		ratio := float64(result.Valid) / float64(result.Streams)
		if _, err := fmt.Fprintf(out, "%s %s %d/%d\n", ui.key("Integrity"), ui.bar(24, ratio), result.Valid, result.Streams); err != nil {
			return err
		}
	}

	if len(result.Issues) == 0 {
		_, err := fmt.Fprintf(out, "%s: %d stream(s) verified\n", ui.ok("OK"), result.Streams)
		return err
	}

	if _, err := fmt.Fprintf(out, "%s %d stream(s): %d ok, %d issue(s)\n", ui.warn("Issues"), result.Streams, result.Valid, len(result.Issues)); err != nil {
		return err
	}
	for _, issue := range result.Issues {
		code := issue.Code
		if ui.color {
			code = ui.err(code)
		}
		if _, err := fmt.Fprintf(out, "- %s [%s] %s\n", issue.StreamPath, code, issue.Message); err != nil {
			return err
		}
	}
	return nil
}

func writeSnapshotResult(cmd *cobra.Command, result maintenanceapp.SnapshotResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := snapshotOutput{
			Streams:     result.Streams,
			Processed:   result.Processed,
			Snapshotted: result.Snapshotted,
			Planned:     result.Planned,
			Skipped:     result.Skipped,
			Truncated:   result.Truncated,
			DryRun:      result.DryRun,
			Issues:      make([]snapshotIssueOutput, 0, len(result.Issues)),
		}
		for _, issue := range result.Issues {
			payload.Issues = append(payload.Issues, snapshotIssueOutput{
				StreamPath: issue.StreamPath,
				Code:       issue.Code,
				Message:    issue.Message,
			})
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if result.Streams > 0 {
		ratio := float64(result.Processed) / float64(result.Streams)
		if _, err := fmt.Fprintf(out, "%s %s %d/%d\n", ui.key("Progress"), ui.bar(24, ratio), result.Processed, result.Streams); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(out, "Streams: %d, Processed: %d, Snapshotted: %d, Planned: %d, Skipped: %d, Issues: %d\n",
		result.Streams, result.Processed, result.Snapshotted, result.Planned, result.Skipped, len(result.Issues)); err != nil {
		return err
	}
	if result.DryRun {
		if _, err := fmt.Fprintln(out, "Dry Run: true"); err != nil {
			return err
		}
	}
	if result.Truncated {
		if _, err := fmt.Fprintln(out, "Truncated: true"); err != nil {
			return err
		}
	}
	for _, issue := range result.Issues {
		code := issue.Code
		if ui.color {
			code = ui.err(code)
		}
		if _, err := fmt.Fprintf(out, "- %s [%s] %s\n", issue.StreamPath, code, issue.Message); err != nil {
			return err
		}
	}
	return nil
}

func writeGCResult(cmd *cobra.Command, prune string, asJSON bool) error {
	out := cmd.OutOrStdout()
	prune = strings.TrimSpace(prune)
	if asJSON {
		payload := gcOutput{
			Status: "ok",
			Prune:  prune,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}
	if prune == "" {
		ui := newRenderer(out, asJSON)
		_, err := fmt.Fprintln(out, ui.ok("GC complete"))
		return err
	}
	ui := newRenderer(out, asJSON)
	_, err := fmt.Fprintf(out, "%s (prune=%s)\n", ui.ok("GC complete"), prune)
	return err
}

func writeInspectResult(cmd *cobra.Command, result inspectapp.BlobResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := inspectOutput{
			ObjectHash:    result.ObjectHash,
			TxHash:        result.TxHash,
			TxID:          result.Tx.TxID,
			Timestamp:     result.Tx.Timestamp,
			Collection:    result.Tx.Collection,
			DocID:         result.Tx.DocID,
			Op:            result.Tx.Op.String(),
			ParentHash:    result.Tx.ParentHash,
			SchemaVersion: result.Tx.SchemaVersion,
			Snapshot:      rawJSON(result.Tx.Snapshot),
			Patch:         rawJSON(result.Tx.Patch),
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	if err := writeKV(out, ui, "Object Hash", result.ObjectHash); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Tx Hash", result.TxHash); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Tx ID", result.Tx.TxID); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Timestamp", fmt.Sprintf("%d", result.Tx.Timestamp)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Collection", result.Tx.Collection); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Doc ID", result.Tx.DocID); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Op", colorOp(ui, result.Tx.Op.String())); err != nil {
		return err
	}
	if result.Tx.ParentHash != "" {
		if err := writeKV(out, ui, "Parent", result.Tx.ParentHash); err != nil {
			return err
		}
	}
	if result.Tx.SchemaVersion != "" {
		if err := writeKV(out, ui, "Schema Version", result.Tx.SchemaVersion); err != nil {
			return err
		}
	}
	if len(result.Tx.Snapshot) > 0 {
		if err := writeKV(out, ui, "Snapshot", string(result.Tx.Snapshot)); err != nil {
			return err
		}
	}
	if len(result.Tx.Patch) > 0 {
		if err := writeKV(out, ui, "Patch", string(result.Tx.Patch)); err != nil {
			return err
		}
	}
	return nil
}

func writeIndexSyncResult(cmd *cobra.Command, result indexapp.SyncResult, asJSON bool) error {
	out := cmd.OutOrStdout()
	if asJSON {
		payload := indexSyncOutput{
			Reset:        result.Reset,
			Fetched:      result.Fetched,
			Commits:      result.Commits,
			TxsApplied:   result.TxsApplied,
			DocsUpserted: result.DocsUpserted,
			DocsDeleted:  result.DocsDeleted,
			Collections:  result.Collections,
			LastCommit:   result.LastCommit,
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(out, asJSON)
	state := ui.dim("idle")
	if result.TxsApplied > 0 || result.Commits > 0 {
		state = ui.ok("applied")
	}
	if err := writeKV(out, ui, "Status", state); err != nil {
		return err
	}
	if result.Reset {
		if err := writeKV(out, ui, "Reset", ui.warn("true")); err != nil {
			return err
		}
	}
	if err := writeKV(out, ui, "Fetched", fmt.Sprintf("%t", result.Fetched)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Commits", fmt.Sprintf("%d", result.Commits)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Txs Applied", fmt.Sprintf("%d", result.TxsApplied)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Docs Upserted", fmt.Sprintf("%d", result.DocsUpserted)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Docs Deleted", fmt.Sprintf("%d", result.DocsDeleted)); err != nil {
		return err
	}
	if err := writeKV(out, ui, "Collections", fmt.Sprintf("%d", result.Collections)); err != nil {
		return err
	}
	if result.LastCommit == "" {
		if err := writeKV(out, ui, "Last Commit", ui.dim("(none)")); err != nil {
			return err
		}
		return nil
	}
	return writeKV(out, ui, "Last Commit", result.LastCommit)
}

func hasIndexChanges(result indexapp.SyncResult) bool {
	return result.Commits > 0 || result.TxsApplied > 0 || result.DocsUpserted > 0 || result.DocsDeleted > 0
}

func newGitStore(opts *RootOptions) *gitrepo.Store {
	return gitrepo.NewStoreWithOptions(gitrepo.StoreOptions{
		SignCommits: opts.SignCommits,
		SignKey:     opts.SignKey,
		HistoryMode: opts.HistoryMode,
	})
}

func runWithAutoSync(cmd *cobra.Command, opts *RootOptions, store *gitrepo.Store, fn func() error) error {
	if !opts.AutoSync {
		return fn()
	}
	if err := autoFetch(cmd, opts, store); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return autoPush(cmd, opts, store)
}

func autoFetch(cmd *cobra.Command, opts *RootOptions, store *gitrepo.Store) error {
	spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
	label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).dim("Fetching origin")
	return withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
		return store.Fetch(cmd.Context(), opts.RepoPath)
	})
}

func autoPush(cmd *cobra.Command, opts *RootOptions, store *gitrepo.Store) error {
	spin := spinnerEnabled(cmd.ErrOrStderr(), opts.JSONOutput)
	label := newRenderer(cmd.ErrOrStderr(), opts.JSONOutput).dim("Pushing origin")
	return withSpinner(cmd.Context(), cmd.ErrOrStderr(), spin, label, func() error {
		return store.Push(cmd.Context(), opts.RepoPath)
	})
}

func writeKV(out io.Writer, ui renderer, key, value string) error {
	_, err := fmt.Fprintf(out, "%s: %s\n", ui.key(key), value)
	return err
}

func colorOp(ui renderer, op string) string {
	switch op {
	case "put":
		return ui.ok(op)
	case "patch":
		return ui.warn(op)
	case "merge":
		return ui.accent(op)
	case "delete":
		return ui.err(op)
	default:
		return ui.dim(op)
	}
}

func rawJSON(data []byte) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	return json.RawMessage(data)
}

func parseCommaList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func readJSONInput(label, inline, filePath string) ([]byte, error) {
	inline = strings.TrimSpace(inline)
	filePath = strings.TrimSpace(filePath)
	if inline != "" && filePath != "" {
		return nil, fmt.Errorf("use either --%s or --file, not both", label)
	}
	if inline == "" && filePath == "" {
		return nil, fmt.Errorf("%s is required (use --%s or --file)", label, label)
	}
	if inline != "" {
		return []byte(inline), nil
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", label, err)
	}
	return data, nil
}

func runHelp(cmd *cobra.Command, _ []string) error {
	return cmd.Help()
}
