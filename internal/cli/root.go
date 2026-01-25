package cli

import (
	"os"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/gitrepo"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
	"github.com/spf13/cobra"
)

type RootOptions struct {
	RepoPath     string
	JSONOutput   bool
	LogLevel     string
	LogFormat    string
	SignCommits  bool
	SignKey      string
	AutoSync     bool
	StreamLayout domain.StreamLayout
	HistoryMode  domain.HistoryMode
}

func newRootCmd() *cobra.Command {
	opts := &RootOptions{
		LogLevel:     envDefault("LEDGERDB_LOG_LEVEL", "info"),
		LogFormat:    envDefault("LEDGERDB_LOG_FORMAT", "text"),
		SignCommits:  envBoolDefault("LEDGERDB_GIT_SIGN", false),
		SignKey:      envDefault("LEDGERDB_GIT_SIGN_KEY", ""),
		AutoSync:     envBoolDefault("LEDGERDB_AUTO_SYNC", true),
		StreamLayout: domain.StreamLayoutFlat,
		HistoryMode:  domain.HistoryModeAppend,
	}
	cmd := &cobra.Command{
		Use:           "ledgerdb",
		Short:         "LedgerDB CLI",
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(opts.SignKey) != "" {
				opts.SignCommits = true
			}
			_, err := platform.ConfigureLogger(opts.LogLevel, opts.LogFormat, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			if cmd.Name() == "init" || cmd.Name() == "clone" {
				return nil
			}
			manifest, err := gitrepo.LoadManifest(opts.RepoPath)
			if err != nil {
				return err
			}
			opts.StreamLayout = manifest.StreamLayout
			opts.HistoryMode = manifest.HistoryMode
			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&opts.RepoPath, "repo", ".", "Path to the repository")
	cmd.PersistentFlags().BoolVar(&opts.JSONOutput, "json", false, "Emit JSON output")
	cmd.PersistentFlags().StringVar(&opts.LogLevel, "log-level", opts.LogLevel, "Log level (debug, info, warn, error)")
	cmd.PersistentFlags().StringVar(&opts.LogFormat, "log-format", opts.LogFormat, "Log format (text, json)")
	cmd.PersistentFlags().BoolVar(&opts.SignCommits, "sign", opts.SignCommits, "Sign git commits (requires gpg/ssh configuration)")
	cmd.PersistentFlags().StringVar(&opts.SignKey, "sign-key", opts.SignKey, "Signing key id for git commit signing")
	cmd.PersistentFlags().BoolVar(&opts.AutoSync, "sync", opts.AutoSync, "Auto-fetch before writes and auto-push after")

	cmd.AddCommand(
		newCloneCmd(opts),
		newInitCmd(opts),
		newStatusCmd(opts),
		newPushCmd(opts),
		newCollectionCmd(opts),
		newDocCmd(opts),
		newIndexCmd(opts),
		newInspectCmd(opts),
		newMaintenanceCmd(opts),
		newIntegrityCmd(opts),
	)

	return cmd
}

func envDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envBoolDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
