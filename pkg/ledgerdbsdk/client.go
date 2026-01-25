package ledgerdbsdk

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	indexapp "github.com/osvaldoandrade/ledgerdb/internal/app/index"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/gitrepo"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/sqliteindex"
)

// Client provides direct access to LedgerDB core services.
type Client struct {
	cfg         Config
	manifest    domain.Manifest
	layout      domain.StreamLayout
	historyMode domain.HistoryMode
	store       *gitrepo.Store

	mu         sync.Mutex
	indexStore *sqliteindex.Store
	db         *sql.DB

	watchMu      sync.Mutex
	watchCancel  context.CancelFunc
	watchErr     chan error
	watchResults chan IndexSyncResult
}

// New creates a client without opening the SQLite index or starting a watch.
func New(cfg Config) (*Client, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	manifest, manifestExists, err := loadManifest(normalized.RepoPath)
	if err != nil {
		return nil, err
	}

	layout, historyMode, err := resolveManifestOverrides(normalized, manifest, manifestExists)
	if err != nil {
		return nil, err
	}
	normalized.StreamLayout = StreamLayout(layout)
	normalized.HistoryMode = HistoryMode(historyMode)

	store := gitrepo.NewStoreWithOptions(gitrepo.StoreOptions{
		SignCommits: normalized.SignCommits,
		SignKey:     normalized.SignKey,
		HistoryMode: historyMode,
	})

	return &Client{
		cfg:         normalized,
		manifest:    manifest,
		layout:      layout,
		historyMode: historyMode,
		store:       store,
	}, nil
}

// Open creates a client, opens the SQLite index, and starts watch if enabled.
func Open(ctx context.Context, cfg Config) (*Client, error) {
	client, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if err := client.OpenIndex(ctx); err != nil {
		return nil, err
	}
	if client.cfg.AutoWatch {
		if err := client.StartIndexWatch(ctx); err != nil {
			_ = client.Close()
			return nil, err
		}
	}
	return client, nil
}

// Close stops the index watch (if running) and closes SQLite.
func (c *Client) Close() error {
	_ = c.StopIndexWatch()

	c.mu.Lock()
	indexStore := c.indexStore
	c.indexStore = nil
	c.db = nil
	c.mu.Unlock()

	if indexStore != nil {
		return indexStore.Close()
	}
	return nil
}

// RepoPath returns the configured repository path.
func (c *Client) RepoPath() string {
	return c.cfg.RepoPath
}

func (c *Client) syncOptions() (indexapp.SyncOptions, error) {
	mode, err := toDomainIndexMode(c.cfg.Index.Mode)
	if err != nil {
		return indexapp.SyncOptions{}, err
	}
	return indexapp.SyncOptions{
		Fetch:        c.cfg.Index.Fetch,
		AllowReset:   c.historyMode == domain.HistoryModeAmend,
		BatchCommits: c.cfg.Index.BatchCommits,
		Mode:         mode,
	}, nil
}

func (c *Client) indexDB() (*sql.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil, ErrIndexNotOpen
	}
	return c.db, nil
}

func (c *Client) ensureIndexStore() (*sqliteindex.Store, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexStore == nil {
		return nil, ErrIndexNotOpen
	}
	return c.indexStore, nil
}

func loadManifest(repoPath string) (domain.Manifest, bool, error) {
	manifestPath := filepath.Join(repoPath, "db.yaml")
	_, err := os.Stat(manifestPath)
	manifestExists := err == nil
	manifest, err := gitrepo.LoadManifest(repoPath)
	if err != nil {
		return domain.Manifest{}, false, err
	}
	return manifest, manifestExists, nil
}

func resolveManifestOverrides(cfg Config, manifest domain.Manifest, manifestExists bool) (domain.StreamLayout, domain.HistoryMode, error) {
	layout := manifest.StreamLayout
	historyMode := manifest.HistoryMode

	if cfg.StreamLayout != "" {
		parsed, err := toDomainStreamLayout(cfg.StreamLayout)
		if err != nil {
			return "", "", err
		}
		if manifestExists && parsed != manifest.StreamLayout {
			return "", "", fmt.Errorf("%w: stream layout", ErrManifestMismatch)
		}
		layout = parsed
	}
	if cfg.HistoryMode != "" {
		parsed, err := toDomainHistoryMode(cfg.HistoryMode)
		if err != nil {
			return "", "", err
		}
		if manifestExists && parsed != manifest.HistoryMode {
			return "", "", fmt.Errorf("%w: history mode", ErrManifestMismatch)
		}
		historyMode = parsed
	}

	layout = domain.NormalizeStreamLayout(layout)
	if layout == "" {
		layout = domain.DefaultStreamLayout
	}
	if historyMode == "" {
		historyMode = domain.DefaultHistoryMode
	}
	return layout, historyMode, nil
}

func toDomainStreamLayout(layout StreamLayout) (domain.StreamLayout, error) {
	switch layout {
	case StreamLayoutFlat:
		return domain.StreamLayoutFlat, nil
	case StreamLayoutSharded:
		return domain.StreamLayoutSharded, nil
	default:
		return "", fmt.Errorf("invalid stream layout: %s", layout)
	}
}

func toDomainHistoryMode(mode HistoryMode) (domain.HistoryMode, error) {
	switch mode {
	case HistoryModeAppend:
		return domain.HistoryModeAppend, nil
	case HistoryModeAmend:
		return domain.HistoryModeAmend, nil
	default:
		return "", fmt.Errorf("invalid history mode: %s", mode)
	}
}

func toDomainIndexMode(mode IndexMode) (indexapp.Mode, error) {
	switch mode {
	case IndexModeHistory:
		return indexapp.ModeHistory, nil
	case IndexModeState:
		return indexapp.ModeState, nil
	default:
		return "", fmt.Errorf("invalid index mode: %s", mode)
	}
}
