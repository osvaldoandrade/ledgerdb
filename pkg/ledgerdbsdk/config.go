package ledgerdbsdk

import (
	"path/filepath"
	"strings"
	"time"
)

type StreamLayout string

const (
	StreamLayoutFlat    StreamLayout = "flat"
	StreamLayoutSharded StreamLayout = "sharded"
)

type HistoryMode string

const (
	HistoryModeAppend HistoryMode = "append"
	HistoryModeAmend  HistoryMode = "amend"
)

type IndexMode string

const (
	IndexModeHistory IndexMode = "history"
	IndexModeState   IndexMode = "state"
)

// Config defines the SDK behavior for direct core access.
type Config struct {
	RepoPath     string
	AutoSync     bool
	AutoWatch    bool
	SignCommits  bool
	SignKey      string
	StreamLayout StreamLayout
	HistoryMode  HistoryMode
	Index        IndexConfig
}

// IndexConfig configures the SQLite sidecar and watch behavior.
type IndexConfig struct {
	DBPath       string
	Mode         IndexMode
	Interval     time.Duration
	Jitter       time.Duration
	BatchCommits int
	Fast         bool
	Fetch        bool
	OnlyChanges  bool
	EmitResults  bool
}

// DefaultConfig returns opinionated defaults for near real-time indexing.
func DefaultConfig(repoPath string) Config {
	return Config{
		RepoPath:  repoPath,
		AutoSync:  true,
		AutoWatch: false,
		Index: IndexConfig{
			DBPath:       filepath.Join(repoPath, "index.db"),
			Mode:         IndexModeState,
			Interval:     1 * time.Second,
			BatchCommits: 200,
			Fast:         true,
			Fetch:        true,
			OnlyChanges:  true,
			EmitResults:  true,
		},
	}
}

func normalizeConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.RepoPath) == "" {
		return cfg, ErrRepoPathRequired
	}
	if cfg.Index.DBPath == "" {
		cfg.Index.DBPath = filepath.Join(cfg.RepoPath, "index.db")
	}
	if cfg.Index.Mode == "" {
		cfg.Index.Mode = IndexModeState
	}
	if cfg.Index.Interval == 0 {
		cfg.Index.Interval = 5 * time.Second
	}
	if cfg.Index.BatchCommits <= 0 {
		cfg.Index.BatchCommits = 1
	}
	return cfg, nil
}
