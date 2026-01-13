package index

import (
	"fmt"
	"strings"
)

type State struct {
	LastCommit    string
	LastStateTree string
}

type CommitTx struct {
	Path  string
	Bytes []byte
}

type StateTxsResult struct {
	HeadHash  string
	StateHash string
	Txs       []CommitTx
}

type Mode string

const (
	ModeHistory Mode = "history"
	ModeState   Mode = "state"
)

func (mode Mode) IsValid() bool {
	return mode == ModeHistory || mode == ModeState
}

func ParseMode(value string) (Mode, error) {
	parsed := Mode(strings.TrimSpace(value))
	if parsed == "" {
		return "", fmt.Errorf("index mode is required")
	}
	if !parsed.IsValid() {
		return "", fmt.Errorf("invalid index mode: %s", value)
	}
	return parsed, nil
}

func NormalizeMode(mode Mode) Mode {
	if mode.IsValid() {
		return mode
	}
	return ModeHistory
}

type DocRecord struct {
	DocID         string
	Payload       []byte
	TxHash        string
	TxID          string
	Op            string
	SchemaVersion string
	UpdatedAt     int64
	Deleted       bool
}

type SyncOptions struct {
	Fetch        bool
	AllowReset   bool
	BatchCommits int
	Mode         Mode
}

type SyncResult struct {
	Reset        bool
	Fetched      bool
	Commits      int
	TxsApplied   int
	DocsUpserted int
	DocsDeleted  int
	Collections  int
	LastCommit   string
}
