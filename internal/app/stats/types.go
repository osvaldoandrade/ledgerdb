package stats

import "github.com/osvaldoandrade/ledgerdb/internal/domain"

// CountObjects mirrors the relevant fields from `git count-objects -v`.
type CountObjects struct {
	LooseObjects int64
	PackCount    int64
	SizeBytes    int64
	SizePackKB   int64
}

// CollectionStat summarizes a single collection.
type CollectionStat struct {
	Name           string
	DocCount       int
	LastTimestamp  int64
	AvgChainDepth  float64
	MaxChainDepth  int
	SampledStreams int
}

// Result is the aggregate output for `ledgerdb stats`.
type Result struct {
	RepoPath     string
	Manifest     domain.Manifest
	HasManifest  bool
	HasHead      bool
	HeadHash     string
	DiskBytes    int64
	CountObjects CountObjects
	Collections  []CollectionStat
	Ahead        int
	Behind       int
	HasRemote    bool
}

// TxRecord is a single transaction's metadata, used by diff.
type TxRecord struct {
	Collection string
	DocID      string
	Op         domain.TxOp
	TxHash     string
	Timestamp  int64
}

// DiffEntry is a single doc difference between two refs. The slice it lives in
// (Added/Changed/Removed) already encodes the kind of change.
type DiffEntry struct {
	Collection string
	DocID      string
	FromHash   string // hash of the doc state at refA (empty when added)
	ToHash     string // hash of the doc state at refB (empty when removed)
}

// DiffCollectionSummary aggregates per-collection counts and (capped) entries.
type DiffCollectionSummary struct {
	Collection string
	Added      []DiffEntry
	Changed    []DiffEntry
	Removed    []DiffEntry
	AddedTotal int
	ChangedAll int
	RemovedAll int
}

// DiffResult is the aggregate output for `ledgerdb diff`.
type DiffResult struct {
	RefA        string
	RefB        string
	HashA       string
	HashB       string
	Collections []DiffCollectionSummary
	Limit       int
}
