package dr

import (
	"errors"
	"time"
)

// Version of the ledgerdb CLI embedded into backup metadata.  Keeping this in
// the dr package avoids pulling a build-time variable across the codebase; the
// release pipeline can override it via -ldflags if needed.
const Version = "0.2.3"

// BackupFormatVersion is bumped whenever the backup tarball layout changes.
const BackupFormatVersion = 1

// BackupManifestName is the metadata file embedded inside the backup tarball.
const BackupManifestName = "backup.json"

// BackupBundleName is the git bundle stored inside the backup tarball.
const BackupBundleName = "repo.bundle"

// BackupRepoManifestName is the copy of db.yaml stored inside the tarball.
const BackupRepoManifestName = "db.yaml"

// SidecarDirName is the directory inside the tarball that holds optional
// sidecar artifacts (currently the SQLite index).
const SidecarDirName = "sidecar"

// ErrInvalidBackup is returned when a backup tarball fails structural or
// checksum validation.
var ErrInvalidBackup = errors.New("invalid backup archive")

// ErrIntegrityFailed is returned when the post-restore integrity verify
// reports any issues.
var ErrIntegrityFailed = errors.New("restored repository failed integrity verification")

// ErrTruncateThresholdRequired is returned when --before is empty.
var ErrTruncateThresholdRequired = errors.New("truncate threshold is required (--before)")

// ErrTruncateConfirmation is returned when truncate is invoked without --yes
// and outside of dry-run mode.
var ErrTruncateConfirmation = errors.New("truncate is destructive; pass --yes to confirm")

// ErrInvalidThreshold is returned when --before cannot be parsed.
var ErrInvalidThreshold = errors.New("invalid truncate threshold")

// BackupOptions controls the produced tarball.
type BackupOptions struct {
	OutputPath      string
	IncludeSidecar  bool
	SidecarPath     string // optional path to the sqlite index file
	LedgerDBVersion string // override Version (used by tests)
}

// BackupResult summarises a completed backup.
type BackupResult struct {
	OutputPath  string    `json:"output_path"`
	BundleSize  int64     `json:"bundle_size"`
	BundleHash  string    `json:"bundle_hash"`
	CreatedAt   time.Time `json:"created_at"`
	IncludesSidecar bool  `json:"includes_sidecar"`
}

// BackupManifest is the JSON document stored inside the archive.
type BackupManifest struct {
	FormatVersion   int       `json:"format_version"`
	CreatedAt       time.Time `json:"created_at"`
	SourceRepoPath  string    `json:"source_repo_path"`
	BundleHash      string    `json:"bundle_hash"`
	BundleSize      int64     `json:"bundle_size"`
	LedgerDBVersion string    `json:"ledgerdb_version"`
	IncludesSidecar bool      `json:"includes_sidecar"`
}

// RestoreOptions controls how a backup tarball is unpacked.
type RestoreOptions struct {
	InputPath  string
	TargetPath string
	SkipVerify bool
}

// RestoreResult summarises a restore.
type RestoreResult struct {
	TargetPath     string `json:"target_path"`
	Streams        int    `json:"streams"`
	IntegrityValid int    `json:"integrity_valid"`
	IntegritySkipped bool `json:"integrity_skipped"`
	BundleHash     string `json:"bundle_hash"`
}

// TruncateOptions controls history truncation.
type TruncateOptions struct {
	Before     string
	Collection string
	DryRun     bool
	Yes        bool
}

// TruncatePlan describes what truncate intends to do — built before any
// destructive action so dry-run can simply report it.
type TruncatePlan struct {
	ThresholdTimestamp int64
	ThresholdTxID      string
	Collection         string
	Streams            []TruncateStreamPlan
}

// TruncateStreamPlan describes the action for a single stream.
type TruncateStreamPlan struct {
	StreamPath  string
	KeptTx      int
	DroppedTx   int
	NewBaseHash string // hash of the synthesised base snapshot tx (if any)
}

// TruncateExecution is the result of actually rewriting history.
type TruncateExecution struct {
	BackupBranch string
	NewBranch    string
}

// TruncateResult is what is returned to the CLI.
type TruncateResult struct {
	Plan         TruncatePlan       `json:"plan"`
	Execution    *TruncateExecution `json:"execution,omitempty"`
	DryRun       bool               `json:"dry_run"`
}
