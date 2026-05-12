package dr

import (
	"context"
	"time"

	integrityapp "github.com/osvaldoandrade/ledgerdb/internal/app/integrity"
)

// Bundler creates a single-file git bundle of the repository.
type Bundler interface {
	// Bundle writes a bundle file at bundlePath containing every reachable
	// object from the repository at repoPath.
	Bundle(ctx context.Context, repoPath, bundlePath string) error
}

// BundleRestorer restores a bundle into a fresh bare repository.
type BundleRestorer interface {
	// RestoreBundle initialises a bare repository at repoPath and pulls every
	// ref from bundlePath into it.
	RestoreBundle(ctx context.Context, bundlePath, repoPath string) error
}

// IntegrityVerifier verifies the integrity of a repository (deep mode is used
// by restore to fail fast if the imported history is broken).
type IntegrityVerifier interface {
	Verify(ctx context.Context, repoPath string, opts integrityapp.VerifyOptions) (integrityapp.VerifyResult, error)
}

// Clock returns the current time. The same shape used elsewhere in app
// services to keep clocks fakeable.
type Clock interface {
	Now() time.Time
}

// TruncateExecutor performs the actual rewrite of the underlying git history,
// producing a new branch that contains only the post-threshold transactions
// while keeping the latest snapshot per document.
type TruncateExecutor interface {
	Truncate(ctx context.Context, repoPath string, plan TruncatePlan, opts TruncateOptions) (TruncateExecution, error)
}
