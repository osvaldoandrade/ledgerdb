package ledgerdbsdk

import "errors"

var (
	ErrRepoPathRequired = errors.New("ledgerdb-sdk: repo path required")
	ErrIndexNotOpen     = errors.New("ledgerdb-sdk: index database is not open")
	ErrWatchRunning     = errors.New("ledgerdb-sdk: index watch already running")
	ErrNotFound         = errors.New("ledgerdb-sdk: document not found")
	ErrManifestMismatch = errors.New("ledgerdb-sdk: config does not match repository manifest")
)
