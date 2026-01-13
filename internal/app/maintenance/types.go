package maintenance

type GCOptions struct {
	Prune string
}

type SnapshotOptions struct {
	Threshold int
	Max       int
	DryRun    bool
}

type SnapshotResult struct {
	Streams     int
	Processed   int
	Snapshotted int
	Planned     int
	Skipped     int
	Truncated   bool
	DryRun      bool
	Issues      []Issue
}

type Issue struct {
	StreamPath string
	Code       string
	Message    string
}
