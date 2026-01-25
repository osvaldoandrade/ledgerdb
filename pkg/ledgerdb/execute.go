package ledgerdb

import "github.com/osvaldoandrade/ledgerdb/internal/cli"

// Execute runs the LedgerDB CLI entrypoint.
func Execute() int {
	return cli.Execute()
}
