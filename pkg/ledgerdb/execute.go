package ledgerdb

import "github.com/codecompany/ledgerdb/internal/cli"

// Execute runs the LedgerDB CLI entrypoint.
func Execute() int {
	return cli.Execute()
}
