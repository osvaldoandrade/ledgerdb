package main

import (
	"os"

	"github.com/codecompany/ledgerdb/pkg/ledgerdb"
)

func main() {
	os.Exit(ledgerdb.Execute())
}
