package main

import (
	"os"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdb"
)

func main() {
	os.Exit(ledgerdb.Execute())
}
