package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

func main() {
	repo := os.Getenv("LEDGERDB_REPO")
	if repo == "" {
		fmt.Fprintln(os.Stderr, "LEDGERDB_REPO is required (path to ledgerdb.git)")
		os.Exit(1)
	}

	cfg := ledgerdbsdk.DefaultConfig(repo)
	cfg.AutoWatch = true
	cfg.Index.Interval = 1 * time.Second

	ctx := context.Background()
	client, err := ledgerdbsdk.Open(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	if _, err := client.SyncIndex(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "index sync: %v\n", err)
	}

	doc, err := client.Get(ctx, "tasks", "task_0001")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
	} else {
		fmt.Printf("ledger get tx=%s op=%s payload=%s\n", doc.TxID, doc.Op, string(doc.Payload))
	}

	rows, err := client.Query(ctx, "SELECT doc_id, payload FROM collection_tasks WHERE deleted = 0 LIMIT 5")
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var docID string
		var payload []byte
		if err := rows.Scan(&docID, &payload); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			return
		}
		fmt.Printf("index row doc_id=%s payload=%s\n", docID, string(payload))
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows: %v\n", err)
	}
}
