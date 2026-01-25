package gitrepo

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
)

func TestPutTxConcurrentCAS(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	streamPath, parentHash, _ := writeTx(t, ctx, store, repoDir, domain.Transaction{
		TxID:       "01HBASE",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	})

	tx1 := domain.Transaction{
		TxID:       "01HCAS1",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
		ParentHash: parentHash,
	}
	tx2 := domain.Transaction{
		TxID:       "01HCAS2",
		Timestamp:  3,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":3}]`),
		ParentHash: parentHash,
	}

	write1 := buildTxWrite(t, repoDir, streamPath, tx1)
	write2 := buildTxWrite(t, repoDir, streamPath, tx2)

	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := store.PutTx(ctx, write1)
		results <- err
	}()
	go func() {
		defer wg.Done()
		_, err := store.PutTx(ctx, write2)
		results <- err
	}()
	wg.Wait()
	close(results)

	var success, headChanged int
	for err := range results {
		if err == nil {
			success++
			continue
		}
		if errors.Is(err, domain.ErrHeadChanged) {
			headChanged++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if success != 1 || headChanged != 1 {
		t.Fatalf("expected 1 success and 1 ErrHeadChanged, got success=%d headChanged=%d", success, headChanged)
	}
}

func buildTxWrite(t *testing.T, repoPath, streamPath string, tx domain.Transaction) doc.TxWrite {
	t.Helper()

	encoder := txv3.Encoder{}
	txBytes, err := encoder.Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	txHash := hash.SHA256{}.SumHex(txBytes)
	return doc.TxWrite{
		RepoPath:   repoPath,
		StreamPath: streamPath,
		TxBytes:    txBytes,
		TxHash:     txHash,
		Tx:         tx,
	}
}
