package gitrepo

import (
	"bytes"
	"context"
	"path"
	"reflect"
	"sort"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
)

func TestPutTxAndReadBack(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	tx := domain.Transaction{
		TxID:       "01HINT",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	}
	streamPath, txHash, txBytes := writeTx(t, ctx, store, repoDir, tx)

	headHash, err := store.LoadStreamHead(ctx, repoDir, streamPath)
	if err != nil {
		t.Fatalf("LoadStreamHead returned error: %v", err)
	}
	if headHash != txHash {
		t.Fatalf("expected head hash %s, got %s", txHash, headHash)
	}

	headTx, err := store.LoadHeadTx(ctx, repoDir, streamPath)
	if err != nil {
		t.Fatalf("LoadHeadTx returned error: %v", err)
	}
	if !bytes.Equal(headTx.Bytes, txBytes) {
		t.Fatalf("unexpected head tx bytes")
	}

	txs, err := store.LoadStreamTxs(ctx, repoDir, streamPath)
	if err != nil {
		t.Fatalf("LoadStreamTxs returned error: %v", err)
	}
	if len(txs) != 1 {
		t.Fatalf("expected 1 tx, got %d", len(txs))
	}
	if !bytes.Equal(txs[0].Bytes, txBytes) {
		t.Fatalf("unexpected tx bytes")
	}
}

func TestListDocStreams(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	writeTx(t, ctx, store, repoDir, domain.Transaction{
		TxID:       "01HINT1",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	})
	writeTx(t, ctx, store, repoDir, domain.Transaction{
		TxID:       "01HINT2",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc2",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":2}`),
	})
	writeTx(t, ctx, store, repoDir, domain.Transaction{
		TxID:       "01HINT3",
		Timestamp:  3,
		Collection: "orders",
		DocID:      "ord1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"b":1}`),
	})

	streams, err := store.ListDocStreams(ctx, repoDir)
	if err != nil {
		t.Fatalf("ListDocStreams returned error: %v", err)
	}

	expected := []string{
		domain.StreamPath(domain.StreamLayoutSharded, "users", "doc1"),
		domain.StreamPath(domain.StreamLayoutSharded, "users", "doc2"),
		domain.StreamPath(domain.StreamLayoutSharded, "orders", "ord1"),
	}
	sort.Strings(expected)

	if !reflect.DeepEqual(streams, expected) {
		t.Fatalf("expected streams %v, got %v", expected, streams)
	}
}

func TestReadBlob(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	tx := domain.Transaction{
		TxID:       "01HINTBLOB",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	}
	streamPath, _, txBytes := writeTx(t, ctx, store, repoDir, tx)

	tree, err := loadMainTree(repoDir)
	if err != nil {
		t.Fatalf("loadMainTree returned error: %v", err)
	}

	txPath := path.Join(normalizeTreePath(streamPath), domain.TxDirName, txFileName(tx))
	entry, err := tree.FindEntry(txPath)
	if err != nil {
		t.Fatalf("FindEntry returned error: %v", err)
	}

	data, err := store.ReadBlob(ctx, repoDir, entry.Hash.String())
	if err != nil {
		t.Fatalf("ReadBlob returned error: %v", err)
	}
	if !bytes.Equal(data, txBytes) {
		t.Fatalf("unexpected blob bytes")
	}
}

func writeTx(t *testing.T, ctx context.Context, store *Store, repoPath string, tx domain.Transaction) (string, string, []byte) {
	t.Helper()

	encoder := txv3.Encoder{}
	txBytes, err := encoder.Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	txHash := hash.SHA256{}.SumHex(txBytes)
	streamPath := domain.StreamPath(domain.StreamLayoutSharded, tx.Collection, tx.DocID)

	_, err = store.PutTx(ctx, doc.TxWrite{
		RepoPath:   repoPath,
		StreamPath: streamPath,
		TxBytes:    txBytes,
		TxHash:     txHash,
		Tx:         tx,
	})
	if err != nil {
		t.Fatalf("PutTx returned error: %v", err)
	}

	return streamPath, txHash, txBytes
}
