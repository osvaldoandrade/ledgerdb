package index

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeFetcher struct {
	called bool
	err    error
}

func (f *fakeFetcher) Fetch(ctx context.Context, repoPath string) error {
	f.called = true
	return f.err
}

type fakeSource struct {
	commits     []string
	txs         map[string][]CommitTx
	stateResult StateTxsResult
	stateErr    error
}

func (f fakeSource) ListCommitHashes(ctx context.Context, repoPath, sinceHash string) ([]string, error) {
	return f.commits, nil
}

func (f fakeSource) CommitTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error) {
	return f.txs[commitHash], nil
}

func (f fakeSource) CommitStateTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error) {
	return f.txs[commitHash], nil
}

func (f fakeSource) StateTxsSince(ctx context.Context, repoPath string, state State) (StateTxsResult, error) {
	return f.stateResult, f.stateErr
}

type memStore struct {
	state       State
	collections map[string]map[string]DocRecord
	resetCalled bool
	beginCount  int
}

func newMemStore() *memStore {
	return &memStore{collections: make(map[string]map[string]DocRecord)}
}

func (m *memStore) GetState(ctx context.Context) (State, error) {
	return m.state, nil
}

func (m *memStore) Begin(ctx context.Context) (StoreTx, error) {
	m.beginCount++
	return &memStoreTx{store: m}, nil
}

func (m *memStore) Reset(ctx context.Context) error {
	m.state = State{}
	m.collections = make(map[string]map[string]DocRecord)
	m.resetCalled = true
	return nil
}

type memStoreTx struct {
	store *memStore
}

func (m *memStoreTx) EnsureCollection(ctx context.Context, collection string) (string, error) {
	if _, ok := m.store.collections[collection]; !ok {
		m.store.collections[collection] = make(map[string]DocRecord)
	}
	return collection, nil
}

func (m *memStoreTx) GetDoc(ctx context.Context, collection, docID string) (DocRecord, bool, error) {
	col := m.store.collections[collection]
	if col == nil {
		return DocRecord{}, false, nil
	}
	record, ok := col[docID]
	return record, ok, nil
}

func (m *memStoreTx) UpsertDoc(ctx context.Context, collection string, record DocRecord) error {
	col := m.store.collections[collection]
	if col == nil {
		col = make(map[string]DocRecord)
		m.store.collections[collection] = col
	}
	col[record.DocID] = record
	return nil
}

func (m *memStoreTx) SetState(ctx context.Context, state State) error {
	m.store.state = state
	return nil
}

func (m *memStoreTx) Commit() error {
	return nil
}

func (m *memStoreTx) Rollback() error {
	return nil
}

type passCanonicalizer struct{}

func (passCanonicalizer) Canonicalize(ctx context.Context, input []byte) ([]byte, error) {
	return input, nil
}

type mapDecoder struct {
	txs map[string]domain.Transaction
}

func (d mapDecoder) Decode(data []byte) (domain.Transaction, error) {
	tx, ok := d.txs[string(data)]
	if !ok {
		return domain.Transaction{}, errors.New("unknown tx")
	}
	return tx, nil
}

type fakePatcher struct {
	out []byte
	err error
}

func (f fakePatcher) Apply(ctx context.Context, doc, patch []byte) ([]byte, error) {
	return f.out, f.err
}

type testHasher struct{}

func (testHasher) SumHex(data []byte) string {
	return "hash:" + string(data)
}

func TestSyncServiceAppliesTxs(t *testing.T) {
	store := newMemStore()
	source := fakeSource{
		commits: []string{"c1"},
		txs: map[string][]CommitTx{
			"c1": {
				{Bytes: []byte("tx1")},
				{Bytes: []byte("tx2")},
				{Bytes: []byte("tx3")},
			},
		},
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
			"tx2": {
				TxID:       "tx2",
				Timestamp:  2,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPatch,
				Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
			},
			"tx3": {
				TxID:       "tx3",
				Timestamp:  3,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpDelete,
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{out: []byte(`{"a":2}`)},
		testHasher{},
	)

	result, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false})
	if err != nil {
		t.Fatalf("expected sync to succeed: %v", err)
	}

	if result.Commits != 1 || result.TxsApplied != 3 || result.DocsUpserted != 2 || result.DocsDeleted != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Collections != 1 || result.LastCommit != "c1" {
		t.Fatalf("unexpected index metadata: %+v", result)
	}

	record := store.collections["users"]["u1"]
	if !record.Deleted || record.Op != "delete" {
		t.Fatalf("expected deleted record, got %+v", record)
	}
	if record.TxHash != "hash:tx3" {
		t.Fatalf("expected tx hash to match last tx, got %s", record.TxHash)
	}
}

func TestSyncServiceStateModeAppliesTxs(t *testing.T) {
	store := newMemStore()
	source := fakeSource{
		stateResult: StateTxsResult{
			HeadHash:  "c9",
			StateHash: "s9",
			Txs: []CommitTx{
				{Bytes: []byte("tx1")},
			},
		},
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{},
		testHasher{},
	)

	result, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false, Mode: ModeState})
	if err != nil {
		t.Fatalf("expected sync to succeed: %v", err)
	}
	if result.Commits != 1 || result.TxsApplied != 1 || result.DocsUpserted != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.LastCommit != "c9" || store.state.LastCommit != "c9" || store.state.LastStateTree != "s9" {
		t.Fatalf("expected last commit to be c9, got %s", result.LastCommit)
	}
}

func TestSyncServiceMissingDoc(t *testing.T) {
	store := newMemStore()
	source := fakeSource{
		commits: []string{"c1"},
		txs: map[string][]CommitTx{
			"c1": {
				{Bytes: []byte("tx1")},
			},
		},
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPatch,
				Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{out: []byte(`{"a":2}`)},
		testHasher{},
	)

	if _, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false}); err == nil || !errors.Is(err, ErrMissingDocument) {
		t.Fatalf("expected ErrMissingDocument, got %v", err)
	}
}

func TestSyncServiceStateModeFallsBackToHistory(t *testing.T) {
	store := newMemStore()
	source := fakeSource{
		commits: []string{"c1"},
		txs: map[string][]CommitTx{
			"c1": {
				{Bytes: []byte("tx1")},
			},
		},
		stateErr: ErrStateUnavailable,
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{},
		testHasher{},
	)

	result, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false, Mode: ModeState})
	if err != nil {
		t.Fatalf("expected sync to succeed: %v", err)
	}
	if result.Commits != 1 || result.LastCommit != "c1" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

type resetSource struct {
	commits     []string
	txs         map[string][]CommitTx
	stateResult StateTxsResult
	stateErr    error
}

func (r resetSource) ListCommitHashes(ctx context.Context, repoPath, sinceHash string) ([]string, error) {
	if sinceHash != "" {
		return nil, ErrCommitNotFound
	}
	return r.commits, nil
}

func (r resetSource) CommitTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error) {
	return r.txs[commitHash], nil
}

func (r resetSource) CommitStateTxs(ctx context.Context, repoPath, commitHash string) ([]CommitTx, error) {
	return r.txs[commitHash], nil
}

func (r resetSource) StateTxsSince(ctx context.Context, repoPath string, state State) (StateTxsResult, error) {
	return r.stateResult, r.stateErr
}

func TestSyncServiceResetsOnMissingCommit(t *testing.T) {
	store := newMemStore()
	store.state = State{LastCommit: "old"}
	store.collections["users"] = map[string]DocRecord{
		"u1": {DocID: "u1", Payload: []byte(`{"a":9}`)},
	}
	source := resetSource{
		commits: []string{"c1"},
		txs: map[string][]CommitTx{
			"c1": {
				{Bytes: []byte("tx1")},
			},
		},
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{},
		testHasher{},
	)

	result, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false, AllowReset: true})
	if err != nil {
		t.Fatalf("expected sync to succeed: %v", err)
	}
	if !result.Reset {
		t.Fatalf("expected reset to be reported")
	}
	if !store.resetCalled {
		t.Fatalf("expected store reset to be called")
	}
	if store.state.LastCommit != "c1" {
		t.Fatalf("expected last commit to be c1, got %s", store.state.LastCommit)
	}
}

func TestSyncServiceBatchesCommits(t *testing.T) {
	store := newMemStore()
	source := fakeSource{
		commits: []string{"c1", "c2", "c3"},
		txs: map[string][]CommitTx{
			"c1": {{Bytes: []byte("tx1")}},
			"c2": {{Bytes: []byte("tx2")}},
			"c3": {{Bytes: []byte("tx3")}},
		},
	}
	decoder := mapDecoder{
		txs: map[string]domain.Transaction{
			"tx1": {
				TxID:       "tx1",
				Timestamp:  1,
				Collection: "users",
				DocID:      "u1",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"a":1}`),
			},
			"tx2": {
				TxID:       "tx2",
				Timestamp:  2,
				Collection: "users",
				DocID:      "u2",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"b":2}`),
			},
			"tx3": {
				TxID:       "tx3",
				Timestamp:  3,
				Collection: "users",
				DocID:      "u3",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"c":3}`),
			},
		},
	}

	service := NewSyncService(
		nil,
		source,
		store,
		passCanonicalizer{},
		decoder,
		fakePatcher{},
		testHasher{},
	)

	result, err := service.Sync(context.Background(), "repo", SyncOptions{Fetch: false, BatchCommits: 2})
	if err != nil {
		t.Fatalf("expected sync to succeed: %v", err)
	}
	if result.Commits != 3 || result.LastCommit != "c3" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if store.beginCount != 2 {
		t.Fatalf("expected 2 batches, got %d", store.beginCount)
	}
	if store.state.LastCommit != "c3" {
		t.Fatalf("expected last commit to be c3, got %s", store.state.LastCommit)
	}
}
