package stats

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeStatsStore struct {
	status       domain.RepoStatus
	statusErr    error
	streams      []string
	streamsErr   error
	heads        map[string]string
	headsErr     error
	streamTxs    map[string][]doc.TxBlob
	streamTxsErr error
	objects      CountObjects
	objectsErr   error
	diskBytes    int64
	diskErr      error
	ahead        int
	behind       int
	abErr        error
}

func (f *fakeStatsStore) LoadStatus(ctx context.Context, repoPath string) (domain.RepoStatus, error) {
	return f.status, f.statusErr
}

func (f *fakeStatsStore) ListDocStreams(ctx context.Context, repoPath string) ([]string, error) {
	return f.streams, f.streamsErr
}

func (f *fakeStatsStore) LoadStreamHead(ctx context.Context, repoPath, streamPath string) (string, error) {
	if f.headsErr != nil {
		return "", f.headsErr
	}
	return f.heads[streamPath], nil
}

func (f *fakeStatsStore) LoadStreamTxs(ctx context.Context, repoPath, streamPath string) ([]doc.TxBlob, error) {
	if f.streamTxsErr != nil {
		return nil, f.streamTxsErr
	}
	return f.streamTxs[streamPath], nil
}

func (f *fakeStatsStore) CountObjects(ctx context.Context, repoPath string) (CountObjects, error) {
	return f.objects, f.objectsErr
}

func (f *fakeStatsStore) RepoDiskBytes(ctx context.Context, repoPath string) (int64, error) {
	return f.diskBytes, f.diskErr
}

func (f *fakeStatsStore) AheadBehind(ctx context.Context, repoPath, localRef, remoteRef string) (int, int, error) {
	return f.ahead, f.behind, f.abErr
}

type fakeDecoder struct {
	txByBytes map[string]domain.Transaction
}

func (f fakeDecoder) Decode(data []byte) (domain.Transaction, error) {
	if tx, ok := f.txByBytes[string(data)]; ok {
		return tx, nil
	}
	return domain.Transaction{}, errors.New("unknown blob")
}

func TestStatsAggregatesCollections(t *testing.T) {
	store := &fakeStatsStore{
		status: domain.RepoStatus{
			HasManifest: true,
			HasHead:     true,
			HeadHash:    "abc",
			Manifest: domain.Manifest{
				Version:      2,
				Name:         "ledger",
				StreamLayout: domain.StreamLayoutSharded,
				HistoryMode:  domain.HistoryModeAppend,
			},
		},
		streams: []string{
			"documents/users/aa/bb/DOC_x",
			"documents/users/cc/dd/DOC_y",
			"documents/items/ee/ff/DOC_z",
		},
		streamTxs: map[string][]doc.TxBlob{
			"documents/users/aa/bb/DOC_x": {
				{Bytes: []byte("u1a")},
				{Bytes: []byte("u1b")},
			},
			"documents/users/cc/dd/DOC_y": {
				{Bytes: []byte("u2a")},
			},
			"documents/items/ee/ff/DOC_z": {
				{Bytes: []byte("i1a")},
			},
		},
		diskBytes: 4096,
		objects:   CountObjects{LooseObjects: 12, PackCount: 1},
		ahead:     2,
		behind:    1,
	}

	decoder := fakeDecoder{
		txByBytes: map[string]domain.Transaction{
			"u1a": {Timestamp: 10, Collection: "users", DocID: "x", Op: domain.TxOpPut},
			"u1b": {Timestamp: 30, Collection: "users", DocID: "x", Op: domain.TxOpPatch},
			"u2a": {Timestamp: 20, Collection: "users", DocID: "y", Op: domain.TxOpPut},
			"i1a": {Timestamp: 5, Collection: "items", DocID: "z", Op: domain.TxOpPut},
		},
	}

	svc := NewService(store, decoder)
	result, err := svc.Stats(context.Background(), ".", Options{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if len(result.Collections) != 2 {
		t.Fatalf("expected 2 collections, got %d", len(result.Collections))
	}
	if result.Collections[0].Name != "items" {
		t.Fatalf("expected first collection items (sorted), got %s", result.Collections[0].Name)
	}
	users := result.Collections[1]
	if users.Name != "users" || users.DocCount != 2 {
		t.Fatalf("unexpected users stat: %+v", users)
	}
	if users.MaxChainDepth != 2 {
		t.Fatalf("expected max chain depth 2, got %d", users.MaxChainDepth)
	}
	if users.AvgChainDepth != 1.5 {
		t.Fatalf("expected avg chain depth 1.5, got %v", users.AvgChainDepth)
	}
	if users.LastTimestamp != 30 {
		t.Fatalf("expected last timestamp 30, got %d", users.LastTimestamp)
	}
	if result.Ahead != 2 || result.Behind != 1 {
		t.Fatalf("expected ahead=2 behind=1, got %d/%d", result.Ahead, result.Behind)
	}
	if !result.HasManifest || result.Manifest.Name != "ledger" {
		t.Fatalf("expected manifest passed through")
	}
}

func TestStatsCollectionFilter(t *testing.T) {
	store := &fakeStatsStore{
		streams: []string{
			"documents/users/aa/DOC_x",
			"documents/items/bb/DOC_y",
		},
		streamTxs: map[string][]doc.TxBlob{
			"documents/users/aa/DOC_x": {{Bytes: []byte("u")}},
			"documents/items/bb/DOC_y": {{Bytes: []byte("i")}},
		},
	}
	decoder := fakeDecoder{
		txByBytes: map[string]domain.Transaction{
			"u": {Timestamp: 1, Collection: "users", DocID: "x", Op: domain.TxOpPut},
			"i": {Timestamp: 2, Collection: "items", DocID: "y", Op: domain.TxOpPut},
		},
	}
	svc := NewService(store, decoder)
	result, err := svc.Stats(context.Background(), ".", Options{Collection: "users"})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if len(result.Collections) != 1 || result.Collections[0].Name != "users" {
		t.Fatalf("filter did not restrict result: %+v", result.Collections)
	}
}

func TestStatsPropagatesStatusError(t *testing.T) {
	wantErr := errors.New("boom")
	store := &fakeStatsStore{statusErr: wantErr}
	svc := NewService(store, fakeDecoder{})
	_, err := svc.Stats(context.Background(), ".", Options{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wantErr, got %v", err)
	}
}
