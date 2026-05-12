package stats

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeDiffStore struct {
	refs         map[string]string
	commitsByTo  map[string][]string
	commitTxs    map[string][]TxRecord
	resolveErr   error
	commitsErr   error
	commitTxsErr error
}

func (f *fakeDiffStore) ResolveRefHash(ctx context.Context, repoPath, ref string) (string, error) {
	if f.resolveErr != nil {
		return "", f.resolveErr
	}
	if hash, ok := f.refs[ref]; ok {
		return hash, nil
	}
	return ref, nil
}

func (f *fakeDiffStore) CollectCommitTxs(ctx context.Context, repoPath, commitHash string) ([]TxRecord, error) {
	if f.commitTxsErr != nil {
		return nil, f.commitTxsErr
	}
	return f.commitTxs[commitHash], nil
}

func (f *fakeDiffStore) CommitsInRange(ctx context.Context, repoPath, from, to string) ([]string, error) {
	if f.commitsErr != nil {
		return nil, f.commitsErr
	}
	return f.commitsByTo[to], nil
}

func TestDiffDetectsAddedChangedRemoved(t *testing.T) {
	store := &fakeDiffStore{
		refs: map[string]string{
			"main~1": "commitA",
			"main":   "commitB",
		},
		commitsByTo: map[string][]string{
			"commitA": {"c1"},
			"commitB": {"c1", "c2"},
		},
		commitTxs: map[string][]TxRecord{
			"c1": {
				{Collection: "users", DocID: "alice", Op: domain.TxOpPut, TxHash: "h-alice-v1", Timestamp: 10},
				{Collection: "users", DocID: "bob", Op: domain.TxOpPut, TxHash: "h-bob-v1", Timestamp: 11},
			},
			"c2": {
				{Collection: "users", DocID: "alice", Op: domain.TxOpPatch, TxHash: "h-alice-v2", Timestamp: 20},
				{Collection: "users", DocID: "bob", Op: domain.TxOpDelete, TxHash: "h-bob-v2", Timestamp: 21},
				{Collection: "items", DocID: "thing", Op: domain.TxOpPut, TxHash: "h-thing-v1", Timestamp: 22},
			},
		},
	}

	svc := NewDiffService(store)
	result, err := svc.Diff(context.Background(), ".", "main~1", "main", DiffOptions{})
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	if result.HashA != "commitA" || result.HashB != "commitB" {
		t.Fatalf("unexpected hashes: %+v", result)
	}
	if len(result.Collections) != 2 {
		t.Fatalf("expected 2 collections, got %d", len(result.Collections))
	}
	items := result.Collections[0]
	users := result.Collections[1]
	if items.Collection != "items" || items.AddedTotal != 1 {
		t.Fatalf("expected items added=1: %+v", items)
	}
	if users.Collection != "users" {
		t.Fatalf("expected second to be users: %+v", users)
	}
	if users.ChangedAll != 1 || users.RemovedAll != 1 {
		t.Fatalf("expected users changed=1 removed=1: %+v", users)
	}
	if len(users.Changed) != 1 || users.Changed[0].DocID != "alice" {
		t.Fatalf("expected alice change, got %+v", users.Changed)
	}
	if len(users.Removed) != 1 || users.Removed[0].DocID != "bob" {
		t.Fatalf("expected bob removal, got %+v", users.Removed)
	}
}

func TestDiffCollectionFilter(t *testing.T) {
	store := &fakeDiffStore{
		refs:        map[string]string{"a": "A", "b": "B"},
		commitsByTo: map[string][]string{"A": {"c1"}, "B": {"c1", "c2"}},
		commitTxs: map[string][]TxRecord{
			"c1": {{Collection: "users", DocID: "u", Op: domain.TxOpPut, TxHash: "h1", Timestamp: 1}},
			"c2": {{Collection: "items", DocID: "i", Op: domain.TxOpPut, TxHash: "h2", Timestamp: 2}},
		},
	}
	svc := NewDiffService(store)
	result, err := svc.Diff(context.Background(), ".", "a", "b", DiffOptions{Collection: "users"})
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(result.Collections) != 1 || result.Collections[0].Collection != "users" {
		t.Fatalf("expected only users, got %+v", result.Collections)
	}
}

func TestDiffLimit(t *testing.T) {
	store := &fakeDiffStore{
		refs:        map[string]string{"a": "A", "b": "B"},
		commitsByTo: map[string][]string{"A": {}, "B": {"c1"}},
		commitTxs: map[string][]TxRecord{
			"c1": {
				{Collection: "users", DocID: "u1", Op: domain.TxOpPut, TxHash: "h1", Timestamp: 1},
				{Collection: "users", DocID: "u2", Op: domain.TxOpPut, TxHash: "h2", Timestamp: 2},
				{Collection: "users", DocID: "u3", Op: domain.TxOpPut, TxHash: "h3", Timestamp: 3},
			},
		},
	}
	svc := NewDiffService(store)
	result, err := svc.Diff(context.Background(), ".", "a", "b", DiffOptions{Limit: 2})
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if result.Collections[0].AddedTotal != 3 || len(result.Collections[0].Added) != 2 {
		t.Fatalf("expected total=3 with limit=2: %+v", result.Collections[0])
	}
}

func TestDiffRequiresRef(t *testing.T) {
	svc := NewDiffService(&fakeDiffStore{})
	if _, err := svc.Diff(context.Background(), ".", "", "x", DiffOptions{}); !errors.Is(err, ErrRefRequired) {
		t.Fatalf("expected ErrRefRequired, got %v", err)
	}
	if _, err := svc.Diff(context.Background(), ".", "x", "", DiffOptions{}); !errors.Is(err, ErrRefRequired) {
		t.Fatalf("expected ErrRefRequired, got %v", err)
	}
}

func TestDiffRejectsNegativeLimit(t *testing.T) {
	svc := NewDiffService(&fakeDiffStore{})
	if _, err := svc.Diff(context.Background(), ".", "a", "b", DiffOptions{Limit: -1}); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("expected ErrInvalidLimit, got %v", err)
	}
}
