package repo

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type fakeStatusStore struct {
	calledPath string
	status     domain.RepoStatus
	err        error
}

func (f *fakeStatusStore) LoadStatus(ctx context.Context, path string) (domain.RepoStatus, error) {
	f.calledPath = path
	return f.status, f.err
}

func TestStatusRequiresPath(t *testing.T) {
	svc := NewStatusService(&fakeStatusStore{})
	_, err := svc.Status(context.Background(), " ")
	if !errors.Is(err, paths.ErrRepoPathRequired) {
		t.Fatalf("expected ErrRepoPathRequired, got %v", err)
	}
}

func TestStatusNormalizesPath(t *testing.T) {
	store := &fakeStatusStore{}
	svc := NewStatusService(store)

	_, err := svc.Status(context.Background(), "repo")
	if err != nil {
		t.Fatalf("Status returned error: %v", err)
	}

	expected, err := filepath.Abs("repo")
	if err != nil {
		t.Fatalf("failed to build abs path: %v", err)
	}

	if store.calledPath != expected {
		t.Fatalf("expected path %q, got %q", expected, store.calledPath)
	}
}
