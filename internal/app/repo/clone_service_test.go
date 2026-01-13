package repo

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type fakeCloner struct {
	calledURL  string
	calledPath string
	err        error
}

func (f *fakeCloner) Clone(ctx context.Context, url, path string) error {
	f.calledURL = url
	f.calledPath = path
	return f.err
}

func TestCloneRequiresURL(t *testing.T) {
	svc := NewCloneService(&fakeCloner{})
	err := svc.Clone(context.Background(), " ", "")
	if !errors.Is(err, ErrRepoURLRequired) {
		t.Fatalf("expected ErrRepoURLRequired, got %v", err)
	}
}

func TestCloneUsesProvidedPath(t *testing.T) {
	cloner := &fakeCloner{}
	svc := NewCloneService(cloner)

	if err := svc.Clone(context.Background(), "https://example.com/repo.git", "target"); err != nil {
		t.Fatalf("Clone returned error: %v", err)
	}

	expected, err := filepath.Abs("target")
	if err != nil {
		t.Fatalf("failed to build abs path: %v", err)
	}

	if cloner.calledPath != expected {
		t.Fatalf("expected path %q, got %q", expected, cloner.calledPath)
	}
}

func TestCloneDefaultsPathFromURL(t *testing.T) {
	cloner := &fakeCloner{}
	svc := NewCloneService(cloner)

	if err := svc.Clone(context.Background(), "https://example.com/ledgerdb.git", ""); err != nil {
		t.Fatalf("Clone returned error: %v", err)
	}

	expected, err := filepath.Abs("ledgerdb")
	if err != nil {
		t.Fatalf("failed to build abs path: %v", err)
	}

	if cloner.calledPath != expected {
		t.Fatalf("expected path %q, got %q", expected, cloner.calledPath)
	}
}
