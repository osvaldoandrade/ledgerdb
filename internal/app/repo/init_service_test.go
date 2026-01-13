package repo

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/codecompany/ledgerdb/internal/app/paths"
	"github.com/codecompany/ledgerdb/internal/domain"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time {
	return f.now
}

type fakeRepoStore struct {
	initPath     string
	manifestPath string
	manifest     domain.Manifest
	remotePath   string
	remoteName   string
	remoteURL    string
	initErr      error
	manifestErr  error
	remoteErr    error
	calls        []string
}

func (f *fakeRepoStore) Init(ctx context.Context, path string) error {
	f.calls = append(f.calls, "init")
	f.initPath = path
	return f.initErr
}

func (f *fakeRepoStore) WriteManifest(ctx context.Context, path string, manifest domain.Manifest) error {
	f.calls = append(f.calls, "manifest")
	f.manifestPath = path
	f.manifest = manifest
	return f.manifestErr
}

func (f *fakeRepoStore) SetRemote(ctx context.Context, path, name, url string) error {
	f.calls = append(f.calls, "remote")
	f.remotePath = path
	f.remoteName = name
	f.remoteURL = url
	return f.remoteErr
}

func TestInitDefaultsName(t *testing.T) {
	store := &fakeRepoStore{}
	now := time.Date(2026, 1, 11, 9, 0, 0, 0, time.UTC)
	svc := NewInitService(store, fakeClock{now: now})

	if err := svc.Init(context.Background(), "repo", InitOptions{}); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	expectedBase := filepath.Base(store.initPath)
	if store.manifest.Name != expectedBase {
		t.Fatalf("expected manifest name %q, got %q", expectedBase, store.manifest.Name)
	}
	if !store.manifest.CreatedAt.Equal(now) {
		t.Fatalf("expected CreatedAt %v, got %v", now, store.manifest.CreatedAt)
	}
}

func TestInitUsesProvidedName(t *testing.T) {
	store := &fakeRepoStore{}
	now := time.Date(2026, 1, 11, 9, 0, 0, 0, time.UTC)
	svc := NewInitService(store, fakeClock{now: now})

	if err := svc.Init(context.Background(), "repo", InitOptions{Name: "ledgerdb"}); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	if store.manifest.Name != "ledgerdb" {
		t.Fatalf("expected manifest name %q, got %q", "ledgerdb", store.manifest.Name)
	}
}

func TestInitRequiresPath(t *testing.T) {
	store := &fakeRepoStore{}
	svc := NewInitService(store, fakeClock{now: time.Now().UTC()})

	err := svc.Init(context.Background(), "  ", InitOptions{Name: "ledgerdb"})
	if !errors.Is(err, paths.ErrRepoPathRequired) {
		t.Fatalf("expected ErrRepoPathRequired, got %v", err)
	}
}

func TestInitStopsOnInitError(t *testing.T) {
	initErr := errors.New("init failed")
	store := &fakeRepoStore{initErr: initErr}
	svc := NewInitService(store, fakeClock{now: time.Now().UTC()})

	err := svc.Init(context.Background(), "repo", InitOptions{Name: "ledgerdb"})
	if !errors.Is(err, initErr) {
		t.Fatalf("expected init error, got %v", err)
	}
	if len(store.calls) != 1 || store.calls[0] != "init" {
		t.Fatalf("expected only init call, got %v", store.calls)
	}
}

func TestInitSetsRemote(t *testing.T) {
	store := &fakeRepoStore{}
	svc := NewInitService(store, fakeClock{now: time.Now().UTC()})

	err := svc.Init(context.Background(), "repo", InitOptions{Name: "ledgerdb", RemoteURL: "https://github.com/org/repo.git"})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	if len(store.calls) != 3 || store.calls[2] != "remote" {
		t.Fatalf("expected init, manifest, remote calls, got %v", store.calls)
	}
	if store.remoteName != "origin" {
		t.Fatalf("expected remote name origin, got %s", store.remoteName)
	}
	if store.remoteURL != "https://github.com/org/repo.git" {
		t.Fatalf("expected remote URL to be set")
	}
}
