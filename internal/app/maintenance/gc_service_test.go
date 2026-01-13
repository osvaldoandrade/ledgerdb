package maintenance

import (
	"context"
	"path/filepath"
	"testing"
)

type fakeGCExecutor struct {
	repo  string
	prune string
	err   error
}

func (f *fakeGCExecutor) RunGC(ctx context.Context, repoPath, prune string) error {
	f.repo = repoPath
	f.prune = prune
	return f.err
}

func TestGCServiceRunsGC(t *testing.T) {
	repoDir := t.TempDir()
	executor := &fakeGCExecutor{}
	service := NewGCService(executor)

	if err := service.GC(context.Background(), repoDir, GCOptions{Prune: " now "}); err != nil {
		t.Fatalf("GC returned error: %v", err)
	}

	expected, err := filepath.Abs(repoDir)
	if err != nil {
		t.Fatalf("Abs returned error: %v", err)
	}

	if executor.repo != expected {
		t.Fatalf("expected repo %s, got %s", expected, executor.repo)
	}
	if executor.prune != "now" {
		t.Fatalf("expected prune 'now', got %q", executor.prune)
	}
}
