// Package bench provides Go benchmark harnesses for LedgerDB's SDK and core.
//
// The harness exercises the public SDK (pkg/ledgerdbsdk) end-to-end against a
// fresh repository created in b.TempDir(). Benchmarks reuse the in-module
// internal/app/repo init service so no on-disk binary or new dependency is
// required. Run with:
//
//	go test -bench=. ./bench/...
package bench

import (
	"context"
	"path/filepath"
	"testing"

	repoapp "github.com/osvaldoandrade/ledgerdb/internal/app/repo"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/gitrepo"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

// setupRepo initializes a fresh LedgerDB repo in b.TempDir() with the given
// layout and history mode and returns an opened SDK client. The client and its
// SQLite index are closed automatically at the end of the benchmark.
//
// AutoSync is disabled because the benchmark repos have no remote configured.
func setupRepo(b *testing.B, layout ledgerdbsdk.StreamLayout, history ledgerdbsdk.HistoryMode) (*ledgerdbsdk.Client, string) {
	b.Helper()

	repoPath := b.TempDir()
	ctx := context.Background()

	domainLayout := toDomainLayout(b, layout)
	domainHistory := toDomainHistory(b, history)

	store := gitrepo.NewStoreWithOptions(gitrepo.StoreOptions{HistoryMode: domainHistory})
	initSvc := repoapp.NewInitService(store, platform.RealClock{})
	if err := initSvc.Init(ctx, repoPath, repoapp.InitOptions{
		Name:         "bench",
		StreamLayout: domainLayout,
		HistoryMode:  domainHistory,
	}); err != nil {
		b.Fatalf("init repo: %v", err)
	}

	cfg := ledgerdbsdk.Config{
		RepoPath:     repoPath,
		AutoSync:     false,
		AutoWatch:    false,
		StreamLayout: layout,
		HistoryMode:  history,
		Index: ledgerdbsdk.IndexConfig{
			DBPath: filepath.Join(repoPath, "index.db"),
			Mode:   ledgerdbsdk.IndexModeState,
			Fast:   true,
		},
	}

	client, err := ledgerdbsdk.New(cfg)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	if err := client.OpenIndex(ctx); err != nil {
		b.Fatalf("open index: %v", err)
	}
	b.Cleanup(func() { _ = client.Close() })

	return client, repoPath
}

func toDomainLayout(b *testing.B, layout ledgerdbsdk.StreamLayout) domain.StreamLayout {
	b.Helper()
	switch layout {
	case ledgerdbsdk.StreamLayoutFlat:
		return domain.StreamLayoutFlat
	case ledgerdbsdk.StreamLayoutSharded:
		return domain.StreamLayoutSharded
	default:
		b.Fatalf("unknown layout: %s", layout)
		return ""
	}
}

func toDomainHistory(b *testing.B, mode ledgerdbsdk.HistoryMode) domain.HistoryMode {
	b.Helper()
	switch mode {
	case ledgerdbsdk.HistoryModeAppend:
		return domain.HistoryModeAppend
	case ledgerdbsdk.HistoryModeAmend:
		return domain.HistoryModeAmend
	default:
		b.Fatalf("unknown history mode: %s", mode)
		return ""
	}
}
