package bench

import (
	"context"
	"fmt"
	"testing"

	maintenanceapp "github.com/osvaldoandrade/ledgerdb/internal/app/maintenance"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/canonicaljson"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/gitrepo"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/ident"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/jsonpatch"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
	"github.com/osvaldoandrade/ledgerdb/internal/platform"
	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

// snapshotDocID is the single document each iteration patches to build a
// long chain that `maintenance snapshot` can then compact.
const snapshotDocID = "doc-snapshot"

// BenchmarkSnapshotCompaction measures end-to-end snapshot compaction across
// varying patch-chain depths. Each subtest pre-populates a fresh repo with a
// single doc patched N times and then times the maintenance snapshot service.
//
// Note: ledgerdbsdk does not currently expose a Snapshot method
// (TODO(#27): promote maintenanceapp.SnapshotService to the SDK). The bench
// wires the same in-module service the CLI uses so it can still run today.
func BenchmarkSnapshotCompaction(b *testing.B) {
	for _, depth := range []int{5, 50, 500} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				client, repoPath := setupRepo(b, ledgerdbsdk.StreamLayoutFlat, ledgerdbsdk.HistoryModeAppend)

				if _, err := client.Put(ctx, benchCollection, snapshotDocID, samplePayload(0)); err != nil {
					b.Fatalf("seed put: %v", err)
				}
				for j := 1; j <= depth; j++ {
					if _, err := client.Patch(ctx, benchCollection, snapshotDocID, patchOps(j)); err != nil {
						b.Fatalf("seed patch %d: %v", j, err)
					}
				}

				svc := newSnapshotService(domain.HistoryModeAppend)
				b.StartTimer()

				if _, err := svc.Snapshot(ctx, repoPath, maintenanceapp.SnapshotOptions{Threshold: 1}); err != nil {
					b.Fatalf("snapshot: %v", err)
				}
			}
		})
	}
}

func newSnapshotService(history domain.HistoryMode) *maintenanceapp.SnapshotService {
	store := gitrepo.NewStoreWithOptions(gitrepo.StoreOptions{HistoryMode: history})
	return maintenanceapp.NewSnapshotService(
		store,
		store,
		store,
		canonicaljson.Canonicalizer{},
		txv3.Encoder{},
		txv3.Decoder{},
		jsonpatch.Patcher{},
		hash.SHA256{},
		platform.RealClock{},
		ident.NewULIDGenerator(),
		history,
	)
}
