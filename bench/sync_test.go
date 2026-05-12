package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

// syncCommitCount controls how many commits are pre-populated before each
// IndexSync benchmark iteration. Kept small (relative to read corpus) so the
// fresh-repo reset per iteration stays affordable.
const syncCommitCount = 200

func benchIndexSync(b *testing.B, layout ledgerdbsdk.StreamLayout) {
	ctx := context.Background()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		client, _ := setupRepo(b, layout, ledgerdbsdk.HistoryModeAppend)
		for j := 0; j < syncCommitCount; j++ {
			docID := fmt.Sprintf("doc-%05d", j)
			if _, err := client.Put(ctx, benchCollection, docID, samplePayload(j)); err != nil {
				b.Fatalf("seed put: %v", err)
			}
		}
		b.StartTimer()

		if _, err := client.SyncIndex(ctx); err != nil {
			b.Fatalf("sync: %v", err)
		}
	}
}

func BenchmarkIndexSyncFlat(b *testing.B)    { benchIndexSync(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkIndexSyncSharded(b *testing.B) { benchIndexSync(b, ledgerdbsdk.StreamLayoutSharded) }
