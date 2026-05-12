package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

const benchCollection = "bench"

// samplePayload returns a small, fixed-shape JSON doc the benchmarks Put.
func samplePayload(i int) []byte {
	return []byte(fmt.Sprintf(`{"i":%d,"name":"bench","tags":["a","b"]}`, i))
}

// patchOps returns a JSON Patch (RFC 6902) replacing the "i" field.
func patchOps(i int) []byte {
	return []byte(fmt.Sprintf(`[{"op":"replace","path":"/i","value":%d}]`, i))
}

func benchPut(b *testing.B, layout ledgerdbsdk.StreamLayout) {
	for _, mode := range []ledgerdbsdk.HistoryMode{ledgerdbsdk.HistoryModeAppend, ledgerdbsdk.HistoryModeAmend} {
		b.Run(string(mode), func(b *testing.B) {
			client, _ := setupRepo(b, layout, mode)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docID := fmt.Sprintf("doc-%d", i)
				if _, err := client.Put(ctx, benchCollection, docID, samplePayload(i)); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
		})
	}
}

func benchPatch(b *testing.B, layout ledgerdbsdk.StreamLayout) {
	for _, mode := range []ledgerdbsdk.HistoryMode{ledgerdbsdk.HistoryModeAppend, ledgerdbsdk.HistoryModeAmend} {
		b.Run(string(mode), func(b *testing.B) {
			client, _ := setupRepo(b, layout, mode)
			ctx := context.Background()

			// Seed a single document; the benchmark loop patches it repeatedly.
			const docID = "patch-target"
			if _, err := client.Put(ctx, benchCollection, docID, samplePayload(0)); err != nil {
				b.Fatalf("seed put: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := client.Patch(ctx, benchCollection, docID, patchOps(i+1)); err != nil {
					b.Fatalf("patch: %v", err)
				}
			}
		})
	}
}

func BenchmarkPutFlat(b *testing.B)    { benchPut(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPutSharded(b *testing.B) { benchPut(b, ledgerdbsdk.StreamLayoutSharded) }

func BenchmarkPatchFlat(b *testing.B)    { benchPatch(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkPatchSharded(b *testing.B) { benchPatch(b, ledgerdbsdk.StreamLayoutSharded) }
