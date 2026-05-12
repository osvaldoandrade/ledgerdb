package bench

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/pkg/ledgerdbsdk"
)

// readCorpusSize is the number of docs pre-populated for read benchmarks.
//
// The default (1k) keeps `go test -bench=. -benchtime=1x` finishing in under a
// minute for CI. Set BENCH_READ_CORPUS=10000 (or any positive int) to scale
// the corpus toward the 10k/100k tiers called out in issue #19.
var readCorpusSize = readEnvInt("BENCH_READ_CORPUS", 1_000)

func readEnvInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func benchGet(b *testing.B, layout ledgerdbsdk.StreamLayout) {
	client, _ := setupRepo(b, layout, ledgerdbsdk.HistoryModeAppend)
	ctx := context.Background()

	b.StopTimer()
	keys := make([]string, readCorpusSize)
	for i := 0; i < readCorpusSize; i++ {
		keys[i] = fmt.Sprintf("doc-%05d", i)
		if _, err := client.Put(ctx, benchCollection, keys[i], samplePayload(i)); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}
	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(readCorpusSize)]
		if _, err := client.Get(ctx, benchCollection, key); err != nil {
			b.Fatalf("get %s: %v", key, err)
		}
	}
}

func BenchmarkGetFlat(b *testing.B)    { benchGet(b, ledgerdbsdk.StreamLayoutFlat) }
func BenchmarkGetSharded(b *testing.B) { benchGet(b, ledgerdbsdk.StreamLayoutSharded) }
