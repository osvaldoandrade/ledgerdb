package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/hash"
	"github.com/osvaldoandrade/ledgerdb/internal/infra/txv3"
)

func TestPutTxConcurrentCAS(t *testing.T) {
	ctx := context.Background()
	repoDir := t.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	streamPath, parentHash, _ := writeTx(t, ctx, store, repoDir, domain.Transaction{
		TxID:       "01HBASE",
		Timestamp:  1,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"a":1}`),
	})

	tx1 := domain.Transaction{
		TxID:       "01HCAS1",
		Timestamp:  2,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":2}]`),
		ParentHash: parentHash,
	}
	tx2 := domain.Transaction{
		TxID:       "01HCAS2",
		Timestamp:  3,
		Collection: "users",
		DocID:      "doc1",
		Op:         domain.TxOpPatch,
		Patch:      []byte(`[{"op":"replace","path":"/a","value":3}]`),
		ParentHash: parentHash,
	}

	write1 := buildTxWrite(t, repoDir, streamPath, tx1)
	write2 := buildTxWrite(t, repoDir, streamPath, tx2)

	results := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := store.PutTx(ctx, write1)
		results <- err
	}()
	go func() {
		defer wg.Done()
		_, err := store.PutTx(ctx, write2)
		results <- err
	}()
	wg.Wait()
	close(results)

	var success, headChanged int
	for err := range results {
		if err == nil {
			success++
			continue
		}
		if errors.Is(err, domain.ErrHeadChanged) {
			headChanged++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if success != 1 || headChanged != 1 {
		t.Fatalf("expected 1 success and 1 ErrHeadChanged, got success=%d headChanged=%d", success, headChanged)
	}
}

// cascadeRetryCounter aggregates CAS retry telemetry across concurrent
// writers. Each call to the production-side casRetryHook records the
// retry attempt (0-indexed) so callers can derive both the total
// retry count and the per-attempt distribution.
type cascadeRetryCounter struct {
	buckets [casMaxRetries]atomic.Int64
}

func (c *cascadeRetryCounter) record(attempt int) {
	if attempt < 0 || attempt >= len(c.buckets) {
		return
	}
	c.buckets[attempt].Add(1)
}

func (c *cascadeRetryCounter) total() int64 {
	var sum int64
	for i := range c.buckets {
		sum += c.buckets[i].Load()
	}
	return sum
}

// snapshot returns the per-attempt retry counts as a fixed-size slice
// where index N holds the number of failed CAS attempts at retry level N.
func (c *cascadeRetryCounter) snapshot() [casMaxRetries]int64 {
	var out [casMaxRetries]int64
	for i := range c.buckets {
		out[i] = c.buckets[i].Load()
	}
	return out
}

// installRetryHook wires casRetryHook to the provided counter for the
// lifetime of the test and restores the previous hook on cleanup.
func installRetryHook(t testing.TB, c *cascadeRetryCounter) {
	t.Helper()
	prev := casRetryHook
	casRetryHook = func(attempt int) { c.record(attempt) }
	t.Cleanup(func() { casRetryHook = prev })
}

// TestCASRetryDistribution_Ramp ramps concurrent writers across a small
// set of fan-outs and reports the observed CAS retry distribution at
// internal/infra/gitrepo/tx_store.go:28-32. Each writer writes to its
// own stream (low contention on the parent-hash check, high contention
// on refs/heads/main) and to a shared "hot doc" (high contention on
// the parent-hash check). The hook in tx_store.go records every failed
// CAS attempt; from those counts we derive per-write retry behaviour.
func TestCASRetryDistribution_Ramp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CAS retry ramp under -short")
	}
	// 100 writes * 2 phases * 64 writers is too heavy for a -timeout 60s
	// run; we scale writes inversely with fan-out to keep the wall clock
	// bounded while still exercising the CAS retry loop at every level.
	cases := []struct {
		writers int
		writes  int
	}{
		{1, 100},
		{4, 50},
		{16, 25},
		{32, 15},
		{64, 10},
	}

	type result struct {
		writers         int
		ownTotalRetries int64
		ownHist         [casMaxRetries]int64
		hotTotalRetries int64
		hotHist         [casMaxRetries]int64
		ownWrites       int64
		hotWrites       int64
		ownErrors       int64
		hotErrors       int64
	}
	summaries := make([]result, 0, len(cases))

	for _, tc := range cases {
		writers := tc.writers
		writesPerWriter := tc.writes
		t.Run(fmt.Sprintf("writers=%d", writers), func(t *testing.T) {
			ctx := context.Background()
			repoDir := t.TempDir()
			store := NewStore()
			if err := store.Init(ctx, repoDir); err != nil {
				t.Fatalf("Init returned error: %v", err)
			}

			// Seed the hot doc so all writers share a valid parent hash.
			hotStreamPath, hotParent, _ := writeTx(t, ctx, store, repoDir, domain.Transaction{
				TxID:       "01HHOTBASE",
				Timestamp:  1,
				Collection: "hot",
				DocID:      "shared",
				Op:         domain.TxOpPut,
				Snapshot:   []byte(`{"v":0}`),
			})

			// Seed every writer's own doc serially so the concurrent
			// phase only races on patches, not on doc creation.
			ownStreams := make([]string, writers)
			ownParents := make([]string, writers)
			for i := 0; i < writers; i++ {
				sp, ph, _ := writeTx(t, ctx, store, repoDir, domain.Transaction{
					TxID:       fmt.Sprintf("01HOWN%d-BASE", i),
					Timestamp:  int64(100 + i),
					Collection: "own",
					DocID:      fmt.Sprintf("w%d", i),
					Op:         domain.TxOpPut,
					Snapshot:   []byte(`{"v":0}`),
				})
				ownStreams[i] = sp
				ownParents[i] = ph
			}

			// Phase 1: own-doc writes. Each writer owns a unique stream,
			// so the parent-hash check always passes; any failure must
			// come from the CAS retry loop on refs/heads/main.
			ownCounter := &cascadeRetryCounter{}
			installRetryHook(t, ownCounter)

			var ownWrites, ownErrors atomic.Int64
			var wg sync.WaitGroup
			wg.Add(writers)
			for i := 0; i < writers; i++ {
				go func() {
					defer wg.Done()
					streamPath := ownStreams[i]
					currentParent := ownParents[i]
					for j := 0; j < writesPerWriter; j++ {
						tx := domain.Transaction{
							TxID:       fmt.Sprintf("01OWN%d-%d", i, j),
							Timestamp:  int64(1000 + i*writesPerWriter + j),
							Collection: "own",
							DocID:      fmt.Sprintf("w%d", i),
							Op:         domain.TxOpPatch,
							Patch:      []byte(`[{"op":"replace","path":"/v","value":1}]`),
							ParentHash: currentParent,
						}
						write := buildTxWrite(t, repoDir, streamPath, tx)
						res, err := store.PutTx(ctx, write)
						if err != nil {
							// Under high CAS contention go-git may surface
							// "reference not found" / refs-changed errors
							// in addition to domain.ErrHeadChanged; the
							// telemetry hook has already counted the retry.
							ownErrors.Add(1)
							if h, lerr := store.LoadStreamHead(ctx, repoDir, streamPath); lerr == nil && h != "" {
								currentParent = h
							}
							continue
						}
						ownWrites.Add(1)
						currentParent = res.TxHash
					}
				}()
			}
			wg.Wait()

			ownTotal := ownCounter.total()
			ownHist := ownCounter.snapshot()

			// Phase 2: hot-doc writes. All writers contend on the same
			// stream, so the parent-hash check rejects most attempts
			// before they reach the CAS retry loop.
			hotCounter := &cascadeRetryCounter{}
			installRetryHook(t, hotCounter)

			var hotWrites, hotErrors atomic.Int64
			currentHot := atomic.Value{}
			currentHot.Store(hotParent)

			wg = sync.WaitGroup{}
			wg.Add(writers)
			for i := 0; i < writers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < writesPerWriter; j++ {
						parent, _ := currentHot.Load().(string)
						tx := domain.Transaction{
							TxID:       fmt.Sprintf("01HOT%d-%d", i, j),
							Timestamp:  int64(10_000 + i*writesPerWriter + j),
							Collection: "hot",
							DocID:      "shared",
							Op:         domain.TxOpPatch,
							Patch:      []byte(`[{"op":"replace","path":"/v","value":1}]`),
							ParentHash: parent,
						}
						write := buildTxWrite(t, repoDir, hotStreamPath, tx)
						res, err := store.PutTx(ctx, write)
						if err != nil {
							hotErrors.Add(1)
							if h, lerr := store.LoadStreamHead(ctx, repoDir, hotStreamPath); lerr == nil && h != "" {
								currentHot.Store(h)
							}
							continue
						}
						hotWrites.Add(1)
						currentHot.Store(res.TxHash)
					}
				}()
			}
			wg.Wait()

			hotTotal := hotCounter.total()
			hotHist := hotCounter.snapshot()

			r := result{
				writers:         writers,
				ownTotalRetries: ownTotal,
				ownHist:         ownHist,
				hotTotalRetries: hotTotal,
				hotHist:         hotHist,
				ownWrites:       ownWrites.Load(),
				hotWrites:       hotWrites.Load(),
				ownErrors:       ownErrors.Load(),
				hotErrors:       hotErrors.Load(),
			}
			summaries = append(summaries, r)

			t.Logf("writers=%d own: writes=%d errors=%d casRetries=%d hist=%v",
				writers, r.ownWrites, r.ownErrors, r.ownTotalRetries, r.ownHist)
			t.Logf("writers=%d hot: writes=%d errors=%d casRetries=%d hist=%v",
				writers, r.hotWrites, r.hotErrors, r.hotTotalRetries, r.hotHist)

			if writers == 1 {
				if ownTotal != 0 {
					t.Fatalf("single writer should never retry CAS on own doc, got %d", ownTotal)
				}
				if hotTotal != 0 {
					t.Fatalf("single writer should never retry CAS on hot doc, got %d", hotTotal)
				}
			}
			if writers >= 16 {
				// With 16+ concurrent writers we expect at least one
				// CAS retry across either path; otherwise the test
				// would be silently dead.
				if ownTotal == 0 && hotTotal == 0 {
					t.Fatalf("writers=%d: expected at least one CAS retry, observed none", writers)
				}
			}
		})
	}

	// Emit a final summary table so CI logs surface the ramp at a glance.
	t.Logf("CAS retry distribution summary (writers, ownRetries, hotRetries, ownHist[a0..a4], hotHist[a0..a4]):")
	for _, r := range summaries {
		t.Logf("  writers=%2d  ownRetries=%5d  hotRetries=%5d  ownHist=%v  hotHist=%v",
			r.writers, r.ownTotalRetries, r.hotTotalRetries, r.ownHist, r.hotHist)
	}
}

// BenchmarkCASContention is a companion benchmark that drives PutTx
// against a shared "hot" stream via b.RunParallel. It reports allocs
// and ns/op alongside the total CAS retries observed during the run.
func BenchmarkCASContention(b *testing.B) {
	ctx := context.Background()
	repoDir := b.TempDir()
	store := NewStore()
	if err := store.Init(ctx, repoDir); err != nil {
		b.Fatalf("Init returned error: %v", err)
	}

	streamPath, parent, _ := writeTx(b, ctx, store, repoDir, domain.Transaction{
		TxID:       "01BENCHBASE",
		Timestamp:  1,
		Collection: "bench",
		DocID:      "shared",
		Op:         domain.TxOpPut,
		Snapshot:   []byte(`{"v":0}`),
	})

	counter := &cascadeRetryCounter{}
	installRetryHook(b, counter)

	currentHead := atomic.Value{}
	currentHead.Store(parent)

	var writerSeq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := writerSeq.Add(1)
		j := int64(0)
		for pb.Next() {
			j++
			h, _ := currentHead.Load().(string)
			tx := domain.Transaction{
				TxID:       fmt.Sprintf("01BENCH-%d-%d", id, j),
				Timestamp:  id*1_000_000 + j,
				Collection: "bench",
				DocID:      "shared",
				Op:         domain.TxOpPatch,
				Patch:      []byte(`[{"op":"replace","path":"/v","value":1}]`),
				ParentHash: h,
			}
			write := buildTxWrite(b, repoDir, streamPath, tx)
			res, err := store.PutTx(ctx, write)
			if err != nil {
				if errors.Is(err, domain.ErrHeadChanged) {
					if newHead, lerr := store.LoadStreamHead(ctx, repoDir, streamPath); lerr == nil {
						currentHead.Store(newHead)
					}
					continue
				}
				b.Fatalf("PutTx: %v", err)
			}
			currentHead.Store(res.TxHash)
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(counter.total()), "cas_retries")
}

func buildTxWrite(t testing.TB, repoPath, streamPath string, tx domain.Transaction) doc.TxWrite {
	t.Helper()

	encoder := txv3.Encoder{}
	txBytes, err := encoder.Encode(tx)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	txHash := hash.SHA256{}.SumHex(txBytes)
	return doc.TxWrite{
		RepoPath:   repoPath,
		StreamPath: streamPath,
		TxBytes:    txBytes,
		TxHash:     txHash,
		Tx:         tx,
	}
}
