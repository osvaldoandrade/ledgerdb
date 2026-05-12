package gitrepo

import (
	"testing"
	"time"
)

// fixedBackoff replicates the pre-jitter exponential schedule (base<<attempt)
// so we can benchmark the wait pattern alone, without the surrounding git ops.
func fixedBackoff(attempt int) time.Duration {
	return casBackoffBase * time.Duration(1<<attempt)
}

// BenchmarkFixedBackoffSchedule measures the worst-case cumulative wait
// across all CAS attempts under the pre-jitter exponential schedule.
func BenchmarkFixedBackoffSchedule(b *testing.B) {
	b.ReportAllocs()
	var total time.Duration
	for i := 0; i < b.N; i++ {
		total = 0
		for attempt := 0; attempt < casMaxRetries; attempt++ {
			total += fixedBackoff(attempt)
		}
	}
	b.ReportMetric(float64(total.Microseconds()), "worst_us")
}

// BenchmarkJitteredBackoffSchedule measures the cumulative wait across all
// CAS attempts using full jitter; on average this is half the fixed schedule.
func BenchmarkJitteredBackoffSchedule(b *testing.B) {
	b.ReportAllocs()
	var total time.Duration
	for i := 0; i < b.N; i++ {
		total = 0
		for attempt := 0; attempt < casMaxRetries; attempt++ {
			total += jitteredBackoff(attempt)
		}
	}
	b.ReportMetric(float64(total.Microseconds()), "sample_us")
}
