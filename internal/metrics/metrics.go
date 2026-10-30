package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync/atomic"
)

// Metrics holds process-local counters, gauges and histograms.
//
// Each process owns its own instance: the API server and the worker never
// share memory, so a metric is only meaningful on the process that increments
// it. The two binaries therefore expose different subsets of these fields
// (see Client.MetricsHandler and Worker.MetricsHandler).
type Metrics struct {
	// Owned by the API server: enqueue and cancel only happen through a Client.
	TasksEnqueuedTotal atomic.Int64
	TasksCanceledTotal atomic.Int64

	// Owned by the worker: incremented by the worker's store and goroutines.
	TasksCompletedTotal      atomic.Int64
	TaskAttemptFailuresTotal atomic.Int64
	TasksRetriedTotal        atomic.Int64
	TasksDeadTotal           atomic.Int64
	TasksReapedTotal         atomic.Int64
	ReaperFailuresTotal      atomic.Int64
	ClaimErrorsTotal         atomic.Int64

	WorkersActive   atomic.Int64
	WorkersInflight atomic.Int64

	// TaskExecutionDuration histograms handler execution time, excluding
	// persistence. The zero value is inert: Observe and Write are no-ops.
	TaskExecutionDuration Histogram
}

func New() *Metrics {
	return &Metrics{
		TaskExecutionDuration: NewHistogram([]float64{
			0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
			1, 2.5, 5, 10, 30, 60, 300,
		}),
	}
}

// Histogram is a fixed-bucket Prometheus histogram. Bucket counts are stored
// individually and accumulated at render time.
type Histogram struct {
	bounds []float64
	counts []atomic.Int64 // one per bound, plus the +Inf bucket
	sum    atomic.Int64   // total observed, in nanoseconds
}

func NewHistogram(bounds []float64) Histogram {
	return Histogram{
		bounds: append([]float64(nil), bounds...),
		counts: make([]atomic.Int64, len(bounds)+1),
	}
}

// Observe records v (in seconds). Safe for concurrent use.
func (h *Histogram) Observe(v float64) {
	if len(h.counts) == 0 {
		return
	}
	// SearchFloat64s returns the first index with bounds[i] >= v, which is
	// exactly the bucket v belongs to under Prometheus' le (<=) semantics.
	i := sort.SearchFloat64s(h.bounds, v)
	h.counts[i].Add(1)
	h.sum.Add(int64(v * 1e9))
}

// Write renders the histogram in Prometheus text exposition format.
func (h *Histogram) Write(w io.Writer, name, help string) {
	if len(h.counts) == 0 {
		return
	}

	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)

	var cumulative int64
	for i, bound := range h.bounds {
		cumulative += h.counts[i].Load()
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, formatFloat(bound), cumulative)
	}
	cumulative += h.counts[len(h.bounds)].Load()
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	fmt.Fprintf(w, "%s_sum %s\n", name, formatFloat(float64(h.sum.Load())/1e9))
	fmt.Fprintf(w, "%s_count %d\n", name, cumulative)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
