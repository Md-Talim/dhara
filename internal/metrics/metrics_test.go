package metrics

import (
	"strings"
	"testing"
)

func TestHistogram_LESemanticsAndRendering(t *testing.T) {
	h := NewHistogram([]float64{0.01, 0.1, 1})
	h.Observe(0.005) // <= 0.01
	h.Observe(0.01)  // exactly on the bound: still <= 0.01
	h.Observe(0.5)   // <= 1
	h.Observe(5)     // +Inf

	var b strings.Builder
	h.Write(&b, "latency_seconds", "test histogram")
	out := b.String()

	for _, want := range []string{
		"# TYPE latency_seconds histogram",
		`latency_seconds_bucket{le="0.01"} 2`,
		`latency_seconds_bucket{le="0.1"} 2`,
		`latency_seconds_bucket{le="1"} 3`,
		`latency_seconds_bucket{le="+Inf"} 4`,
		"latency_seconds_sum 5.515",
		"latency_seconds_count 4",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
}

func TestHistogram_ZeroValueIsInert(t *testing.T) {
	var h Histogram
	h.Observe(1)

	var b strings.Builder
	h.Write(&b, "x", "x")
	if b.Len() != 0 {
		t.Errorf("zero-value histogram wrote %q, want nothing", b.String())
	}
}
