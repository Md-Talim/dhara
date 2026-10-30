package dhara

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/md-talim/dhara/internal/metrics"
)

func TestWriteServerMetrics_OnlyServerOwned(t *testing.T) {
	m := metrics.New()
	m.TasksEnqueuedTotal.Add(3)
	m.TasksCanceledTotal.Add(1)
	m.TasksCompletedTotal.Add(99) // worker-owned: must not leak

	var b strings.Builder
	writeServerMetrics(&b, m)
	out := b.String()

	for _, want := range []string{
		"tasks_enqueued_total 3",
		"tasks_canceled_total 1",
		"# TYPE tasks_enqueued_total counter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q\n%s", want, out)
		}
	}
	for _, notWant := range []string{"tasks_completed_total", "workers_active", "reaper_failures_total"} {
		if strings.Contains(out, notWant) {
			t.Errorf("server output leaked worker-owned %q\n%s", notWant, out)
		}
	}
}

func TestWriteWorkerMetrics(t *testing.T) {
	m := metrics.New()
	m.TasksCompletedTotal.Add(2)
	m.TaskAttemptFailuresTotal.Add(1)
	m.TasksDeadTotal.Add(1)
	m.ClaimErrorsTotal.Add(4)
	m.WorkersActive.Add(5)
	m.WorkersInflight.Add(2)
	m.TaskExecutionDuration.Observe(0.02)

	var b strings.Builder
	writeWorkerMetrics(&b, m)
	out := b.String()

	for _, want := range []string{
		"tasks_completed_total 2",
		"tasks_attempt_failures_total 1",
		"tasks_dead_total 1",
		"worker_claim_errors_total 4",
		"workers_active 5",
		"workers_inflight 2",
		`task_execution_duration_seconds_bucket{le="+Inf"} 1`,
		"task_execution_duration_seconds_count 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "tasks_enqueued_total") {
		t.Error("worker output leaked server-owned tasks_enqueued_total")
	}
}

func TestWriteQueueMetrics(t *testing.T) {
	qm := &QueueMetrics{
		TasksPending:        4,
		TasksRunning:        1,
		TasksCompleted:      10,
		TasksCanceled:       2,
		TasksDead:           3,
		TasksPendingReady:   3,
		TasksPendingDelayed: 1,
	}

	var b strings.Builder
	writeQueueMetrics(&b, qm)
	out := b.String()

	for _, want := range []string{
		`tasks_by_status{status="PENDING"} 4`,
		`tasks_by_status{status="RUNNING"} 1`,
		`tasks_by_status{status="COMPLETED"} 10`,
		`tasks_by_status{status="CANCELED"} 2`,
		`tasks_by_status{status="DEAD"} 3`,
		`tasks_pending_breakdown{status="ready"} 3`,
		`tasks_pending_breakdown{status="delayed"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q\n%s", want, out)
		}
	}
}

func TestWorkerMetricsHandler(t *testing.T) {
	w := NewWorker(nil) // the handler never touches the pool
	w.metrics.TasksCompletedTotal.Add(2)
	w.metrics.WorkersActive.Add(1)
	w.metrics.TaskExecutionDuration.Observe(0.02)

	rec := httptest.NewRecorder()
	w.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != prometheusContentType {
		t.Errorf("Content-Type = %q, want %q", got, prometheusContentType)
	}
	if !strings.Contains(rec.Body.String(), "tasks_completed_total 2") {
		t.Errorf("body missing completed counter\n%s", rec.Body.String())
	}
}
