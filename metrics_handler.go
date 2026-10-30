package dhara

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/md-talim/dhara/internal/metrics"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// MetricsHandler renders this client's process-local counters alongside the
// Postgres-derived queue gauges.
//
// It always responds 200. When Postgres cannot be read, queue_metrics_up is 0
// and the gauge series are omitted, so counters keep flowing during the
// outage you are trying to debug.
func (c *Client) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		w.Header().Set("Content-Type", prometheusContentType)
		w.Header().Set("Cache-Control", "no-store")

		qm, err := c.QueueMetrics(ctx)
		if err != nil {
			writeGauge(w, "queue_metrics_up", "Whether the Postgres-derived queue gauges were readable at scrape time.", 0)
		} else {
			writeGauge(w, "queue_metrics_up", "Whether the Postgres-derived queue gauges were readable at scrape time.", 1)
			writeQueueMetrics(w, qm)
		}

		writeServerMetrics(w, c.metrics)
	})
}

// MetricsHandler renders the worker's process-local counters, gauges and
// execution-latency histogram. It never touches Postgres, so it stays
// scrapeable while the database is down.
func (w *Worker) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", prometheusContentType)
		rw.Header().Set("Cache-Control", "no-store")
		writeWorkerMetrics(rw, w.metrics)
	})
}

func writeQueueMetrics(w io.Writer, qm *QueueMetrics) {
	fmt.Fprint(w, "# HELP tasks_by_status Current number of tasks per status.\n")
	fmt.Fprint(w, "# TYPE tasks_by_status gauge\n")
	fmt.Fprintf(w, "tasks_by_status{status=%q} %d\n", TaskStatusPending, qm.TasksPending)
	fmt.Fprintf(w, "tasks_by_status{status=%q} %d\n", TaskStatusRunning, qm.TasksRunning)
	fmt.Fprintf(w, "tasks_by_status{status=%q} %d\n", TaskStatusCompleted, qm.TasksCompleted)
	fmt.Fprintf(w, "tasks_by_status{status=%q} %d\n", TaskStatusCanceled, qm.TasksCanceled)
	fmt.Fprintf(w, "tasks_by_status{status=%q} %d\n", TaskStatusDead, qm.TasksDead)

	fmt.Fprint(w, "# HELP tasks_pending_breakdown Pending tasks split by readiness.\n")
	fmt.Fprint(w, "# TYPE tasks_pending_breakdown gauge\n")
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"ready\"} %d\n", qm.TasksPendingReady)
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"delayed\"} %d\n", qm.TasksPendingDelayed)
}

func writeServerMetrics(w io.Writer, m *metrics.Metrics) {
	writeCounter(w, "tasks_enqueued_total", "Tasks enqueued through this process.", m.TasksEnqueuedTotal.Load())
	writeCounter(w, "tasks_canceled_total", "Tasks canceled through this process.", m.TasksCanceledTotal.Load())
}
func writeWorkerMetrics(w io.Writer, m *metrics.Metrics) {
	writeCounter(w, "tasks_completed_total", "Tasks completed by this process.", m.TasksCompletedTotal.Load())
	writeCounter(w, "tasks_attempt_failures_total", "Handler attempts that returned an error.", m.TaskAttemptFailuresTotal.Load())
	writeCounter(w, "tasks_retried_total", "Failed attempts scheduled for retry.", m.TasksRetriedTotal.Load())
	writeCounter(w, "tasks_dead_total", "Tasks dead-lettered by this process.", m.TasksDeadTotal.Load())
	writeCounter(w, "tasks_reaped_total", "Stale tasks requeued or dead-lettered by the reaper.", m.TasksReapedTotal.Load())
	writeCounter(w, "reaper_failures_total", "Reaper runs that failed to complete.", m.ReaperFailuresTotal.Load())
	writeCounter(w, "worker_claim_errors_total", "Claim calls that failed for reasons other than an empty queue.", m.ClaimErrorsTotal.Load())

	writeGauge(w, "workers_active", "Worker goroutines currently polling.", m.WorkersActive.Load())
	writeGauge(w, "workers_inflight", "Handlers currently executing.", m.WorkersInflight.Load())

	m.TaskExecutionDuration.Write(w, "task_execution_duration_seconds", "Handler execution time in seconds, excluding persistence.")
}

func writeCounter(w io.Writer, name, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	fmt.Fprintf(w, "%s %d\n", name, v)
}

func writeGauge(w io.Writer, name, help string, v int64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	fmt.Fprintf(w, "%s %d\n", name, v)
}
