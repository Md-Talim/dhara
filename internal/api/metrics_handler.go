package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
)

type MetricsHandler struct {
	Store   store.MetricsReader
	Metrics *metrics.Metrics
}

func NewMetricsHandler(s store.MetricsReader, m *metrics.Metrics) *MetricsHandler {
	return &MetricsHandler{
		Store:   s,
		Metrics: m,
	}
}

func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbm, err := h.Store.GetQueueDBMetrics(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "metrics unavailable")
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "# TYPE tasks_by_status gauge\n")
	fmt.Fprintf(w, "tasks_by_status{status=\"PENDING\"} %d\n", dbm.TasksPending)
	fmt.Fprintf(w, "tasks_by_status{status=\"RUNNING\"} %d\n", dbm.TasksRunning)
	fmt.Fprintf(w, "tasks_by_status{status=\"COMPLETED\"} %d\n", dbm.TasksCompleted)
	fmt.Fprintf(w, "tasks_by_status{status=\"CANCELED\"} %d\n", dbm.TasksCanceled)
	fmt.Fprintf(w, "tasks_by_status{status=\"DEAD\"} %d\n", dbm.TasksDead)

	fmt.Fprintf(w, "# TYPE tasks_pending_breakdown gauge\n")
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"ready\"} %d\n", dbm.TasksPendingReady)
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"delayed\"} %d\n", dbm.TasksPendingDelayed)

	fmt.Fprintf(w, "# TYPE tasks_enqueued_total counter\n")
	fmt.Fprintf(w, "tasks_enqueued_total %d\n", h.Metrics.TasksEnqueuedTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_completed_total counter\n")
	fmt.Fprintf(w, "tasks_completed_total %d\n", h.Metrics.TasksCompletedTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_attempt_failures_total counter\n")
	fmt.Fprintf(w, "tasks_attempt_failures_total %d\n", h.Metrics.TaskAttemptFailuresTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_retried_total counter\n")
	fmt.Fprintf(w, "tasks_retried_total %d\n", h.Metrics.TasksRetriedTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_dead_total counter\n")
	fmt.Fprintf(w, "tasks_dead_total %d\n", h.Metrics.TasksDeadTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_canceled_total counter\n")
	fmt.Fprintf(w, "tasks_canceled_total %d\n", h.Metrics.TasksCanceledTotal.Load())

	fmt.Fprintf(w, "# TYPE tasks_reaped_total counter\n")
	fmt.Fprintf(w, "tasks_reaped_total %d\n", h.Metrics.TasksReapedTotal.Load())

	fmt.Fprintf(w, "# TYPE workers_total gauge\n")
	fmt.Fprintf(w, "workers_total %d\n", h.Metrics.WorkersTotal.Load())

	fmt.Fprintf(w, "# TYPE workers_inflight gauge\n")
	fmt.Fprintf(w, "workers_inflight %d\n", h.Metrics.WorkersInflight.Load())
}
