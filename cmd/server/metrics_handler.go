package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/md-talim/dhara"
)

type metricsHandler struct {
	client *dhara.Client
}

func newMetricsHandler(c *dhara.Client) *metricsHandler {
	return &metricsHandler{client: c}
}

func (h *metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	qm, err := h.client.QueueMetrics(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "metrics unavailable")
		return
	}

	m := h.client.Metrics()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "# TYPE tasks_by_status gauge\n")
	fmt.Fprintf(w, "tasks_by_status{status=\"%s\"} %d\n", dhara.TaskStatusPending, qm.TasksPending)
	fmt.Fprintf(w, "tasks_by_status{status=\"%s\"} %d\n", dhara.TaskStatusRunning, qm.TasksRunning)
	fmt.Fprintf(w, "tasks_by_status{status=\"%s\"} %d\n", dhara.TaskStatusCompleted, qm.TasksCompleted)
	fmt.Fprintf(w, "tasks_by_status{status=\"%s\"} %d\n", dhara.TaskStatusCanceled, qm.TasksCanceled)
	fmt.Fprintf(w, "tasks_by_status{status=\"%s\"} %d\n", dhara.TaskStatusDead, qm.TasksDead)

	fmt.Fprintf(w, "# TYPE tasks_pending_breakdown gauge\n")
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"ready\"} %d\n", qm.TasksPendingReady)
	fmt.Fprintf(w, "tasks_pending_breakdown{status=\"delayed\"} %d\n", qm.TasksPendingDelayed)

	fmt.Fprintf(w, "# TYPE tasks_enqueued_total counter\n")
	fmt.Fprintf(w, "tasks_enqueued_total %d\n", m.TasksEnqueuedTotal)

	fmt.Fprintf(w, "# TYPE tasks_completed_total counter\n")
	fmt.Fprintf(w, "tasks_completed_total %d\n", m.TasksCompletedTotal)

	fmt.Fprintf(w, "# TYPE tasks_attempt_failures_total counter\n")
	fmt.Fprintf(w, "tasks_attempt_failures_total %d\n", m.TaskAttemptFailuresTotal)

	fmt.Fprintf(w, "# TYPE tasks_retried_total counter\n")
	fmt.Fprintf(w, "tasks_retried_total %d\n", m.TasksRetriedTotal)

	fmt.Fprintf(w, "# TYPE tasks_dead_total counter\n")
	fmt.Fprintf(w, "tasks_dead_total %d\n", m.TasksDeadTotal)

	fmt.Fprintf(w, "# TYPE tasks_canceled_total counter\n")
	fmt.Fprintf(w, "tasks_canceled_total %d\n", m.TasksCanceledTotal)

	fmt.Fprintf(w, "# TYPE tasks_reaped_total counter\n")
	fmt.Fprintf(w, "tasks_reaped_total %d\n", m.TasksReapedTotal)

	fmt.Fprintf(w, "# TYPE workers_total gauge\n")
	fmt.Fprintf(w, "workers_total %d\n", m.WorkersTotal)

	fmt.Fprintf(w, "# TYPE workers_inflight gauge\n")
	fmt.Fprintf(w, "workers_inflight %d\n", m.WorkersInflight)
}
