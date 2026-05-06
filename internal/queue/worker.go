package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/md-talim/dhara/internal/ctxlog"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

type Worker struct {
	workerID          string
	store             store.TaskStore
	registry          tasks.HandlerRegistry
	logger            *slog.Logger
	metrics           *metrics.Metrics
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	handlerTimeout    time.Duration
	baseBackoff       time.Duration
	maxBackoff        time.Duration
}

func (w *Worker) Start(ctx context.Context) {
	w.metrics.WorkersTotal.Add(1)
	defer w.metrics.WorkersTotal.Add(-1)

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	w.logger.Info("worker started", "worker_id", w.workerID)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker stopping", "worker_id", w.workerID)
			return
		case <-ticker.C:
			if err := w.processNext(ctx); err != nil {
				w.logger.Error("process next task failed", "err", err)
			}
		}
	}
}

func (w *Worker) processNext(ctx context.Context) error {
	task, err := w.store.Claim(ctx, w.workerID)
	if errors.Is(err, store.ErrTaskNotAvailable) {
		return nil // nothing to do
	}
	if err != nil {
		return fmt.Errorf("claim task: %w", err)
	}

	taskLogger := w.taskLogger(task)
	handler, ok := w.registry.Get(task.Type)
	if !ok {
		taskLogger.Warn("no handler registered for task type")
		return w.store.MarkDead(ctx, task.ID.String(), w.workerID, "no handler registered for type: "+task.Type, "no handler registered")
	}

	taskCtx, cancel := context.WithTimeout(context.Background(), w.handlerTimeout)
	defer cancel()

	taskCtx = ctxlog.WithLogger(taskCtx, taskLogger)

	stopHeartbeat := w.startHeartbeat(taskCtx, task)
	defer stopHeartbeat()

	w.metrics.WorkersInflight.Add(1)
	defer w.metrics.WorkersInflight.Add(-1)

	taskStartTime := time.Now()
	if err = handler(taskCtx, task.Payload); err != nil {
		return w.handleFailure(taskCtx, task, err)
	}

	taskDurationMS := time.Since(taskStartTime).Milliseconds()
	taskLogger.Info("task completed", "duration_ms", taskDurationMS)
	return w.store.MarkCompleted(taskCtx, task.ID.String(), w.workerID, taskDurationMS)
}

func (w *Worker) startHeartbeat(heartbeatCtx context.Context, task *store.Task) func() {
	heartbeatCtx, cancel := context.WithCancel(heartbeatCtx)
	taskLogger := w.taskLogger(task)

	go func() {
		ticker := time.NewTicker(w.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := w.store.Heartbeat(heartbeatCtx, task.ID.String(), w.workerID); err != nil {
					if errors.Is(err, store.ErrTaskOwnershipLost) {
						taskLogger.Warn("lost task ownership during heartbeat")
					} else {
						taskLogger.Warn("heartbeat failed", "err", err)
					}
					cancel()
					return
				}
			}
		}
	}()

	return cancel
}

func (w *Worker) handleFailure(ctx context.Context, task *store.Task, err error) error {
	taskLogger := w.taskLogger(task)
	taskLogger.Warn("task failed", "err", err)

	if task.Attempts >= task.MaxRetries {
		return w.store.MarkDead(ctx, task.ID.String(), w.workerID, err.Error(), "all attempts exhausted")
	}

	// exponential backoff: 10s, 20s, 40s, 80s...
	backoff := w.baseBackoff * (1 << max(task.Attempts-1, 0))
	if backoff > w.maxBackoff {
		backoff = w.maxBackoff
	}
	nextRunAt := time.Now().Add(backoff)

	taskLogger.Info("retrying task",
		"next_run_at", nextRunAt.UTC().Format(time.RFC3339),
		"backoff_ms", backoff.Milliseconds(),
	)

	return w.store.MarkPending(ctx, task, w.workerID, err.Error(), nextRunAt)
}

func (w *Worker) taskLogger(task *store.Task) *slog.Logger {
	return w.logger.With(
		"worker_id", w.workerID,
		"task_id", task.ID.String(),
		"task_type", task.Type,
		"attempts", task.Attempts,
		"max_retries", task.MaxRetries,
	)
}
