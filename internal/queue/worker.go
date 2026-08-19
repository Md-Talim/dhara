package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
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
	defer w.logger.Info("worker stopping", "worker_id", w.workerID)

	for {
		task, err := w.claim(ctx)
		if err != nil {
			// No work available or context cancelled: wait before trying again.
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return
			}
		}

		// Got a task: process it immediately, then loop back to claim again.
		if err := w.execute(ctx, task); err != nil {
			w.logger.Error("execute task failed", "err", err)
		}
	}
}

// claim attempts to acquire a pending task from the store.
// Returns the task on success, or an error if no task is available or the claim failed.
func (w *Worker) claim(ctx context.Context) (*store.TaskRow, error) {
	task, err := w.store.Claim(ctx, w.workerID)
	if errors.Is(err, store.ErrTaskNotAvailable) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("claim task: %w", err)
	}
	return task, nil
}

// execute runs the handler for a claimed task, manages heartbeats, and
// persists the result (completed, retry, or dead).
func (w *Worker) execute(ctx context.Context, task *store.TaskRow) error {
	taskLogger := w.taskLogger(task)
	handler, ok := w.registry.Get(task.Type)
	if !ok {
		taskLogger.Warn("no handler registered for task type")
		if err := w.store.MarkDead(ctx, task.ID.String(), w.workerID, "no handler registered for type: "+task.Type, "no handler registered"); err != nil && !errors.Is(err, store.ErrTaskOwnershipLost) {
			return err
		}
		return nil
	}

	taskCtx, cancel := context.WithTimeout(context.Background(), w.handlerTimeout)
	defer cancel()
	taskCtx = ctxlog.WithLogger(taskCtx, taskLogger)

	stopHeartbeat := w.startHeartbeat(taskCtx, task)
	defer stopHeartbeat()

	w.metrics.WorkersInflight.Add(1)
	defer w.metrics.WorkersInflight.Add(-1)

	taskStartTime := time.Now()
	if err := handler(taskCtx, task.Payload); err != nil {
		return w.handleFailure(taskCtx, task, err)
	}

	taskDurationMS := time.Since(taskStartTime).Milliseconds()
	if err := w.store.MarkCompleted(taskCtx, task.ID.String(), w.workerID, taskDurationMS); err != nil {
		if errors.Is(err, store.ErrTaskOwnershipLost) {
			taskLogger.Warn("lost task ownership before marking completed")
			return nil
		}
		return fmt.Errorf("mark task completed: %w", err)
	}

	taskLogger.Info("task completed", "duration_ms", taskDurationMS)
	return nil
}

func (w *Worker) startHeartbeat(heartbeatCtx context.Context, task *store.TaskRow) func() {
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

func (w *Worker) handleFailure(ctx context.Context, task *store.TaskRow, err error) error {
	taskLogger := w.taskLogger(task)
	taskLogger.Warn("task failed", "err", err)

	var storeErr error
	if task.Attempts >= task.MaxRetries {
		storeErr = w.store.MarkDead(ctx, task.ID.String(), w.workerID, err.Error(), "all attempts exhausted")
	} else {
		backoff := w.computeBackoff(task.Attempts)

		nextRunAt := time.Now().Add(backoff)
		storeErr = w.store.MarkPending(ctx, task, w.workerID, err.Error(), nextRunAt)
		if storeErr == nil {
			taskLogger.Info("retrying task",
				"next_run_at", nextRunAt.UTC().Format(time.RFC3339),
				"backoff_ms", backoff.Milliseconds(),
			)
		}
	}

	if errors.Is(storeErr, store.ErrTaskOwnershipLost) {
		taskLogger.Warn("lost task ownership before persisting failure result")
		return nil
	}
	if storeErr != nil {
		return storeErr
	}
	return nil
}

func (w *Worker) taskLogger(task *store.TaskRow) *slog.Logger {
	return w.logger.With(
		"worker_id", w.workerID,
		"task_id", task.ID.String(),
		"task_type", task.Type,
		"attempts", task.Attempts,
		"max_retries", task.MaxRetries,
	)
}

func (w *Worker) computeBackoff(attempts int) time.Duration {
	base := float64(w.baseBackoff)
	cap := float64(w.maxBackoff)
	attempt := float64(max(attempts-1, 0))

	exp := math.Min(cap, base*math.Pow(2, attempt))
	jitter := rand.Float64() * exp
	backoff := time.Duration(jitter)
	return backoff
}
