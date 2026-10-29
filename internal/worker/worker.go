package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/md-talim/dhara/dharatype"
	"github.com/md-talim/dhara/internal/ctxlog"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

// persistTimeout bounds the terminal store write (MarkCompleted/MarkPending/
// MarkDead). It is deliberately independent of the handler deadline: a
// handler that consumed its whole budget must still be able to record its
// outcome instead of leaving the row RUNNING for the reaper.
const persistTimeout = 10 * time.Second

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
	if errors.Is(err, dharatype.ErrTaskNotAvailable) {
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
		persistCtx, cancel := w.persistContext(task)
		defer cancel()
		err := w.store.MarkDead(persistCtx, task.ID.String(), w.workerID, "no handler registered for type: "+task.Type, "no handler registered")
		if err != nil && !errors.Is(err, dharatype.ErrTaskOwnershipLost) {
			return err
		}
		return nil
	}

	taskCtx, cancel := context.WithTimeout(ctx, w.handlerTimeout)
	defer cancel()
	taskCtx = ctxlog.WithLogger(taskCtx, taskLogger)

	stopHeartbeat := w.startHeartbeat(taskCtx, task)
	defer stopHeartbeat()

	w.metrics.WorkersInflight.Add(1)
	defer w.metrics.WorkersInflight.Add(-1)

	taskStartTime := time.Now()
	handlerErr := w.runTaskHandler(taskCtx, task, handler)

	persistCtx, cancelPersist := w.persistContext(task)
	defer cancelPersist()

	if handlerErr != nil {
		return w.handleFailure(persistCtx, task, handlerErr)
	}

	taskDurationMS := time.Since(taskStartTime).Milliseconds()
	if err := w.store.MarkCompleted(persistCtx, task.ID.String(), w.workerID, taskDurationMS); err != nil {
		if errors.Is(err, dharatype.ErrTaskOwnershipLost) {
			taskLogger.Warn("lost task ownership before marking completed")
			return nil
		}
		return fmt.Errorf("mark task completed: %w", err)
	}

	taskLogger.Info("task completed", "duration_ms", taskDurationMS)
	return nil
}

// runTaskHandler invokes the handler with a panic guard. A panic in user code is
// converted into an ordinary handler error so it flows through the normal
// retry/backoff/dead-letter path instead of terminating the process.
func (w *Worker) runTaskHandler(ctx context.Context, task *store.TaskRow, handler tasks.HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
			w.taskLogger(task).Error("handler panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	return handler(ctx, task.Payload)
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
				err := w.store.Heartbeat(heartbeatCtx, task.ID.String(), w.workerID)
				if err == nil {
					continue
				}
				if errors.Is(err, dharatype.ErrTaskOwnershipLost) {
					taskLogger.Warn("lost task ownership during heartbeat")
					cancel()
					return
				}
				// Transient (pool timeout, connection reset): keep beating.
				taskLogger.Warn("heartbeat failed, will retry next tick", "err", err)
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

	if errors.Is(storeErr, dharatype.ErrTaskOwnershipLost) {
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

// persistContext returns a fresh, bounded context for a terminal store write,
// tagged with the task logger. It must not derive from the handler context:
// that one may already be expired by the time we need to persist.
func (w *Worker) persistContext(task *store.TaskRow) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	return ctxlog.WithLogger(ctx, w.taskLogger(task)), cancel
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
