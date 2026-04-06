package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/md-talim/relay/internal/ctxlog"
	"github.com/md-talim/relay/internal/store"
	"github.com/md-talim/relay/internal/tasks"
)

type Worker struct {
	workerID     string
	store        store.TaskStore
	registry     tasks.HandlerRegistry
	logger       *slog.Logger
	pollInterval time.Duration
}

func (w *Worker) Start(ctx context.Context) {
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
		return w.store.MarkDead(ctx, task.ID.String(), "no handler registered for type: "+task.Type, "no handler registered")
	}

	handlerCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	handlerCtx = ctxlog.WithLogger(handlerCtx, taskLogger)

	taskStartTime := time.Now()
	if err = handler(handlerCtx, task.Payload); err != nil {
		return w.handleFailure(ctx, task, err)
	}

	taskDurationMS := time.Since(taskStartTime).Milliseconds()
	taskLogger.Info("task completed")
	return w.store.MarkCompleted(ctx, task.ID.String(), w.workerID, taskDurationMS)
}

func (w *Worker) handleFailure(ctx context.Context, task *store.Task, err error) error {
	taskLogger := w.taskLogger(task)
	taskLogger.Warn("task failed", "err", err)

	// TODO: handle retries and backoff; for now mark dead to keep demo predictable
	return w.store.MarkDead(ctx, task.ID.String(), err.Error(), "all attempts exhausted")
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
