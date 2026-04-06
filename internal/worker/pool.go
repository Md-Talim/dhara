package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/md-talim/relay/internal/store"
	"github.com/md-talim/relay/internal/tasks"
)

type WorkerPool struct {
	store        store.TaskStore
	logger       *slog.Logger
	pollInterval time.Duration

	workerPrefix string
	concurrency  int

	mu       sync.RWMutex
	registry tasks.HandlerRegistry

	started atomic.Bool
}

func NewWorkerPool(
	store store.TaskStore,
	registry tasks.HandlerRegistry,
	logger *slog.Logger,
	workerPrefix string,
	concurrency int,
	pollInterval time.Duration,
) *WorkerPool {
	if logger == nil {
		logger = slog.Default()
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	if workerPrefix == "" {
		workerPrefix = "relay-worker"
	}

	return &WorkerPool{
		store:        store,
		logger:       logger.With("component", "worker_pool"),
		registry:     registry,
		pollInterval: pollInterval,
		workerPrefix: workerPrefix,
		concurrency:  concurrency,
	}
}

func (p *WorkerPool) Started() bool {
	return p.started.Load()
}

func (p *WorkerPool) Start(ctx context.Context) {
	if p.started.Swap(true) {
		// already started; ignore
		return
	}

	p.logger.Info(
		"starting worker pool",
		"concurrency", p.concurrency,
		"poll_interval_ms", p.pollInterval.Microseconds(),
	)

	for i := 0; i < p.concurrency; i++ {
		workerID := fmt.Sprintf("%s-%d", p.workerPrefix, i+1)
		w := p.newWorker(workerID)
		go w.Start(ctx)
	}
}

func (p *WorkerPool) newWorker(workerID string) *Worker {
	return &Worker{
		workerID:     workerID,
		store:        p.store,
		logger:       p.logger.With("worker_id", workerID),
		registry:     p.registry,
		pollInterval: p.pollInterval,
	}
}
