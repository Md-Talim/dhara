package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

type WorkerPool struct {
	store    store.TaskStore
	logger   *slog.Logger
	settings Settings

	mu       sync.RWMutex
	registry tasks.HandlerRegistry
	started  atomic.Bool

	metrics *metrics.Metrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorkerPool(
	store store.TaskStore,
	registry tasks.HandlerRegistry,
	logger *slog.Logger,
	settings Settings,
	m *metrics.Metrics,
) *WorkerPool {
	if logger == nil {
		logger = slog.Default()
	}
	settings.normalize()

	return &WorkerPool{
		store:    store,
		logger:   logger.With("component", "worker_pool"),
		registry: registry,
		settings: settings,
		metrics:  m,
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

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.Info(
		"starting worker pool",
		"concurrency", p.settings.Concurrency,
		"poll_interval_ms", p.settings.PollInterval.Milliseconds(),
		"reaper_interval_ms", p.settings.ReaperInterval.Milliseconds(),
	)

	for i := 0; i < p.settings.Concurrency; i++ {
		workerID := fmt.Sprintf("%s-%d", p.settings.WorkerPrefix, i+1)
		w := p.newWorker(workerID)

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			w.Start(p.ctx)
		}()
	}

	reaperID := fmt.Sprintf("%s-reaper", p.settings.WorkerPrefix)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		newReaper(
			p.store,
			p.logger,
			p.settings.ReaperInterval,
			p.settings.StaleThreshold,
			reaperID,
		).start(p.ctx)
	}()
}

func (p *WorkerPool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *WorkerPool) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (p *WorkerPool) newWorker(workerID string) *Worker {
	return &Worker{
		workerID:          workerID,
		store:             p.store,
		logger:            p.logger.With("worker_id", workerID),
		metrics:           p.metrics,
		registry:          p.registry,
		pollInterval:      p.settings.PollInterval,
		heartbeatInterval: p.settings.HeartbeatInterval,
		handlerTimeout:    p.settings.HandlerTimeout,
		baseBackoff:       p.settings.BaseBackoff,
		maxBackoff:        p.settings.MaxBackoff,
	}
}

type Settings struct {
	WorkerPrefix      string
	PollInterval      time.Duration
	Concurrency       int
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
	ReaperInterval    time.Duration
	StaleThreshold    time.Duration
}

func (s *Settings) normalize() {
	if s.WorkerPrefix == "" {
		s.WorkerPrefix = "dhara-worker"
	}
	if s.Concurrency <= 0 {
		s.Concurrency = 1
	}
	if s.PollInterval <= 0 {
		s.PollInterval = 250 * time.Millisecond
	}
	if s.HeartbeatInterval <= 0 {
		s.HeartbeatInterval = 30 * time.Second
	}
	if s.ReaperInterval <= 0 {
		s.ReaperInterval = 30 * time.Second
	}
	if s.StaleThreshold <= 0 {
		s.StaleThreshold = 5 * time.Minute
	}
	if s.HandlerTimeout <= 0 {
		s.HandlerTimeout = 5 * time.Minute
	}
	if s.BaseBackoff <= 0 {
		s.BaseBackoff = 10 * time.Second
	}
	if s.MaxBackoff <= 0 {
		s.MaxBackoff = 5 * time.Minute
	}
	if s.BaseBackoff > s.MaxBackoff {
		s.BaseBackoff = s.MaxBackoff
	}
}
