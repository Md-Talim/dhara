package dhara

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/queue"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

type WorkerOption func(*workerConfig)

type workerConfig struct {
	workerPrefix      string
	maxWorkers        int
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	handlerTimeout    time.Duration
	baseBackoff       time.Duration
	maxBackoff        time.Duration
	reaperInterval    time.Duration
	stuckThreshold    time.Duration
	shutdownTimeout   time.Duration
	logger            *slog.Logger
}

func defaultWorkerConfig() workerConfig {
	return workerConfig{
		workerPrefix:      "dhara-worker",
		maxWorkers:        5,
		pollInterval:      time.Second,
		heartbeatInterval: 30 * time.Second,
		handlerTimeout:    5 * time.Minute,
		baseBackoff:       time.Second,
		maxBackoff:        5 * time.Minute,
		reaperInterval:    30 * time.Second,
		stuckThreshold:    5 * time.Minute,
		shutdownTimeout:   30 * time.Second,
		logger:            slog.Default(),
	}
}

func WithWorkerPrefix(prefix string) WorkerOption {
	return func(c *workerConfig) { c.workerPrefix = prefix }
}

func WithMaxWorkers(n int) WorkerOption {
	return func(c *workerConfig) { c.maxWorkers = n }
}

func WithPollInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.pollInterval = d }
}

func WithHeartbeatInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.heartbeatInterval = d }
}

func WithHandlerTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.handlerTimeout = d }
}

func WithBaseBackoff(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.baseBackoff = d }
}

func WithMaxBackoff(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.maxBackoff = d }
}

func WithReaperInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.reaperInterval = d }
}

func WithStuckThreshold(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.stuckThreshold = d }
}

func WithShutdownTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.shutdownTimeout = d }
}

func WithLogger(l *slog.Logger) WorkerOption {
	return func(c *workerConfig) { c.logger = l }
}

type Worker struct {
	pool     *pgxpool.Pool
	registry *tasks.MapRegistry
	config   workerConfig

	store   *store.PostgresTaskStore
	metrics *metrics.Metrics

	started  atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewWorker(pool *pgxpool.Pool, opts ...WorkerOption) *Worker {
	cfg := defaultWorkerConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	m := metrics.New()
	return &Worker{
		pool:     pool,
		registry: tasks.NewRegistry(nil),
		config:   cfg,
		store:    store.NewTaskStore(pool, m),
		metrics:  m,
		stopCh:   make(chan struct{}),
	}
}

func (w *Worker) RegisterHandler(taskType string, handler HandlerFunc) {
	if w.started.Load() {
		panic("dhara: RegisterHandler called after Start")
	}
	w.registry.Add(taskType, handler)
}

func (w *Worker) Start(ctx context.Context) error {
	if w.started.Swap(true) {
		return errors.New("dhara: worker already started")
	}

	wp := queue.NewWorkerPool(w.store, w.registry, w.config.logger, queue.Settings{
		WorkerPrefix:      w.config.workerPrefix,
		Concurrency:       w.config.maxWorkers,
		PollInterval:      w.config.pollInterval,
		HeartbeatInterval: w.config.heartbeatInterval,
		HandlerTimeout:    w.config.handlerTimeout,
		BaseBackoff:       w.config.baseBackoff,
		MaxBackoff:        w.config.maxBackoff,
		ReaperInterval:    w.config.reaperInterval,
		StaleThreshold:    w.config.stuckThreshold,
	}, w.metrics)

	wp.Start(ctx)

	select {
	case <-ctx.Done():
	case <-w.stopCh:
	}

	wp.Stop()

	waitCtx, cancel := context.WithTimeout(context.Background(), w.config.shutdownTimeout)
	defer cancel()
	return wp.Wait(waitCtx)
}

func (w *Worker) Stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}
