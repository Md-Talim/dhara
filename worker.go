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
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
	"github.com/md-talim/dhara/internal/worker"
)

// WorkerOption configures a Worker.
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

// WithWorkerPrefix sets the identity prefix for worker IDs and reaper logs.
// Default "dhara-worker".
func WithWorkerPrefix(prefix string) WorkerOption {
	return func(c *workerConfig) { c.workerPrefix = prefix }
}

// WithMaxWorkers sets the number of concurrent handler goroutines. Default 5.
func WithMaxWorkers(n int) WorkerOption {
	return func(c *workerConfig) { c.maxWorkers = n }
}

// WithPollInterval sets how long an idle worker waits before polling again.
// Default 1s.
func WithPollInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.pollInterval = d }
}

// WithHeartbeatInterval sets how often running tasks refresh their lock.
// Keep it well below WithStuckThreshold. Default 30s.
func WithHeartbeatInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.heartbeatInterval = d }
}

// WithHandlerTimeout bounds a single handler invocation. Default 5m.
func WithHandlerTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.handlerTimeout = d }
}

// WithBaseBackoff sets the initial retry delay. Default 1s.
func WithBaseBackoff(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.baseBackoff = d }
}

// WithMaxBackoff caps the retry delay. Default 5m.
func WithMaxBackoff(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.maxBackoff = d }
}

// WithReaperInterval sets how often stale RUNNING tasks are scanned.
// Default 30s.
func WithReaperInterval(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.reaperInterval = d }
}

// WithStuckThreshold sets how long a task may go without a heartbeat before
// the reaper requeues it. Default 5m.
func WithStuckThreshold(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.stuckThreshold = d }
}

// WithShutdownTimeout bounds how long Start waits for in-flight handlers
// after its context is cancelled. Default 30s.
func WithShutdownTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) { c.shutdownTimeout = d }
}

// WithLogger sets the structured logger. Default slog.Default().
func WithLogger(l *slog.Logger) WorkerOption {
	return func(c *workerConfig) { c.logger = l }
}

// Worker claims and executes tasks. Register handlers before calling Start.
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

// NewWorker returns a Worker that claims tasks from pool. It starts no
// goroutines until Start is called.
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

// RegisterHandler associates taskType with handler. It panics if called after
// Start.
func (w *Worker) RegisterHandler(taskType string, handler HandlerFunc) {
	if w.started.Load() {
		panic("dhara: RegisterHandler called after Start")
	}
	w.registry.Add(taskType, handler)
}

// Start runs the worker pool and blocks until ctx is cancelled or Stop is
// called. In-flight handlers are cancelled, their outcomes persisted, and the
// call returns after the pool drains or WithShutdownTimeout elapses. It
// returns an error if the worker was already started or the drain timed out.
func (w *Worker) Start(ctx context.Context) error {
	if w.started.Swap(true) {
		return errors.New("dhara: worker already started")
	}

	wp := worker.NewWorkerPool(w.store, w.registry, w.config.logger, worker.Settings{
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

// Stop requests a graceful shutdown. It is safe to call more than once.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

// Metrics returns a point-in-time read of this worker's process-local counters.
func (w *Worker) Metrics() MetricsSnapshot {
	return snapshotMetrics(w.metrics)
}
