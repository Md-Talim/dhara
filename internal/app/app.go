package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/api"
	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/queue"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

type Application struct {
	db             *pgxpool.Pool
	startTime      time.Time
	healthHandler  *api.HealthHandler
	taskHandler    *api.TaskHandler
	metricsHandler *api.MetricsHandler
	workerPool     *queue.WorkerPool
}

func NewApplication(start time.Time, cfg *config.Config, logger *slog.Logger) (*Application, error) {
	if logger == nil {
		logger = slog.Default()
	}

	pool, err := openDB()
	if err != nil {
		return nil, err
	}

	m := metrics.New()
	taskStore := store.NewTaskStore(pool, m)

	healthHandler := api.NewHealthHandler(start, pool)
	taskHandler := api.NewTaskHandler(taskStore, logger)
	metricsHandler := api.NewMetricsHandler(taskStore, m)

	registry := tasks.NewDemoRegistry()

	wp := newWorkerPool(taskStore, registry, logger, cfg, m)
	healthHandler.IsWorkerReady = wp.Started

	return &Application{
		db:             pool,
		startTime:      start,
		healthHandler:  healthHandler,
		taskHandler:    taskHandler,
		metricsHandler: metricsHandler,
		workerPool:     wp,
	}, nil
}

func (a *Application) Start(ctx context.Context) {
	if a.workerPool != nil {
		a.workerPool.Start(ctx)
	}
}

func (a *Application) Shutdown(ctx context.Context) error {
	if a.workerPool != nil {
		a.workerPool.Stop()
		if err := a.workerPool.Wait(ctx); err != nil {
			return fmt.Errorf("wait for worker pool shutdown: %w", err)
		}
	}

	if a.db != nil {
		a.db.Close()
		a.db = nil
	}

	return nil
}

func (app *Application) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/livez", app.healthHandler.CheckLiveness)
	mux.HandleFunc("GET /api/v1/readyz", app.healthHandler.CheckReadiness)
	mux.HandleFunc("GET /api/v1/health", app.healthHandler.CheckReadiness)

	mux.HandleFunc("GET /api/v1/tasks", app.taskHandler.HandleListTasks)
	mux.HandleFunc("POST /api/v1/tasks", app.taskHandler.HandleCreateTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", app.taskHandler.HandleGetTaskById)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", app.taskHandler.HandleDeleteTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", app.taskHandler.HandleRetryDeadTask)

	mux.Handle("GET /api/v1/metrics", app.metricsHandler)

	return mux
}

func openDB() (*pgxpool.Pool, error) {
	ctx := context.Background()

	pool, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create db pool: %w", err)
	}

	return pool, nil
}

func newWorkerPool(
	taskStore store.TaskStore,
	registry tasks.HandlerRegistry,
	logger *slog.Logger,
	cfg *config.Config,
	m *metrics.Metrics,
) *queue.WorkerPool {
	workerSettings := queue.Settings{
		WorkerPrefix:      cfg.WorkerPrefix,
		Concurrency:       cfg.WorkerCount,
		PollInterval:      cfg.PollInterval,
		HeartbeatInterval: cfg.HeartbeatInterval,
		HandlerTimeout:    cfg.HandlerTimeout,
		BaseBackoff:       cfg.BaseBackoff,
		MaxBackoff:        cfg.MaxBackoff,
		ReaperInterval:    cfg.ReaperInterval,
		StaleThreshold:    cfg.StuckThreshold,
	}

	return queue.NewWorkerPool(taskStore, registry, logger, workerSettings, m)
}
