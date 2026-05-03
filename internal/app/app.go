package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
	DB             *pgxpool.Pool
	Start          time.Time
	Logger         *slog.Logger
	HealthHandler  *api.HealthHandler
	TaskHandler    *api.TaskHandler
	MetricsHandler *api.MetricsHandler
	WorkerPool     *queue.WorkerPool
}

func NewApplication(start time.Time, cfg *config.Config) (*Application, error) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	pool, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create db pool: %w", err)
	}

	m := metrics.New()

	taskStore := store.NewTaskStore(pool, m)

	healthHandler := api.NewHealthHandler(start, pool)
	taskHandler := api.NewTaskHandler(taskStore, logger)
	metricsHandler := api.NewMetricsHandler(taskStore, m)

	registry := tasks.NewDemoRegistry()

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

	wp := queue.NewWorkerPool(taskStore, registry, logger, workerSettings, m)
	healthHandler.IsWorkerReady = wp.Started

	app := &Application{
		DB:             pool,
		Start:          start,
		Logger:         logger,
		HealthHandler:  healthHandler,
		TaskHandler:    taskHandler,
		MetricsHandler: metricsHandler,
		WorkerPool:     wp,
	}

	return app, nil
}
