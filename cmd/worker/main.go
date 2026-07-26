package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/db/migrations"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/queue"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.NewFromEnv()
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		return 1
	}

	logger := newLogger(cfg.LogFormat, cfg.LogLevel)
	registry := tasks.NewDemoRegistry()

	pool, err := db.Open(context.Background())
	if err != nil {
		logger.Error("failed to open database", "err", err)
		return 1
	}
	defer pool.Close()

	if cfg.AutoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := migrations.RunMigrations(mctx, pool, logger, cfg.MigrationsDir); err != nil {
			logger.Error("run migrations", "err", err)
			return 1
		}
	}

	m := metrics.New()
	taskStore := store.NewTaskStore(pool, m)
	wp := newWorkerPool(taskStore, registry, logger, cfg, m)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	wp.Start(ctx)

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	wp.Stop()
	if err := wp.Wait(shutdownCtx); err != nil {
		logger.Error("worker pool shutdown failed", "err", err)
		return 1
	}

	logger.Info("worker stopped")
	return 0
}

func newLogger(format string, level string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLogLevel(level)}

	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	default:
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
}

func parseLogLevel(level string) slog.Leveler {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
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
