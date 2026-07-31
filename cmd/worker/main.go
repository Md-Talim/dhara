package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/db/migrations"
	"github.com/md-talim/dhara/internal/logging"
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

	logger := logging.New(cfg.LogFormat, cfg.LogLevel)
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
