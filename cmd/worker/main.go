package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/logging"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/queue"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
	"github.com/md-talim/vow"
)

func main() {
	os.Exit(run())
}

func run() int {
	dbCfg := config.NewDatabaseConfig()
	migCfg := config.NewMigrationsConfig()
	workerCfg := config.NewWorkerConfig()
	shutdownCfg := config.NewShutdownConfig()
	logCfg := config.NewLoggingConfig()

	if err := config.Load(dbCfg, migCfg, workerCfg, shutdownCfg, logCfg); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		return 1
	}

	logger := logging.New(logCfg.Format, logCfg.Level)
	registry := tasks.NewDemoRegistry()

	pool, err := db.Open(context.Background(), dbCfg.URL)
	if err != nil {
		logger.Error("failed to open database", "err", err)
		return 1
	}
	defer pool.Close()

	if migCfg.AutoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		migrator, err := vow.New(pool, os.DirFS(migCfg.MigrationsDir),
			vow.WithTableName("dhara_vow_migrations"),
			vow.WithLockName("dhara_vow_lock"),
			vow.WithLogger(logger),
		)
		if err != nil {
			logger.Error("failed to create migrator", "err", err)
			return 1
		}

		result, err := migrator.Up(mctx)
		if err != nil {
			logger.Error("failed to run migrations", "err", err)
			return 1
		}
		logger.Info("migrations applied", "applied", result.Versions, "skipped", result.Skipped, "duration", result.Duration)
	}

	m := metrics.New()
	taskStore := store.NewTaskStore(pool, m)

	workerSettings := queue.Settings{
		WorkerPrefix:      workerCfg.WorkerPrefix,
		Concurrency:       workerCfg.WorkerCount,
		PollInterval:      workerCfg.PollInterval,
		HeartbeatInterval: workerCfg.HeartbeatInterval,
		HandlerTimeout:    workerCfg.HandlerTimeout,
		BaseBackoff:       workerCfg.BaseBackoff,
		MaxBackoff:        workerCfg.MaxBackoff,
		ReaperInterval:    workerCfg.ReaperInterval,
		StaleThreshold:    workerCfg.StuckThreshold,
	}
	wp := queue.NewWorkerPool(taskStore, registry, logger, workerSettings, m)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	wp.Start(ctx)

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownCfg.ShutdownTimeout)
	defer cancel()

	wp.Stop()
	if err := wp.Wait(shutdownCtx); err != nil {
		logger.Error("worker pool shutdown failed", "err", err)
		return 1
	}

	logger.Info("worker stopped")
	return 0
}
