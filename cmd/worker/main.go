package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/logging"
	"github.com/md-talim/dhara/internal/tasks"
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
	pool, err := db.Open(context.Background(), dbCfg.URL, int32(dbCfg.MaxConns), int32(dbCfg.MinConns))
	if err != nil {
		logger.Error("failed to open database", "err", err)
		return 1
	}
	defer pool.Close()

	if migCfg.AutoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		result, err := dhara.Migrate(mctx, pool)
		if err != nil {
			logger.Error("failed to run migrations", "err", err)
			return 1
		}
		logger.Info("migrations applied", "applied", result.Versions, "skipped", result.Skipped)
	}

	worker := dhara.NewWorker(pool,
		dhara.WithWorkerPrefix(workerCfg.WorkerPrefix),
		dhara.WithMaxWorkers(workerCfg.WorkerCount),
		dhara.WithPollInterval(workerCfg.PollInterval),
		dhara.WithHeartbeatInterval(workerCfg.HeartbeatInterval),
		dhara.WithHandlerTimeout(workerCfg.HandlerTimeout),
		dhara.WithBaseBackoff(workerCfg.BaseBackoff),
		dhara.WithMaxBackoff(workerCfg.MaxBackoff),
		dhara.WithReaperInterval(workerCfg.ReaperInterval),
		dhara.WithStuckThreshold(workerCfg.StuckThreshold),
		dhara.WithShutdownTimeout(shutdownCfg.ShutdownTimeout),
		dhara.WithLogger(logger),
	)

	worker.RegisterHandler("echo", tasks.Echo)
	worker.RegisterHandler("send_email", tasks.SendEmail)
	worker.RegisterHandler("always_fail", tasks.AlwaysFails)
	worker.RegisterHandler("slow_task", tasks.SlowTask)
	worker.RegisterHandler("realistic_work", tasks.RealisticWork)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("worker starting")
	if err := worker.Start(ctx); err != nil {
		logger.Error("worker stopped with error", "err", err)
		return 1
	}

	logger.Info("worker stopped")
	return 0
}
