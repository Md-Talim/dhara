package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/md-talim/dhara/internal/logging"
	"github.com/md-talim/dhara/internal/playground"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if len(os.Args) > 1 && os.Args[1] == "-crash-worker" {
		logger := logging.New(os.Getenv("LOG_FORMAT"), os.Getenv("LOG_LEVEL"))
		if err := playground.RunCrashWorker(ctx, logger); err != nil {
			logger.Error("isolated crash worker failed", "err", err)
			return 1
		}
		return 0
	}

	cfg, err := playground.LoadConfig()
	if err != nil {
		os.Stderr.WriteString("playground config: " + err.Error() + "\n")
		return 1
	}
	logger := logging.New(cfg.LogFormat, cfg.LogLevel)

	app, err := playground.NewApp(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to build playground", "err", err)
		return 1
	}
	if err := app.Run(ctx); err != nil {
		logger.Error("playground exited with error", "err", err)
		return 1
	}
	return 0
}
