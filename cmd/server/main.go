package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/logging"
)

func main() {
	os.Exit(run())
}

func run() int {
	start := time.Now()

	dbCfg := config.NewDatabaseConfig()
	migCfg := config.NewMigrationsConfig()
	serverCfg := config.NewServerConfig()
	shutdownCfg := config.NewShutdownConfig()
	logCfg := config.NewLoggingConfig()

	if err := config.Load(dbCfg, migCfg, serverCfg, shutdownCfg, logCfg); err != nil {
		return fail(err)
	}

	logger := logging.New(logCfg.Format, logCfg.Level)
	application, err := newApplication(appDeps{
		startTime:   start,
		logger:      logger,
		autoMigrate: migCfg.AutoMigrate,
		databaseURL: dbCfg.URL,
	})
	if err != nil {
		logger.Error("failed to build application", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:              ":" + strconv.Itoa(serverCfg.Port),
		Handler:           application.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("http server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()

	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownCfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
		return 1
	}

	if err := application.shutdown(); err != nil {
		logger.Error("application shutdown failed", "err", err)
		return 1
	}

	logger.Info("server stopped")
	return 0
}

func fail(err error) int {
	os.Stderr.WriteString(err.Error() + "\n")
	return 1
}
