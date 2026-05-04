package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/md-talim/dhara/internal/app"
	"github.com/md-talim/dhara/internal/config"
)

func main() {
	os.Exit(run())
}

func run() int {
	start := time.Now()

	cfg, err := config.NewFromEnv()
	if err != nil {
		return fail(err)
	}

	logger := newLogger(cfg.LogFormat)

	application, err := app.NewApplication(start, cfg, logger)
	if err != nil {
		logger.Error("failed to build application", "err", err)
		return 1
	}
	defer application.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application.Start(ctx)

	server := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           application.Routes(),
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
		return 1
	}

	logger.Info("server stopped")
	return 0
}

func newLogger(format string) *slog.Logger {
	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	default:
		return slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
}

func fail(err error) int {
	os.Stderr.WriteString(err.Error() + "\n")
	return 1
}
