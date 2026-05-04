package main

import (
	"context"
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

	application, err := app.NewApplication(start, cfg)
	if err != nil {
		return fail(err)
	}
	defer application.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application.WorkerPool.Start(ctx)

	server := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           application.SetupRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		application.Logger.Info("http server starting", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			application.Logger.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()

	application.Logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		application.Logger.Error("http shutdown failed", "err", err)
		return 1
	}

	application.Logger.Info("server stopped")
	return 0
}

func fail(err error) int {
	os.Stderr.WriteString(err.Error() + "\n")
	return 1
}
