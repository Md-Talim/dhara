package playground

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/tasks"
)

// App is the composition root for the playground. It keeps the demo server
// and worker in one process so a single small container can expose the full
// Dhara lifecycle. The production server/worker binaries remain untouched.
type App struct {
	cfg    *Config
	logger *slog.Logger

	pool     *pgxpool.Pool
	client   *dhara.Client
	worker   *dhara.Worker
	store    *Store
	crash    *CrashManager
	http     *HTTPServer
	started  time.Time
	shutdown bool
}

func NewApp(ctx context.Context, cfg *Config, logger *slog.Logger) (*App, error) {
	pool, err := PrepareSchema(ctx, cfg, cfg.Schema)
	if err != nil {
		return nil, err
	}

	client := dhara.NewClient(pool)
	store := NewStore(pool)

	worker := dhara.NewWorker(pool,
		dhara.WithWorkerPrefix(cfg.WorkerPrefix),
		dhara.WithMaxWorkers(cfg.WorkerCount),
		dhara.WithPollInterval(cfg.PollInterval),
		dhara.WithHeartbeatInterval(cfg.HeartbeatInterval),
		dhara.WithHandlerTimeout(cfg.HandlerTimeout),
		dhara.WithBaseBackoff(cfg.BaseBackoff),
		dhara.WithMaxBackoff(cfg.MaxBackoff),
		dhara.WithReaperInterval(cfg.ReaperInterval),
		dhara.WithStuckThreshold(cfg.StuckThreshold),
		dhara.WithShutdownTimeout(cfg.ShutdownTimeout),
		dhara.WithLogger(logger),
	)
	worker.RegisterHandler(TypeEcho, tasks.Echo)
	worker.RegisterHandler(TypeSendEmail, tasks.SendEmail)
	worker.RegisterHandler(TypeAlwaysFail, tasks.AlwaysFails)
	worker.RegisterHandler(TypeSlowTask, tasks.SlowTask)
	worker.RegisterHandler(TypeRealisticWork, tasks.RealisticWork)
	worker.RegisterHandler(TypeFlaky, FlakyWork)

	crash, err := NewCrashManager(ctx, cfg, logger)
	if err != nil {
		// The main playground remains useful without the optional crash demo.
		// It is disabled in the UI rather than failing the entire process.
		logger.Warn("crash demo disabled because setup failed", "err", err)
		crash = nil
	}

	service := NewService(cfg, client, store, worker, crash)
	httpServer := NewHTTPServer(cfg, client, service, pool,
		client.MetricsHandler(), worker.MetricsHandler(), logger)

	return &App{
		cfg:     cfg,
		logger:  logger,
		pool:    pool,
		client:  client,
		worker:  worker,
		store:   store,
		crash:   crash,
		http:    httpServer,
		started: time.Now(),
	}, nil
}

// Run starts the real Dhara worker, retention loop, and HTTP server, then
// blocks until ctx is cancelled or the server fails.
func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerErr := make(chan error, 1)
	go func() {
		workerErr <- a.worker.Start(runCtx)
	}()

	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		a.cleanupLoop(runCtx)
	}()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.cfg.Port),
		Handler:           a.http.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	serverErr := make(chan error, 1)
	go func() {
		a.logger.Info("playground listening", "addr", server.Addr, "schema", a.cfg.Schema)
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	var runErr error
	workerStopped := false
	select {
	case <-ctx.Done():
		a.logger.Info("playground shutdown requested")
	case err := <-workerErr:
		workerStopped = true
		if err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("worker stopped unexpectedly", "err", err)
			runErr = err
		} else {
			a.logger.Warn("worker stopped before shutdown")
		}
	case err := <-serverErr:
		if err != nil {
			a.logger.Error("playground http server failed", "err", err)
			runErr = err
		}
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		a.logger.Error("playground http shutdown failed", "err", err)
	}
	<-cleanupDone

	if !workerStopped {
		select {
		case err := <-workerErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error("worker stopped with error", "err", err)
				if runErr == nil {
					runErr = err
				}
			}
		case <-shutdownCtx.Done():
			a.logger.Warn("worker did not stop before shutdown timeout")
		}
	}

	a.Close()
	a.logger.Info("playground stopped", "uptime", time.Since(a.started).Round(time.Second))
	return runErr
}

func (a *App) Close() {
	if a.shutdown {
		return
	}
	a.shutdown = true
	if a.crash != nil {
		a.crash.Close()
	}
	if a.pool != nil {
		a.pool.Close()
	}
}

func (a *App) cleanupLoop(ctx context.Context) {
	timer := time.NewTimer(a.cfg.CleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			a.cleanup(ctx)
			timer.Reset(a.cfg.CleanupInterval)
		}
	}
}

func (a *App) cleanup(ctx context.Context) {
	cutoff := time.Now().Add(-a.cfg.Retention)
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	n, err := a.store.DeleteOlderThan(cctx, cutoff)
	if err != nil {
		a.logger.Error("playground task cleanup failed", "err", err)
	} else if n > 0 {
		a.logger.Info("playground task cleanup removed rows", "count", n, "cutoff", cutoff)
	}

	if a.crash != nil {
		cn, err := a.crash.Cleanup(cctx, cutoff)
		if err != nil {
			a.logger.Error("crash-demo cleanup failed", "err", err)
		} else if cn > 0 {
			a.logger.Info("crash-demo cleanup removed rows", "count", cn, "cutoff", cutoff)
		}
	}
}
