package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/db"
)

// application is the composition root for the HTTP server: it owns the
// database pool, wires the handlers to the library client, and builds the
// route table. Everything here is package-private — cmd/server is a binary,
// not a library.
type application struct {
	db             *pgxpool.Pool
	startTime      time.Time
	healthHandler  *healthHandler
	taskHandler    *taskHandler
	metricsHandler *metricsHandler
}

type appDeps struct {
	startTime   time.Time
	logger      *slog.Logger
	autoMigrate bool
	databaseURL string
	maxConns    int
	minConns    int
}

func newApplication(deps appDeps) (*application, error) {
	if deps.logger == nil {
		deps.logger = slog.Default()
	}

	pool, err := db.Open(context.Background(), deps.databaseURL, int32(deps.maxConns), int32(deps.minConns))
	if err != nil {
		return nil, fmt.Errorf("failed to create db pool: %w", err)
	}

	if deps.autoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result, err := dhara.Migrate(mctx, pool)
		if err != nil {
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
		deps.logger.Info("migrations applied", "applied", result.Versions, "skipped", result.Skipped)
	}

	client := dhara.NewClient(pool)

	return &application{
		db:             pool,
		startTime:      deps.startTime,
		healthHandler:  newHealthHandler(deps.startTime, pool),
		taskHandler:    newTaskHandler(client, deps.logger),
		metricsHandler: newMetricsHandler(client),
	}, nil
}

func (a *application) shutdown() error {
	if a.db != nil {
		a.db.Close()
		a.db = nil
	}
	return nil
}

func (a *application) routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/livez", a.healthHandler.checkLiveness)
	mux.HandleFunc("GET /api/v1/readyz", a.healthHandler.checkReadiness)
	mux.HandleFunc("GET /api/v1/health", a.healthHandler.checkReadiness)

	mux.HandleFunc("GET /api/v1/tasks", a.taskHandler.handleListTasks)
	mux.HandleFunc("POST /api/v1/tasks", a.taskHandler.handleCreateTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", a.taskHandler.handleGetTaskById)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", a.taskHandler.handleDeleteTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", a.taskHandler.handleRetryDeadTask)

	mux.Handle("GET /api/v1/metrics", a.metricsHandler)
	mux.Handle("GET /metrics", a.metricsHandler)

	return mux
}
