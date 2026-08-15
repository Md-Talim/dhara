package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/api"
	"github.com/md-talim/dhara/internal/db"
)

type Application struct {
	db             *pgxpool.Pool
	startTime      time.Time
	healthHandler  *api.HealthHandler
	taskHandler    *api.TaskHandler
	metricsHandler *api.MetricsHandler
}

type AppDependencies struct {
	StartTime   time.Time
	Logger      *slog.Logger
	AutoMigrate bool
	DatabaseURL string
}

func NewApplication(deps AppDependencies) (*Application, error) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	pool, err := db.Open(context.Background(), deps.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create db pool: %w", err)
	}

	if deps.AutoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result, err := dhara.Migrate(mctx, pool)
		if err != nil {
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
		deps.Logger.Info("migrations applied", "applied", result.Versions, "skipped", result.Skipped)
	}

	client := dhara.NewClient(pool)

	healthHandler := api.NewHealthHandler(deps.StartTime, pool)
	taskHandler := api.NewTaskHandler(client, deps.Logger)
	metricsHandler := api.NewMetricsHandler(client)

	return &Application{
		db:             pool,
		startTime:      deps.StartTime,
		healthHandler:  healthHandler,
		taskHandler:    taskHandler,
		metricsHandler: metricsHandler,
	}, nil
}

func (a *Application) Shutdown(ctx context.Context) error {
	if a.db != nil {
		a.db.Close()
		a.db = nil
	}
	return nil
}

func (app *Application) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/livez", app.healthHandler.CheckLiveness)
	mux.HandleFunc("GET /api/v1/readyz", app.healthHandler.CheckReadiness)
	mux.HandleFunc("GET /api/v1/health", app.healthHandler.CheckReadiness)

	mux.HandleFunc("GET /api/v1/tasks", app.taskHandler.HandleListTasks)
	mux.HandleFunc("POST /api/v1/tasks", app.taskHandler.HandleCreateTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", app.taskHandler.HandleGetTaskById)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", app.taskHandler.HandleDeleteTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", app.taskHandler.HandleRetryDeadTask)

	mux.Handle("GET /api/v1/metrics", app.metricsHandler)
	mux.Handle("GET /metrics", app.metricsHandler)

	return mux
}
