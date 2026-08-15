package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/api"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/queue"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/vow"
)

type Application struct {
	db             *pgxpool.Pool
	startTime      time.Time
	healthHandler  *api.HealthHandler
	taskHandler    *api.TaskHandler
	metricsHandler *api.MetricsHandler
	workerPool     *queue.WorkerPool
}

type AppDependencies struct {
	StartTime     time.Time
	Logger        *slog.Logger
	AutoMigrate   bool
	DatabaseURL   string
	MigrationsDir string
}

func NewApplication(deps AppDependencies) (*Application, error) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	pool, err := openDB(deps.DatabaseURL)
	if err != nil {
		return nil, err
	}

	if deps.AutoMigrate {
		mctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		migrator, err := vow.New(pool, os.DirFS(deps.MigrationsDir),
			vow.WithTableName("dhara_vow_migrations"),
			vow.WithLockName("dhara_vow_lock"),
			vow.WithLogger(deps.Logger),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create migrator: %w", err)
		}

		result, err := migrator.Up(mctx)
		if err != nil {
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
		deps.Logger.Info("migrations applied", "applied", result.Versions, slog.Duration("duration", result.Duration))
	}

	m := metrics.New()
	taskStore := store.NewTaskStore(pool, m)

	healthHandler := api.NewHealthHandler(deps.StartTime, pool)
	taskHandler := api.NewTaskHandler(taskStore, deps.Logger)
	metricsHandler := api.NewMetricsHandler(taskStore, m)

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

func openDB(databaseURL string) (*pgxpool.Pool, error) {
	ctx := context.Background()

	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create db pool: %w", err)
	}

	return pool, nil
}
