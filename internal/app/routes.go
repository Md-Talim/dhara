package app

import (
	"net/http"
)

func (app *Application) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/livez", app.HealthHandler.CheckLiveness)
	mux.HandleFunc("GET /api/v1/readyz", app.HealthHandler.CheckReadiness)
	mux.HandleFunc("GET /api/v1/health", app.HealthHandler.CheckReadiness)

	mux.HandleFunc("GET /api/v1/tasks", app.TaskHandler.HandleListTasks)
	mux.HandleFunc("POST /api/v1/tasks", app.TaskHandler.HandleCreateTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", app.TaskHandler.HandleGetTaskById)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", app.TaskHandler.HandleDeleteTask)

	return mux
}
