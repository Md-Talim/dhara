<p align="center">
  <img src="assets/dhara-logo.png" alt="Dhara logo" style="max-width: 100%; width: 600px;" />
</p>

# dhara

Dhara is a Go-based distributed task queue built on PostgreSQL.

It is an active work in progress, but the current codebase already covers the core queue lifecycle:

- creating tasks via HTTP
- listing and fetching tasks
- canceling tasks
- retrying dead tasks
- claiming tasks from PostgreSQL
- executing tasks in worker pools
- heartbeating running tasks
- reaping stale running tasks
- recording task status changes and logs
- exposing operational health and metrics endpoints
- running schema migrations

## Quick Start (demo)

Prereqs: Go 1.26+, Docker, and running PostgreSQL instance.

1. Start Postgres via Docker:

    ```bash
    docker compose up db
    ```

2. Set the required environment variable:

    ```bash
    export DHARA_DATABASE_URL="postgres://dhara:dhara@localhost:5432/dhara?sslmode=disable"
    ```

    If you prefer a file, copy `.env.example` to `.env` and load it in your shell.

3. Run the server (auto-migrates by default):

    ```bash
    go run ./cmd/server
    ```

4. Create a task:

    ```bash
    curl -X POST http://localhost:8080/api/v1/tasks \
    -H "Content-Type: application/json" \
    -d '{"type":"echo","payload":{"message":"hello"}}'
    ```

5. List tasks:

    ```bash
    curl "http://localhost:8080/api/v1/tasks?limit=20"
    ```

6. To register custom task types, see [Adding custom task types](#adding-custom-task-types).

## Configuration

Required:

- `DHARA_DATABASE_URL` — PostgreSQL connection string.

Optional (defaults shown in `.env.example`):

- `AUTO_MIGRATE` (default `true`)
- `MIGRATIONS_DIR` (default `internal/db/migrations`)
- `PORT` (default `8080`)
- `WORKER_COUNT` (default `5`)
- `HANDLER_TIMEOUT` (default `5m`)
- `SHUTDOWN_TIMEOUT` (default `30s`)
- `LOG_LEVEL` (default `info`)
- `LOG_FORMAT` (default `text`)

## Current features

- PostgreSQL-backed task storage
- Task creation API
- Task listing API
- Task retrieval API
- Task cancellation API
- Manual retry of dead tasks
- Worker pool that polls for pending tasks
- Heartbeats for running tasks
- Reaper for stale running tasks
- Task handlers for demo workloads
- Automatic migrations on startup (configurable)
- `/api/v1/livez`, `/api/v1/readyz`, `api/v1/health` health endpoints
- Prometheus-style `/api/v1/metrics` endpoint
- Structured logging with `slog`

## Project overview

Dhara uses PostgreSQL as the source of truth for task state.

<p align="center">
  <img src="assets/architecture.png" alt="Dhara architecture diagram" style="max-width: 100%; width: 800px;" />
</p>

### Main components

- **HTTP server**
  Exposes the task API, health endpoints, and metrics endpoint.

- **Task store**
  Handles database operations for creating, claiming, updating, retrying, canceling, and reading tasks.

- **Worker pool**
  Continuously polls for pending tasks and executes handlers concurrently.

- **Reaper**
  Detects stale running tasks and either requeues them or marks them dead.

- **Task handlers**
  Business logic for task types such as `echo`, `send_email`, `always_fail`, and `slow_task`.

- **Migrations**
  Sets up and evolves the database schema.

- **Metrics**
  Exposes queue health and lifecycle counters for operational visibility.

## Current execution flow

1. A client submits a task through the HTTP API.
2. The task is stored in PostgreSQL with `PENDING` status.
3. Worker goroutines poll for available tasks.
4. A worker atomically claims a pending task.
5. The matching handler executes the task payload.
6. The task is marked `COMPLETED`, retried with backoff, or marked `DEAD`.
7. Heartbeats keep running tasks alive while they are executing.
8. The reaper requeues stale running tasks or moves them to dead-letter state.
9. Task status changes are recorded in `task_logs`.

## Health endpoints

The service exposes health endpoints to help with orchestration and monitoring.

- `GET /api/v1/livez`
  Liveness probe. Returns `200` when the process is running.

- `GET /api/v1/readyz`
  Readiness probe. Typically checks database connectivity and startup gates.

- `GET /api/v1/health`
  Alias of readiness in the current API layout.

## Metrics endpoint

- `GET /metrics`

Returns metrics in Prometheus text exposition format.

The endpoint includes:

- task lifecycle counters
- current queue state from PostgreSQL
- worker counts
- inflight execution counts

Example metric families:

- `tasks_enqueued_total`
- `tasks_completed_total`
- `tasks_attempt_failures_total`
- `tasks_retried_total`
- `tasks_dead_total`
- `tasks_canceled_total`
- `tasks_reaped_total`
- `tasks_by_status{status="PENDING"}`
- `tasks_pending_breakdown{status="ready"}`
- `workers_total`
- `workers_inflight`

## HTTP API

The API is still evolving, but currently includes:

- `POST /api/v1/tasks`
- `GET /api/v1/tasks/{id}`
- `GET /api/v1/tasks`
- `DELETE /api/v1/tasks/{id}`
- `POST /api/v1/tasks/{id}/retry`

More endpoints may be added later, including:

- richer filtering and pagination options
- task introspection/debug endpoints
- per-task execution history
- operational admin endpoints

## Task lifecycle

Dhara currently supports the following task states:

- `PENDING`
- `RUNNING`
- `COMPLETED`
- `CANCELED`
- `DEAD`

### Lifecycle behavior

- `PENDING` tasks are eligible for workers to claim.
- `RUNNING` tasks are being processed by a worker.
- `COMPLETED` tasks finished successfully.
- `CANCELED` tasks were canceled before execution.
- `DEAD` tasks exhausted retries or were explicitly marked unrecoverable.

## Retry and recovery behavior

Dhara includes basic failure recovery mechanics:

- failed tasks can be retried with exponential backoff
- running tasks send periodic heartbeats
- stale running tasks can be reaped and requeued
- tasks that exceed retry limits can be marked dead

This is intentionally simple today, but it mirrors the core mechanics used by production queue systems.

## Migrations

By default, the server runs migrations on startup when `AUTO_MIGRATE=true`.

To run them manually (or if `AUTO_MIGRATE=false`):

```bash
go run ./cmd/migrate
```

The `MIGRATIONS_DIR` environment variable controls the directory (default `internal/db/migrations`).

## Running the server

```bash
go run ./cmd/server
```

Make sure `DHARA_DATABASE_URL` is set.

The server will:

- open the database pool
- run migrations when `AUTO_MIGRATE=true`
- initialize task storage
- start the worker pool
- serve HTTP on `:8080`

## Demo task types

The current demo handlers include:

- `echo`
- `send_email`
- `always_fail`
- `slow_task`

These are intentionally simple and are meant to exercise the worker and recovery flow.

## Adding custom task types

Dhara uses a registry that maps task type strings to handler function. To add your own:

1. Implement a handler with signature `func(ctx context.Context, payload json.RawMessage) error`.

    Example handler (e.g., `internal/tasks/custom_handlers.go`):

    ```go
    import (
        "context"
        "encoding/json"
        "fmt"

        "github.com/md-talim/dhara/internal/ctxlog"
    )

    func WelcomeEmail(ctx context.Context, payload json.RawMessage) error {
        var p struct {
            To      string `json:"to"`
            Subject string `json:"subject"`
        }
        if err := json.Unmarshal(payload, &p); err != nil {
            return fmt.Errorf("invalid payload: %w", err)
        }

        ctxlog.From(ctx).Info("sending welcome email", "to", p.To, "subject", p.Subject)
        return nil
    }
    ```

2) Register your handler in `cmd/server/main.go`:

    ```go
    registry := tasks.NewRegistry(map[string]tasks.HandlerFunc{
         "echo":          tasks.Echo,
         "send_email":    tasks.SendEmail,
         "welcome_email": tasks.WelcomeEmail,
     })

     application, err := app.NewApplication(start, cfg, logger, registry)
    ```

3) Submit tasks with `"type": "welcome_email"` in the API request.

See [`internal/tasks/demo_handlers.go`](./internal/tasks/demo_handlers.go)`` for more examples.

## Design goals

Dhara is being built with the following goals in mind:

- correctness first
- PostgreSQL as the durable queue backend
- clear task state transitions
- observable worker behavior
- minimal dependencies
- no HTTP framework, just Go’s standard library
- production-style operational visibility without external hosted services

## Notes

This repository is not finished yet.

Package names, structure, and APIs may be refactored as development continues.

## Planned work

- ~~stronger retry semantics with jitter~~
- improved dead-letter handling
- task execution histograms
- better queue latency metrics
- more complete health/readiness gates
- richer operational dashboards
- more robust cancellation semantics
- stronger validation and test coverage
- clearer startup and wiring structure
- refactoring around application bootstrap and lifecycle management

## License

TBD
