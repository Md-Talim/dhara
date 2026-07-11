<p align="center">
  <img src="assets/dhara-logo.png" alt="Dhara logo" style="max-width: 100%; width: 600px;" />
</p>

# dhara

**Dhara** is a lightweight, production-focused distributed task queue implementation in Go and backed by PostgreSQL. Designed for simplicity, reliability, and high visibility, Dhara handles the complete task queue lifecycle without requiring heavy external dependencies like Redis or RabbitMQ.

[Quick Start](#quick-start) • [Architecture](#architecture-and-task-lifecycle) • [Configuration](#configuration) • [API Reference](#api--observability-reference)

## Engineering Highlights & Architectural Decisions

This project is build from scratch to demonstrate production-grade Go patterns, reliable system design, and strict transactional guarantees.

1. **Atomic Concurrency with PostgreSQL (`SKIP LOCKED`)**

    Instead of using external distributed lock managers, Dhara utilizes PostgreSQL as a highly concurrent queue broker. Task claiming is implemented using transactional **`SELECT ... FOR UPDATE SKIP LOCKED`** operations. This guarantees that:

    - Multiple horizontal workers can poll the database concurrently without race conditions.
    - Each pending task is claimed atomically by exactly one worker.
    - Lock contention is completely avoided, maintaining high throughput.

2. **Failure Recovery: Heartbeats & The Reaper Pattern**

    Distributed workers can crash, lose network connectivity, or experience hardware failures. Dhara ensures task execution safely through a dual-mechanism recovery pattern:

    - **Heartbeating:** Running worker goroutines periodically update task heartbeats in the database.
    - **The Reaper:** A background process detects stale tasks that have missed their heartbeat window. Depending on the configuration, the reaper atomically requeues the task (incrementing its attempt counter) or moves it to a dead-letter state once max retries are exhausted.

3. **Resilient Retries with Full Jitter**

    To prevent the "thundering herd" problem when retrying failed tasks, Dhara implements an exponential backoff retry algorithm augmented with **Full Jitter**. This spreads out retry attempts randomly across a safe window, protecting downstream databases and services from traffic spikes.

4. **Zero-Dependency & Clean Architecture:** **Standard Library Routing**

    Implements standard HTTP routing using Go 1.22's enhanced standard library `http.ServeMux`, keeping the binary footprint small and eliminating external framework bloat.

    - **Structured Logging:** Uses the standard library `slog` for structured logging (available in both plain text and JSON formats for modern log aggregators).
    - **Graceful Shutdown:** Handles `SIGINT` / `SIGTERM` signals natively. On shutdown, it stops new task ingestion, allows active workers a configurable timeout to finish processing in-flight tasks, and gracefully drains database connection pools to prevent database state corruption.

## Architecture and Task Lifecycle

Dhara uses PostgreSQL as the single source of truth for task state and execution logs.

<p align="center">
  <img src="assets/architecture.png" alt="Dhara architecture diagram" style="max-width: 100%; width: 800px;" />
</p>

### The Task Lifecycle Flow

```mermaid
stateDiagram-v2
    [*] --> PENDING : Task Created via API
    PENDING --> RUNNING : Claimed by Worker (SKIP LOCKED)
    RUNNING --> COMPLETED : Execution Success
    RUNNING --> PENDING : Execution Failure / Stale Heartbeat (Retry with Jitter)
    RUNNING --> DEAD : Max Retries Exhausted
    PENDING --> CANCELED : Canceled via API
    RUNNING --> CANCELED : Canceled via API
```

## Quick Start

Prerequisites: Go 1.26+, Docker & Docker compose

1. **Spin up the Database**

    Start a local PostgreSQL instance:

    ```bash
    docker compose up db -d
    ```

2. **Set Up Environment & Run**

    Copy the example environment configuration and run the server (which automatically runs database schema migrations on startup by default):

    ```bash
    export DHARA_DATABASE_URL="postgres://dhara:dhara@localhost:5432/dhara?sslmode=disable"
    go run ./cmd/server
    ```

3. **Interact with the Queue**

    **Create a task:**

    ```bash
    curl -X POST http://localhost:8080/api/v1/tasks \
    -H "Content-Type: application/json" \
    -d '{"type":"echo","payload":{"message":"Hello, Dhara!"}}'
    ```

    **List active tasks:**

    ```bash
    curl "http://localhost:8080/api/v1/tasks?limit=20"
    ```

4. To register custom task types, see [Adding custom task types](#adding-custom-task-types).

## Configuration

Dhara is configured entirely via environment variables.

| Variable             | Default Value            | Description                                            |
| :------------------- | :----------------------- | :----------------------------------------------------- |
| `DHARA_DATABASE_URL` | _Required_               | PostgreSQL connection string                           |
| `AUTO_MIGRATE`       | `true`                   | Automatically run migrations on server startup         |
| `MIGRATIONS_DIR`     | `internal/db/migrations` | Path to the SQL migrations directory                   |
| `PORT`               | `8080`                   | Port for the HTTP API server                           |
| `WORKER_COUNT`       | `5`                      | Size of the local concurrent worker pool               |
| `HANDLER_TIMEOUT`    | `5m`                     | Maximum execution time limit for any single task       |
| `SHUTDOWN_TIMEOUT`   | `30s`                    | Maximum time allowed to drain active tasks on shutdown |
| `LOG_LEVEL`          | `info`                   | Logging verbosity (`debug`, `info`, `warn`, `error`)   |
| `LOG_FORMAT`         | `text`                   | Structured log output style (`text` or `json`)         |

## API & Observability Reference

### Task Management API

- `POST /api/v1/tasks` — Enqueue a new task.
- `GET /api/v1/tasks` — Query and list tasks with metadata.
- `GET /api/v1/tasks/{id}` — Fetch detailed status of a specific task.
- `DELETE /api/v1/tasks/{id}` — Cancel a `PENDING` or `RUNNING` task.
- `POST /api/v1/tasks/{id}/retry` — Manually trigger a retry for a `DEAD` task.

### Orchestration Health Probes

- `GET /api/v1/livez` — Liveness probe. Returns `200 OK` when the process is up.
- `GET /api/v1/readyz` — Readiness probe. Verifies database connectivity and worker pool status before routing traffic.

### Prometheus Metrics

- `GET /metrics` or `GET /api/v1/metrics`
  Exposes system-level metrics in standard Prometheus exposition format. Prominent metrics include:
- `tasks_enqueued_total`, `tasks_completed_total`, `tasks_dead_total` (lifecycle tracking)
- `tasks_by_status{status="..."}` (queue size & backlogs)
- `workers_total`, `workers_inflight` (worker resource utilization)

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
        // perform task logic here...
        return nil
    }
    ```

2)  Register your custom handler in `cmd/server/main.go` with the registry:

    ```go
    registry := tasks.NewRegistry(map[string]tasks.HandlerFunc{
         "echo":          tasks.Echo,
         "send_email":    tasks.SendEmail,
         "welcome_email": tasks.WelcomeEmail,
     })

     application, err := app.NewApplication(start, cfg, logger, registry)
    ```

3)  Submit tasks with `"type": "welcome_email"` in the API request.

    ```bash
    curl -X POST http://localhost:8080/api/v1/tasks \
     -H "Content-Type: application/json" \
     -d '{"type":"welcome_email","payload":{"to":"dev@example.com","subject":"Welcome to Dhara!"}}'
    ```

See [`internal/tasks/demo_handlers.go`](./internal/tasks/demo_handlers.go)`` for more examples.

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
