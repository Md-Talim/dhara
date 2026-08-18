<p align="center">
  <img src="assets/dhara-logo.png" alt="Dhara logo" style="max-width: 100%; width: 600px;" />
</p>

# dhara

**Dhara** is a lightweight, production-focused distributed task queue library and service for Go, backed by PostgreSQL. Designed for simplicity, reliability, and high visibility, Dhara handles the complete task queue lifecycle without requiring heavy external dependencies like Redis or RabbitMQ.

Use it two ways:

- **As an embeddable library:** call `client.Enqueue(...)` from your application (even inside your own DB transaction) and run a `dhara.Worker` to execute tasks.
- **As pre-built services:** `cmd/server` (HTTP API) and `cmd/worker` (task processor) are thin binaries assembled entirely from the library, so they can never drift from it.

[Quick Start](#quick-start) • [Migrations](#migrations) • [Architecture](#architecture-and-task-lifecycle) • [Configuration](#configuration) • [API Reference](#api--observability-reference)

## Engineering Highlights & Architectural Decisions

This project is built from scratch to demonstrate production-grade Go patterns, reliable system design, and strict transactional guarantees.

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

4. **Transactional Enqueue (`InsertTx`)**

    Tasks can be inserted inside the caller's transaction, so a task is committed atomically with the business writes that produced it; the task never exists without its trigger, and never survives a rollback. Idempotency is enforced with `INSERT ... ON CONFLICT DO NOTHING` so a conflicting replay never aborts the caller's transaction.

5. **Zero-Dependency & Clean Architecture: Standard Library Routing**

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
    [*] --> PENDING : Task Created (API / Client)
    PENDING --> RUNNING : Claimed by Worker (SKIP LOCKED)
    RUNNING --> COMPLETED : Execution Success
    RUNNING --> PENDING : Execution Failure / Stale Heartbeat (Retry with Jitter)
    RUNNING --> DEAD : Max Retries Exhausted
    PENDING --> CANCELED : Canceled via API
    RUNNING --> CANCELED : Canceled via API
```

## Quick Start

Prerequisites: Go 1.26+, Docker & Docker compose.

### Option A: Use it as a library in your own app

```bash
go get github.com/md-talim/dhara
```

**1. Create a client and enqueue tasks.** A `Client` is lightweight and safe to construct anywhere (web handlers, background jobs, migrations). It starts zero goroutines.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://dhara:dhara@localhost:5432/dhara?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close() // you own the pool; the library never closes it

	// Apply migrations (embedded in the library, no MIGRATIONS_DIR needed)
	if _, err := dhara.Migrate(ctx, pool); err != nil {
		log.Fatal(err)
	}

	client := dhara.NewClient(pool)

	// Enqueue standalone, auto-wraps in its own transaction
	res, err := client.Enqueue(ctx, "send_email", EmailPayload{
		To:      "user@example.com",
		Subject: "Welcome!",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created task %s (duplicate=%v)\n", res.Task.ID, res.Duplicate)
}
```

**2. Enqueue atomically inside your own transaction.** The task is committed only if the surrounding transaction commits. Roll back the `tx` and the task never exists.

```go
tx, err := pool.Begin(ctx)
if err != nil {
	log.Fatal(err)
}

// Your business logic...
if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, orderID, 42.00); err != nil {
	_ = tx.Rollback(ctx)
	log.Fatal(err)
}

// Task + order commit atomically
res, err := client.EnqueueTx(ctx, tx, "send_email", EmailPayload{
	To:      "user@example.com",
	Subject: "Order #" + orderID,
})
if err != nil {
	_ = tx.Rollback(ctx)
	log.Fatal(err)
}

if err := tx.Commit(ctx); err != nil {
	log.Fatal(err)
}
```

**3. Run a worker to execute tasks.** A `Worker` owns the goroutine pool, heartbeats, and the reaper. `Start` blocks until the context is cancelled, then drains in-flight work.

```go
worker := dhara.NewWorker(pool,
	dhara.WithMaxWorkers(10),
	dhara.WithPollInterval(1*time.Second),
	dhara.WithHandlerTimeout(5*time.Minute),
)

worker.RegisterHandler("send_email", func(ctx context.Context, payload json.RawMessage) error {
	var p EmailPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	log.Printf("sending email to %s: %s", p.To, p.Subject)
	// do the work...
	return nil
})

ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

if err := worker.Start(ctx); err != nil {
	log.Fatal(err)
}
```

**Enqueue options.** `Enqueue` and `EnqueueTx` accept variadic options:

```go
client.Enqueue(ctx, "send_email", payload,
	dhara.WithIdempotencyKey("order-1234"), // replay-safe: same key + payload returns the existing task
	dhara.WithPriority(10),
	dhara.WithMaxRetries(3),
	dhara.WithRunAt(time.Now().Add(1*time.Hour)), // delay execution
)
```

Every enqueue returns an `*EnqueueResult`:

```go
type EnqueueResult struct {
	Task      *Task // the created task, or the existing task on an idempotent replay
	Duplicate bool  // true if an existing (type, idempotency_key) task was returned instead
}
```

### Option B: Run the whole stack in 5 minutes

Dhara ships as two independent binaries: the **Server** (HTTP API) and the **Worker** (task processor). They share the same database and can be scaled independently.

**Easiest (one command):** starts Postgres in Docker, waits for it, applies migrations, then runs the server and worker together. Ctrl-C stops both:

```bash
make dev
```

**Or run the whole stack in containers with zero manual steps:**

```bash
docker compose up --build
```

This builds and starts Postgres, runs migrations via a dedicated one-shot `migrate` service, then starts the server (`http://localhost:8080`) and the worker. `docker compose down` stops everything.

**Interact with the queue:**

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"echo","payload":{"message":"Hello, Dhara!"}}'

curl "http://localhost:8080/api/v1/tasks?limit=20"
```

The worker binary registers a few demo handlers: `echo`, `send_email`, `always_fail`, `slow_task`, so you can see retries, dead-lettering, and timeouts in action. To register your own task types, see [Adding custom task types](#adding-custom-task-types).

## Migrations

Migrations are **embedded in the library** (`go:embed`), so users never need a `MIGRATIONS_DIR` or a separate migration step. Each migration ships as an `up`/`down` pair, and rollback runs the matching `.down.sql`.

There are three ways to migrate, in increasing order of control:

**1. In your application (recommended for getting started).** Call `dhara.Migrate` once before enqueuing:

```go
res, err := dhara.Migrate(ctx, pool)
// res.Versions: the migration versions that were applied
// res.Skipped: how many were already applied and skipped
```

Roll back programmatically with `dhara.MigrateDown(ctx, pool, steps)`: each step runs the corresponding `.down.sql`:

```go
if _, err := dhara.MigrateDown(ctx, pool, 1); err != nil { // roll back 1 migration
	log.Fatal(err)
}
```

**2. From the CLI (river-style, recommended for production).** `cmd/migrate` is a tiny CLI over the same embedded migrations:

```bash
go run ./cmd/migrate           # apply all pending migrations (default: up)
go run ./cmd/migrate down      # roll back 1 migration (runs the .down.sql)
go run ./cmd/migrate down 3    # roll back 3 migrations
```

It reads `DHARA_DATABASE_URL` from the environment, just like the server and worker.

**3. With your own migration files.** If you maintain a fork of the schema, pass any `fs.FS` rooted at your migration directory (flat `NNNNNN_name.up.sql` / `.down.sql` files):

```go
res, err := dhara.MigrateWith(ctx, pool, os.DirFS("./migrations"))
res, err = dhara.MigrateDownWith(ctx, pool, os.DirFS("./migrations"), 2)
```

> **Note:** The library never auto-migrates by itself, DDL at app startup is a footgun in production. You choose when to run it: once in your app, via the CLI, or via `AUTO_MIGRATE` on the pre-built binaries.

## Configuration

The pre-built binaries are configured via environment variables. Library users configure the same knobs with the `dhara.With*` option functions instead.

| Variable             | Default        | Description                                                              |
| :------------------- | :------------- | :----------------------------------------------------------------------- |
| `DHARA_DATABASE_URL` | _Required_     | PostgreSQL connection string                                             |
| `DHARA_MAX_CONNS`    | `10`           | Maximum number of database connections in the pool                       |
| `DHARA_MIN_CONNS`    | `2`            | Minimum number of database connections in the pool                       |
| `AUTO_MIGRATE`       | `true`         | Automatically run embedded migrations on binary startup                  |
| `PORT`               | `8080`         | Port for the HTTP API server                                             |
| `WORKER_PREFIX`      | `dhara-worker` | Worker identity prefix                                                   |
| `WORKER_COUNT`       | `5`            | Size of the local concurrent worker pool                                 |
| `POLL_INTERVAL`      | `1s`           | How often workers poll for pending tasks                                 |
| `HEARTBEAT_INTERVAL` | `30s`          | How often running tasks update their heartbeat                           |
| `HANDLER_TIMEOUT`    | `5m`           | Maximum execution time limit for any single task                         |
| `STUCK_THRESHOLD`    | `5m`           | How long a task can run without a heartbeat before it's considered stuck |
| `REAPER_INTERVAL`    | `30s`          | How often the reaper scans for stuck tasks                               |
| `BASE_BACKOFF`       | `1s`           | Initial retry backoff (exponential with full jitter)                     |
| `MAX_BACKOFF`        | `5m`           | Maximum retry backoff                                                    |
| `SHUTDOWN_TIMEOUT`   | `30s`          | Maximum time allowed to drain active tasks on shutdown                   |
| `LOG_LEVEL`          | `info`         | Logging verbosity (`debug`, `info`, `warn`, `error`)                     |
| `LOG_FORMAT`         | `text`         | Structured log output style (`text` or `json`)                           |

Durations accept either Go duration strings (`30s`, `5m`) or plain integers interpreted as seconds.

## API & Observability Reference

### Task Management API

- `POST /api/v1/tasks`: Enqueue a new task.
- `GET /api/v1/tasks`: Query and list tasks with metadata.
- `GET /api/v1/tasks/{id}`: Fetch detailed status of a specific task.
- `DELETE /api/v1/tasks/{id}`: Cancel a `PENDING` or `RUNNING` task.
- `POST /api/v1/tasks/{id}/retry`: Manually trigger a retry for a `DEAD` task.

Every endpoint is a thin wrapper over the same `dhara.Client` methods the library exposes (`Insert`, `ListTasks`, `GetTask`, `CancelTask`, `RetryTask`), so the API and library share one code path by construction.

### Orchestration Health Probes

- `GET /api/v1/livez`: Liveness probe. Returns `200 OK` when the process is up.
- `GET /api/v1/readyz`: Readiness probe. Verifies database connectivity before routing traffic.

### Prometheus Metrics

- `GET /metrics` or `GET /api/v1/metrics`
  Exposes system-level metrics in standard Prometheus exposition format. Prominent metrics include:
- `tasks_enqueued_total`, `tasks_completed_total`, `tasks_dead_total` (lifecycle tracking)
- `tasks_by_status{status="..."}` (queue size & backlogs)
- `workers_total`, `workers_inflight` (worker resource utilization)

## Adding custom task types

Handlers have the signature `func(ctx context.Context, payload json.RawMessage) error`, return `nil` for success, any error to trigger a retry (until max retries exhaust and the task goes `DEAD`).

**Using the library:** register a handler on your `Worker`:

```go
worker.RegisterHandler("welcome_email", func(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Printf("sending welcome email to %s: %s", p.To, p.Subject)
	// perform task logic here...
	return nil
})
```

**Using the binaries:** add the same handler to the registration block in `cmd/worker/main.go`:

```go
worker.RegisterHandler("echo", tasks.Echo)
worker.RegisterHandler("send_email", tasks.SendEmail)
worker.RegisterHandler("welcome_email", myWelcomeEmailHandler) // yours
```

Then submit tasks with that `"type"`:

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"welcome_email","payload":{"to":"dev@example.com","subject":"Welcome to Dhara!"}}'
```

See [`internal/tasks/demo_handlers.go`](./internal/tasks/demo_handlers.go) for more examples.

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
