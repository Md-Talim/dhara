# dhara

[![CI](https://github.com/md-talim/dhara/actions/workflows/ci.yml/badge.svg)](https://github.com/md-talim/dhara/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/md-talim/dhara.svg)](https://pkg.go.dev/github.com/md-talim/dhara)
[![Go Version](https://img.shields.io/github/go-mod/go-version/md-talim/dhara)](https://github.com/md-talim/dhara/blob/main/go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

Dhara is a distributed task queue library and service for Go, backed by PostgreSQL. It covers the full task lifecycle: enqueue, claim, retry, dead-letter. No external dependencies like Redis or RabbitMQ.

Use it two ways:

- **As an embeddable library:** call `client.Enqueue(...)` from your application (even inside your own DB transaction) and run a `dhara.Worker` to execute tasks.
- **As pre-built services:** `cmd/server` (HTTP API) and `cmd/worker` (task processor) are thin binaries assembled entirely from the library, so they can never drift from it.

[Quick Start](#quick-start) • [Migrations](#migrations) • [Design](#design-decisions) • [Benchmarks](#benchmarks) • [Configuration](#configuration) • [API Reference](#api--observability-reference)

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

## Design Decisions

1. **Atomic concurrency with PostgreSQL (`SKIP LOCKED`)**

    Task claiming uses transactional `SELECT ... FOR UPDATE SKIP LOCKED` instead of an external lock manager or broker. Multiple workers poll concurrently, each pending task is claimed by exactly one worker, and workers never block waiting on the same row.

2. **Failure recovery: heartbeats and the reaper**

    Workers periodically update a heartbeat for the task they're running. A background reaper then finds tasks that have missed their heartbeat window and atomically requeues them (the attempt was already counted when the task was claimed), or moves them to a dead-letter state once retries are exhausted.

3. **Retries with full jitter**

    Failed tasks retry on an exponential backoff with full jitter, which spreads retries across a safe window and keeps a thundering herd away from downstream services.

4. **Transactional Enqueue (`EnqueueTx`)**

    `EnqueueTx` inserts the task inside your existing transaction, so it commits atomically with the business writes that produced it: the task never exists without its trigger, and never survives a rollback. Idempotency uses `INSERT ... ON CONFLICT DO NOTHING`, so a duplicate replay never aborts the caller's transaction.

5. **At-least-once delivery**

    Tasks are guaranteed to run at least once, not exactly once. Failed attempts retry with backoff, stale heartbeats are reaped, and handlers still running during shutdown are cancelled and their tasks requeued. A handler can therefore see the same task more than once, so it must be idempotent, for example by deduplicating on the task ID or an idempotency key. Enqueue-side idempotency prevents duplicate submission, not duplicate execution. Handlers should honor `ctx.Done()`; one that ignores cancellation delays shutdown until `SHUTDOWN_TIMEOUT`.

## Benchmarks

Load tested with [k6](https://k6.io) against the HTTP API, backed by Postgres as the source of truth for what actually happened (task state, not just HTTP response codes).

**Setup:** 20 workers, `realistic_work` handler simulating 50-200ms of I/O, i3 (11th gen) / 8GB RAM / 500GB SSD, `MaxConns=25`.

### Finding a real bottleneck

Predicted throughput from worker count × handler duration: ~160 tasks/sec. First measured result: ~20 tasks/sec.

The cause was a claim loop that only attempted one claim per poll tick, regardless of how fast a task actually finished:

```go
select {
case <-ticker.C:
    processNext(ctx)
}
```

20 workers × 1 claim/sec (1s poll interval) = 20/sec, the math matched the measurement almost exactly. Fixed by claiming continuously while work is available, and only falling back to the poll interval once the queue is empty. Re-measured: **~148 tasks/sec**, within a few percent of the original prediction.

### Results

| Submission rate | Worker utilization | p50   | p95   | p99   | Task loss |
| :-------------- | :----------------- | :---- | :---- | :---- | :-------- |
| 100 tasks/sec   | ~67% (headroom)    | 181ms | 299ms | 380ms | 0 / 6,001 |
| ~148 tasks/sec  | ~100% (saturation) | 1.30s | 2.03s | 2.06s | 0 / 9,001 |

Latency is end-to-end (`completed_at - created_at`), not just HTTP enqueue time, enqueue latency alone stayed under 10ms (p99) throughout every run, including the one bottlenecked at 20/sec, which is exactly why the completion-side bottleneck was invisible until checked directly against task state in Postgres rather than HTTP response times.

At saturation, the jump in latency reflects queueing delay from running at the edge of worker capacity, not degraded correctness. Every run completed 100% of submitted tasks with zero loss, including a separate burst test that intentionally oversubmitted (120/sec against a then-20/sec ceiling, pre-fix) and fully recovered a 5,800-task backlog with zero losses once draining caught up.

### Reproducing this

```bash
k6 run benchmarks/load-test.js
watch -n 5 'psql "$DHARA_DATABASE_URL" -f benchmarks/drain-check.sql'
```

See [`benchmarks/RESULTS.md`](./benchmarks/RESULTS.md) for full methodology and raw data.

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

**3. Run a worker to execute tasks.** A `Worker` owns the goroutine pool, heartbeats, and the reaper. `Start` blocks until the context is cancelled, then cancels in-flight handlers and returns; interrupted tasks are requeued for redelivery.

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

| Variable              | Default        | Description                                                              |
| :-------------------- | :------------- | :----------------------------------------------------------------------- |
| `DHARA_DATABASE_URL`  | _Required_     | PostgreSQL connection string                                             |
| `DHARA_MAX_CONNS`     | `10`           | Maximum number of database connections in the pool                       |
| `DHARA_MIN_CONNS`     | `2`            | Minimum number of database connections in the pool                       |
| `AUTO_MIGRATE`        | `true`         | Automatically run embedded migrations on binary startup                  |
| `PORT`                | `8080`         | Port for the HTTP API server                                             |
| `WORKER_PREFIX`       | `dhara-worker` | Worker identity prefix                                                   |
| `WORKER_COUNT`        | `5`            | Size of the local concurrent worker pool                                 |
| `POLL_INTERVAL`       | `1s`           | How often workers poll for pending tasks                                 |
| `HEARTBEAT_INTERVAL`  | `30s`          | How often running tasks update their heartbeat                           |
| `HANDLER_TIMEOUT`     | `5m`           | Maximum execution time limit for any single task                         |
| `STUCK_THRESHOLD`     | `5m`           | How long a task can run without a heartbeat before it's considered stuck |
| `REAPER_INTERVAL`     | `30s`          | How often the reaper scans for stuck tasks                               |
| `BASE_BACKOFF`        | `1s`           | Initial retry backoff (exponential with full jitter)                     |
| `MAX_BACKOFF`         | `5m`           | Maximum retry backoff                                                    |
| `WORKER_METRICS_PORT` | `9090`         | Port for the worker's `/metrics` endpoint (`0` disables it)              |
| `SHUTDOWN_TIMEOUT`    | `30s`          | Maximum time to wait for workers to exit after shutdown                  |
| `LOG_LEVEL`           | `info`         | Logging verbosity (`debug`, `info`, `warn`, `error`)                     |
| `LOG_FORMAT`          | `text`         | Structured log output style (`text` or `json`)                           |

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

Metrics are **process-local**: each process exposes only what it produces. Scrape both endpoints and aggregate in PromQL; metric names are identical across processes.

| Endpoint                                    | Process    | Exposes                                                                                                                                                                                                                                                 |
| :------------------------------------------ | :--------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `GET /metrics` (also `/api/v1/metrics`)     | API server | Postgres queue gauges, `tasks_enqueued_total`, `tasks_canceled_total`                                                                                                                                                                                   |
| `GET :9090/metrics` (`WORKER_METRICS_PORT`) | Worker     | `tasks_completed_total`, `tasks_attempt_failures_total`, `tasks_retried_total`, `tasks_dead_total`, `tasks_reaped_total`, `reaper_failures_total`, `worker_claim_errors_total`, `workers_active`, `workers_inflight`, `task_execution_duration_seconds` |

The server endpoint always returns `200`. If Postgres is unreachable, `queue_metrics_up` is `0` and the gauge series are omitted, while process counters keep flowing.

Useful queries:

```promql
sum(rate(tasks_enqueued_total[5m]))
sum(rate(tasks_completed_total[5m]))
sum(tasks_by_status{status="pending"})
histogram_quantile(0.95, sum by (le) (rate(task_execution_duration_seconds_bucket[5m])))
```

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
