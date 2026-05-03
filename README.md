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

The long-term goal is to evolve Dhara into a production-ready distributed task queue with:

- reliable retries and backoff
- stronger task recovery semantics
- dead-letter handling
- richer observability
- improved task routing and scheduling
- safer cancellation and retry flows
- better operational tooling for debugging and support

## Status

**In active development**

Package names, structure, and startup flow may still change as the project matures.

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
- Migration runner for database schema setup
- `/livez` and `/readyz` health endpoints
- `/healthz` style readiness/liveness gates, depending on routing setup
- Prometheus-style `/metrics` endpoint
- Structured logging with `slog`

## Project overview

Dhara uses PostgreSQL as the source of truth for task state.

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

To apply database migrations:

```bash
go run ./cmd/migrate
```

Make sure `DHARA_DATABASE_URL` is set.

## Running the server

```bash
go run ./cmd/server
```

Environment:

- `DHARA_DATABASE_URL` — PostgreSQL connection string

The server will:

- open the database pool
- run migrations
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

- stronger retry semantics with jitter
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
