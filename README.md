# Relay

Relay is a Go-based distributed task queue built on PostgreSQL.

It is an active work in progress. The current codebase focuses on the core task lifecycle:

- creating tasks via HTTP
- claiming tasks from PostgreSQL
- executing tasks in worker pools
- recording task status changes and logs
- running schema migrations
- exposing liveness and readiness endpoints

The long-term goal is to evolve Relay into a production-ready distributed task queue with:

- reliable retries and backoff
- heartbeats and stuck-task recovery
- dead-letter handling
- task listing and task detail APIs
- observability and metrics
- safer task cancellation and recovery flows

## Status

**In active development**

Some package names and structure may still change as the project matures.

## Current features

- PostgreSQL-backed task storage
- Task creation API
- Task retrieval API
- Task cancellation API
- Worker pool that polls for pending tasks
- Task handlers for demo workloads
- Migration runner for database schema setup
- `/livez` and `/readyz` health endpoints
- Structured logging with `slog`

## Project overview

Relay uses PostgreSQL as the source of truth for task state.

### Main components

- **HTTP server**
  Exposes the task API and health endpoints.

- **Task store**
  Handles database operations for creating, claiming, updating, and reading tasks.

- **Worker pool**
  Continuously polls for pending tasks and executes handlers concurrently.

- **Task handlers**
  Business logic for task types such as `echo`, `send_email`, `always_fail`, and `slow_task`.

- **Migrations**
  Sets up and evolves the database schema.

## Current execution flow

1. A client submits a task through the HTTP API.
2. The task is stored in PostgreSQL with `PENDING` status.
3. Worker goroutines poll for available tasks.
4. A worker claims a task atomically.
5. The matching handler executes the task payload.
6. The task is marked `COMPLETED`, `DEAD`, or later retried.
7. Task status changes are recorded in `task_logs`.

## Health endpoints

- `GET /api/v1/livez`
  Liveness probe. Returns `200` when the process is running.

- `GET /api/v1/readyz`
  Readiness probe. Checks database connectivity and readiness gates.

- `GET /api/v1/health`
  Alias of readiness.

## HTTP API

The API is still evolving, but currently includes:

- `POST /api/v1/tasks`
- `GET /api/v1/tasks/{id}`
- `DELETE /api/v1/tasks/{id}`

More endpoints will be added later, including:

- task listing
- retrying dead tasks
- metrics
- richer field selection

## Migrations

To apply database migrations:

```bash
go run ./cmd/migrate
```

Make sure `RELAY_DATABASE_URL` is set.

## Running the server

```bash
go run ./cmd/server
```

Environment:

- `RELAY_DATABASE_URL` — PostgreSQL connection string

The server will:

- open the database pool
- run migrations
- start the worker pool
- serve HTTP on `:8080`

## Demo task types

The current demo handlers include:

- `echo`
- `send_email`
- `always_fail`
- `slow_task`

These are intentionally simple and are meant to exercise the worker flow.

## Design goals

Relay is being built with the following goals in mind:

- correctness first
- PostgreSQL as the durable queue backend
- clear task state transitions
- observable worker behavior
- minimal dependencies
- no HTTP framework, just Go’s standard library

## Notes

This repository is not finished yet.
Package names, structure, and APIs may be refactored as development continues.

## Planned work

- retries with exponential backoff and jitter
- heartbeat updates and reaper recovery
- dead-letter table support
- task list and detail APIs
- manual retry of dead tasks
- metrics endpoint
- more complete health/readiness gates
- improved observability
- stronger validation and test coverage

## License

TBD
