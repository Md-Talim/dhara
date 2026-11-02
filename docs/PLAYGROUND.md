# Dhara Interactive Playground

The playground is a small, self-contained binary that runs a real Dhara worker and a real HTTP API against PostgreSQL. It exists to make the queue's behavior observable: tasks are claimed with `FOR UPDATE SKIP LOCKED`, retried with backoff, dead-lettered, and recovered by the heartbeat reaper just as they are in the library.

There is no simulated execution in the browser. The dashboard polls PostgreSQL and the running worker process; every row and counter comes from those systems.

## Architecture

```
browser (vanilla HTML/CSS/JS)
        │  1.5s polling
        ▼
cmd/playground ──────────────► dhara.Client ──► PostgreSQL schema
        │                                        (dhara_playground)
        ├── dhara.Worker (in-process) ───────────┘
        │
        └── crash demo: same executable, -crash-worker
                    └── isolated schema (dhara_playground_crash)
```

- `cmd/playground` is a separate binary. `cmd/server` and `cmd/worker` are unchanged.
- The main playground uses an existing `dhara.Client`, `dhara.Worker`, and the existing demo handlers.
- A dedicated PostgreSQL schema is selected with `search_path`, so all unmodified Dhara SQL is automatically scoped to the playground.
- The crash demo uses a second dedicated schema and a child process running the same binary with `-crash-worker`.
- Only two playground-specific handlers exist: `playground_flaky` and `playground_crash`.

## Quick start

### Local, using the repository's development database

```bash
make db-up          # starts Postgres on localhost:5432
make playground     # runs cmd/playground on http://localhost:8080
```

The default schema is `dhara_playground`; it is created and migrated automatically. The crash schema is `dhara_playground_crash`.

### Local, fully containerized

```bash
make playground-up
# or:
docker compose -f docker-compose.playground.yml up --build
```

Open <http://localhost:8080>. Stop with `make playground-down` or `docker compose -f docker-compose.playground.yml down`.

### Build only

```bash
go build -o bin/dhara-playground ./cmd/playground
docker build --build-arg BINARY=playground -t dhara-playground .
```

## Scenarios

| Scenario | Handler | What happens |
| :--- | :--- | :--- |
| Burst | `echo` | Enqueues up to 50 jobs (default 25). Each sleeps 2s, so the worker pool visibly drains the ready queue. |
| Flaky, recover | `playground_flaky` | Fails until an absolute `succeed_at` timestamp, then succeeds. Default 6s, max 8 total attempts. |
| Flaky, dead-letter | `always_fail` | Fails every attempt. With `max_retries=3`, the third failed attempt writes a dead letter. |
| Slow task | `slow_task` | Sleeps 10 minutes, but the playground's default handler timeout is 12s. It times out, retries once, then dead-letters. |
| Worker crash | `playground_crash` | A real child worker claims a 6s task; the parent kills the child, waits for the reaper to requeue the stale task, restarts the child, and observes attempt 2 complete. |

A handler timeout is an ordinary handler failure. The reaper path is distinct: a RUNNING task that stops refreshing `locked_at` is recovered by `RequeueStaleRunning`. The crash demo is the only path that intentionally stops heartbeats.

## Crash demo details

The parent process owns the visible reaper loop:

- isolated schema: `PLAYGROUND_CRASH_SCHEMA`
- stale threshold: `PLAYGROUND_CRASH_STUCK_THRESHOLD` (default 4s)
- reaper interval: `PLAYGROUND_CRASH_REAPER_INTERVAL` (default 1s)
- child worker heartbeat: `PLAYGROUND_CRASH_HEARTBEAT_INTERVAL` (default 1s)
- child has one worker and registers only `playground_crash`

Sequence:

1. Start the child worker.
2. Enqueue `playground_crash` with `max_retries=2` and a 6s payload.
3. Wait until PostgreSQL reports `RUNNING` and the child has claimed it.
4. Kill only the child process. Its heartbeat stops.
5. The parent reaper sees the stale lease and returns the task to `PENDING`.
6. Restart the child. It claims attempt 2 and completes it.

The demo is single-flight per process. Only one visitor can run it at a time. Starting it requires a valid playground session header and is rate-limited like other mutations.

If the crash schema cannot be prepared, the crash demo is disabled and the rest of the playground continues to work. If spawning fails when a demo starts, the demo reports a failed phase and the rest of the playground is unaffected. Set `PLAYGROUND_ENABLE_CRASH_DEMO=false` to disable it explicitly.

## Public API

All task mutation endpoints require `X-Playground-Session: <uuid>`. The value is a browser-generated correlation identifier, **not authentication**.

| Method | Path | Purpose |
| :--- | :--- | :--- |
| `GET` | `/api/v1/livez` | Process liveness |
| `GET` | `/api/v1/readyz` | Database readiness |
| `GET` | `/api/v1/tasks` | Basic task listing (same semantics as the core API) |
| `POST` | `/api/v1/tasks` | Enqueue a predefined task type |
| `GET` | `/api/v1/tasks/{id}` | Basic task record |
| `DELETE` | `/api/v1/tasks/{id}` | Cancel an owned pending task |
| `POST` | `/api/v1/tasks/{id}/retry` | Retry an owned DEAD task |
| `GET` | `/api/v1/playground/snapshot` | Dashboard snapshot; `scope=all\|mine`, optional `status` and `limit` |
| `GET` | `/api/v1/playground/tasks/{id}` | Task detail; payload and logs only for the owning session |
| `POST` | `/api/v1/playground/scenarios/burst` | `{"size":25}` |
| `POST` | `/api/v1/playground/scenarios/flaky` | `{"count":3,"mode":"recover\|dead","succeed_after_ms":6000}` |
| `POST` | `/api/v1/playground/scenarios/slow` | `{"count":1}` |
| `POST` | `/api/v1/playground/scenarios/crash` | Start the isolated crash demo |
| `GET` | `/metrics` | Core client/queue Prometheus metrics |
| `GET` | `/worker/metrics` | In-process worker Prometheus metrics |

### Session ownership

Playground-created tasks get an idempotency key of the form:

```
demo:<session-uuid>:<nonce>
```

This is used only for correlation, filtering, and deciding whether to reveal payloads and logs. It is not a security boundary against someone who already knows another visitor's 128-bit UUID. The global task list never includes payloads, detailed errors, logs, or idempotency keys. Standard task list/get responses are sanitized too, because the core task model carries the idempotency key. A task detail for a non-owner shows aggregate state and timing only.

## Configuration

The playground reuses the existing `DHARA_*`, worker, and logging variable names, with shorter defaults for a responsive demo.

| Variable | Default | Notes |
| :--- | :--- | :--- |
| `DHARA_DATABASE_URL` | required | PostgreSQL connection string |
| `DHARA_MAX_CONNS` / `DHARA_MIN_CONNS` | `10` / `2` | Connection pool size |
| `PORT` | `8080` | HTTP listen port |
| `AUTO_MIGRATE` | `true` | Run embedded migrations for both playground schemas at startup |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown budget |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | Structured logging |
| `PLAYGROUND_DB_SCHEMA` | `dhara_playground` | Main schema; must be dedicated, not `public` or `pg_*` |
| `PLAYGROUND_CRASH_SCHEMA` | `dhara_playground_crash` | Isolated crash-demo schema; must differ from the main schema |
| `PLAYGROUND_TRUST_PROXY` | `false` | Trust the first `X-Forwarded-For` hop for rate-limit keys |
| `WORKER_PREFIX` | `playground-worker` | Worker identity prefix |
| `WORKER_COUNT` | `4` | In-process workers (1–32) |
| `POLL_INTERVAL` | `250ms` | Worker claim poll interval |
| `HEARTBEAT_INTERVAL` | `2s` | Main worker heartbeat |
| `HANDLER_TIMEOUT` | `12s` | Main handler deadline; drives the slow-task demo |
| `STUCK_THRESHOLD` | `10s` | Main reaper stale threshold |
| `REAPER_INTERVAL` | `2s` | Main reaper interval |
| `BASE_BACKOFF` / `MAX_BACKOFF` | `1s` / `30s` | Retry backoff bounds |
| `PLAYGROUND_MAX_BATCH` | `50` | Maximum scenario batch |
| `PLAYGROUND_MAX_BODY_BYTES` | `16384` | Request body cap |
| `PLAYGROUND_MAX_PAYLOAD_BYTES` | `4096` | Task payload cap |
| `PLAYGROUND_MAX_PENDING` | `500` | Pending + running task cap |
| `PLAYGROUND_RATE_LIMIT_PER_MIN` / `_BURST` | `30` / `10` | Per-IP mutation rate |
| `PLAYGROUND_SESSION_RATE_LIMIT_PER_MIN` / `_BURST` | `20` / `8` | Per-session mutation rate |
| `PLAYGROUND_READ_RATE_LIMIT_PER_MIN` / `_BURST` | `180` / `40` | Per-IP read rate |
| `PLAYGROUND_CLEANUP_INTERVAL` | `5m` | Retention sweep interval |
| `PLAYGROUND_TASK_RETENTION` | `1h` | Age after which non-RUNNING demo rows are deleted |
| `PLAYGROUND_ENABLE_CRASH_DEMO` | `true` | Enable the isolated crash demo |
| `PLAYGROUND_CRASH_DEMO_TIMEOUT` | `35s` | Overall crash-demo budget |
| `PLAYGROUND_CRASH_TASK_DURATION` | `6s` | Duration of the crash-demo task |
| `PLAYGROUND_CRASH_HEARTBEAT_INTERVAL` | `1s` | Child worker heartbeat |
| `PLAYGROUND_CRASH_STUCK_THRESHOLD` | `4s` | Parent crash reaper threshold |
| `PLAYGROUND_CRASH_REAPER_INTERVAL` | `1s` | Parent crash reaper interval |
| `PLAYGROUND_CRASH_HANDLER_TIMEOUT` | `30s` | Child handler timeout |
| `PLAYGROUND_CRASH_WORKER_PREFIX` | `playground-crash-worker` | Child worker prefix |
| `PLAYGROUND_CRASH_POLL_INTERVAL` | `150ms` | Child claim poll interval |

Durations accept Go duration strings (`30s`, `5m`) or integers interpreted as seconds.

### Migrations and schema isolation

The playground runs the embedded `dhara.Migrate` against its dedicated schema. Vow's migration table (`dhara_vow_migrations`) is therefore created inside that schema, not in `public`. The `dhara_vow_lock` name is a PostgreSQL advisory lock, which is database-wide rather than schema-scoped.

Keep `AUTO_MIGRATE=true` for the playground unless you have a separate migration path that sets `search_path` correctly. `cmd/migrate` is not schema-aware; it migrates the connection's default schema. The playground's startup migration is the supported path. The database role must be able to `CREATE SCHEMA` and create tables/functions, or the schemas must be pre-created and owned/granted appropriately.

### Retention

Every five minutes (configurable) the playground deletes rows older than the retention window:

1. `dead_letters` for old tasks
2. `task_logs` for old tasks
3. `tasks` older than the cutoff, excluding `RUNNING`

It does this separately for the main and crash schemas, and only through pools whose `search_path` points at those schemas. There is no global `DELETE` across the database.

## Safety properties

- Only predefined task types are accepted. The public API cannot register handlers, run shell commands, or perform arbitrary outbound requests.
- `send_email` is the existing demo handler; it logs and sleeps, but sends no email.
- Request bodies, payloads, batch sizes, queue depth, and retention are bounded.
- Validation rejects unknown JSON fields, invalid task types, malformed payloads, and oversized requests.
- Mutations require a session header and are rate-limited per IP and per session. Reads are rate-limited per IP.
- Worker crash simulation is isolated to a child process and a separate schema. Visitors cannot stop, start, scale, or control the main worker pool.
- Global snapshots omit payloads, payload-derived errors, logs, and idempotency keys. Detailed data is returned only to the session that created the task. Standard task list/get responses are sanitized because the core task model carries the key.
- CSP, `nosniff`, `no-referrer`, and `DENY` framing headers are set on every response.
- Browser state is a UUID in `localStorage`; it is correlation only, never authentication.
- Payloads are demo data. They are stored for the retention window and may be written to server logs by existing handlers. Do not submit secrets.

## Known limitations

- **Single instance.** In-memory rate limits, the crash-demo single-flight lock, and `os.Executable()`-based child spawning assume one playground process. Run one replica and scale vertically.
- **Process-local metrics.** Client and worker counters from `/metrics` and `/worker/metrics` describe this process only. Queue gauges and task rows come from PostgreSQL.
- **Crash demo requires process spawning.** The container/platform must allow the playground to fork/exec itself. If not, disable the demo.
- **Crash demo needs the same binary.** It invokes the current executable with `-crash-worker`. A wrapper or custom launcher that changes `os.Executable()` semantics may break it.
- **Global crash status is public.** Any visitor can watch the active crash demo's phase and task state. Payloads and logs are not exposed.
- **No authentication.** Sessions prevent accidental cross-visitor detail views; they are not an authorization system.
- **The crash demo is not exercised by the automated Go tests.** The test binary is not the playground binary, so the child-process path is verified manually and by running the built playground.
- **No SSE or WebSockets.** Polling every 1.5s is sufficient for this queue size and pauses when the tab is hidden.
- **Retention never deletes RUNNING tasks.** A permanently stuck RUNNING row relies on the reaper to move it out of RUNNING before cleanup.

## Testing

```bash
make test                                              # full suite with test database
go test ./internal/playground -short                   # fast unit tests
DHARA_TEST_DATABASE_URL='postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable' \
  go test ./internal/playground -count=1 -v           # includes DB-backed playground tests
```

The DB-backed tests create uniquely named schemas, migrate them, verify schema-scoped retention, and exercise the real worker retry/dead-letter/retry-to-completion path. They drop their schemas afterwards.

## Deployment handoff

This repository intentionally stops at a deployable artifact and documented local/compose workflow. It does not create cloud resources, DNS records, accounts, or credentials.

Recommended shape:

1. Build the image: `docker build --build-arg BINARY=playground -t dhara-playground .`
2. Provide a PostgreSQL database and a strong `DHARA_DATABASE_URL`.
3. Set `PLAYGROUND_DB_SCHEMA` and `PLAYGROUND_CRASH_SCHEMA` to dedicated names.
4. Keep `AUTO_MIGRATE=true` for the first start, or pre-create/migrate the schemas with `search_path` set correctly.
5. Run exactly one replica. Put TLS and any edge rate limiting in front of it.
6. Point health checks at `GET /api/v1/readyz`.
7. Set `PLAYGROUND_TRUST_PROXY=true` only when a trusted reverse proxy supplies `X-Forwarded-For`.
8. Ensure the runtime allows child processes if the crash demo is enabled.
9. Monitor container memory/CPU and PostgreSQL growth. Retention bounds task rows, but completed tasks still consume space during the window.

Dhara is a task queue, not a sandbox. The playground's safety comes from a fixed handler allowlist and bounded inputs, not from executing untrusted code.
