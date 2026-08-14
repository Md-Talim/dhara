-- =============================================================================
-- Migration: 000001_init_task_queue
-- =============================================================================

-- =============================================================================
-- TASKS
-- =============================================================================

CREATE TABLE IF NOT EXISTS tasks (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Idempotency scoped to (type, idempotency_key) — not globally unique.
    -- Nullable: callers opt in per submission; no key = no dedup.
    idempotency_key VARCHAR(255)    NULL,

    type            VARCHAR(100)    NOT NULL CHECK (type <> ''),
    payload         JSONB           NOT NULL DEFAULT '{}',
    payload_hash    BYTEA           NOT NULL,

    -- State machine: PENDING → RUNNING → COMPLETED
    --                                  ↘ retry → PENDING
    --                                  ↘ exhausted → DEAD
    --                PENDING → CANCELED (before picked up)
    -- FAILED is intentionally absent: retry loop stays PENDING.
    -- DEAD is the only terminal failure state.
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'CANCELED', 'DEAD')),

    priority        INT             NOT NULL DEFAULT 0,   -- higher = claimed first
    attempts        INT             NOT NULL DEFAULT 0,
    max_retries     INT             NOT NULL DEFAULT 3,

    run_at          TIMESTAMPTZ     NOT NULL DEFAULT now(), -- supports delayed tasks

    -- Lifecycle timestamps
    started_at      TIMESTAMPTZ     NULL,
    completed_at    TIMESTAMPTZ     NULL,

    -- Worker ownership / heartbeat.
    -- locked_at is refreshed every HEARTBEAT_INTERVAL while RUNNING.
    -- Reaper fires when: status='RUNNING' AND locked_at < now() - STUCK_THRESHOLD
    locked_by       VARCHAR(100)    NULL,
    locked_at       TIMESTAMPTZ     NULL,

    last_error      TEXT            NULL,  -- most recent error; full history in task_logs

    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Scoped idempotency: same key is allowed across different task types.
-- NULL values are excluded from unique constraints in Postgres — nullable is safe.
ALTER TABLE tasks
    ADD CONSTRAINT tasks_idempotency_type_key UNIQUE (type, idempotency_key);

-- -----------------------------------------------------------------------------
-- Indexes: tasks
-- -----------------------------------------------------------------------------

-- Claim query:
--   WHERE status = 'PENDING' AND run_at <= now()
--   ORDER BY priority DESC, run_at ASC
--   LIMIT 1 FOR UPDATE SKIP LOCKED
-- status is implied by the partial filter — excluded from column list.
CREATE INDEX idx_tasks_claimable
    ON tasks (priority DESC, run_at ASC)
    WHERE status = 'PENDING';

-- Reaper query:
--   WHERE status = 'RUNNING' AND locked_at < now() - interval '45s'
-- Partial on RUNNING keeps this index tiny.
CREATE INDEX idx_tasks_reaper
    ON tasks (locked_at)
    WHERE status = 'RUNNING';


-- =============================================================================
-- TASK LOGS (append-only audit trail)
-- =============================================================================

CREATE TABLE IF NOT EXISTS task_logs (
    id          BIGSERIAL       PRIMARY KEY,
    task_id     UUID            NOT NULL REFERENCES tasks (id),
    status      VARCHAR(20)     NOT NULL,
    message     TEXT            NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- GET /api/tasks/:id?fields=logs — always returned in chronological order.
-- created_at in the index eliminates the sort step.
CREATE INDEX idx_task_logs_task_id
    ON task_logs (task_id, created_at ASC);


-- =============================================================================
-- DEAD LETTERS
-- =============================================================================

CREATE TABLE IF NOT EXISTS dead_letters (
    id          BIGSERIAL       PRIMARY KEY,
    task_id     UUID            NOT NULL REFERENCES tasks (id),
    last_error  TEXT            NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX idx_dead_letters_task_id
    ON dead_letters (task_id);
