package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var pgErrUniqueViolation = "23505"

type Task struct {
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
	PayloadHash    []byte
	IdempotencyKey *string
	Status         string
	Priority       int
	Attempts       int
	MaxRetries     int
	RunAt          time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	LockedAt       *time.Time
	LockedBy       *string
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// TaskListFilter holds the optional filters and pagination for listing tasks.
type TaskListFilter struct {
	Status   string
	Type     string
	Retrying *bool
	Limit    int
	Offset   int
}

type TaskStore interface {
	Create(ctx context.Context, task *Task) (bool, error)
	GetById(ctx context.Context, id string) (*Task, error)
	List(ctx context.Context, filter TaskListFilter) (tasks []Task, total int, err error)
	Cancel(ctx context.Context, id string) (*Task, error)
	Claim(ctx context.Context, workerID string) (*Task, error)
	Heartbeat(ctx context.Context, taskID, workerID string) error
	MarkCompleted(ctx context.Context, taskID, workerID string, durationMS int64) error
	MarkPending(ctx context.Context, task *Task, workerID, lastError string, runAt time.Time) error
	MarkDead(ctx context.Context, taskID, workerID, lastError, reason string) error
	RetryDead(ctx context.Context, taskID string) (*Task, error)
	RequeueStaleRunning(ctx context.Context, staleThreshold time.Duration, reaperID string) (int64, error)
}

type PostgresTaskStore struct {
	db *pgxpool.Pool
}

func NewTaskStore(db *pgxpool.Pool) *PostgresTaskStore {
	return &PostgresTaskStore{db: db}
}

func (ts *PostgresTaskStore) Create(ctx context.Context, task *Task) (bool, error) {
	payloadHash, err := hashPayload(task.Payload) // new task payload hash
	if err != nil {
		return false, err
	}

	query := `
	INSERT INTO tasks(type, payload, payload_hash, idempotency_key,  priority, max_retries, run_at)
	VALUES($1, $2, $3, $4, $5, $6, $7)
	RETURNING id, status, attempts, created_at, updated_at
	`

	err = ts.db.QueryRow(
		ctx,
		query,
		task.Type,
		task.Payload,
		payloadHash,
		task.IdempotencyKey,
		task.Priority,
		task.MaxRetries,
		task.RunAt,
	).Scan(&task.ID, &task.Status, &task.Attempts, &task.CreatedAt, &task.UpdatedAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgErrUniqueViolation {
			if pgErr.ConstraintName == "tasks_idempotency_type_key" {
				existingTask, err := ts.getByTypeAndIdempotencyKey(ctx, task.Type, *task.IdempotencyKey)
				if err != nil {
					return false, err
				}
				if !bytes.Equal(existingTask.PayloadHash, payloadHash) {
					return false, ErrTaskConflict
				}

				*task = *existingTask
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func (ts *PostgresTaskStore) GetById(ctx context.Context, id string) (*Task, error) {
	query := `
	SELECT
		id, type, payload, idempotency_key, status, priority, attempts, max_retries,
		run_at, started_at, completed_at, last_error, created_at, updated_at
    FROM tasks WHERE id = $1
	`

	task := &Task{}
	err := ts.db.QueryRow(ctx, query, id).Scan(
		&task.ID, &task.Type, &task.Payload, &task.IdempotencyKey, &task.Status, &task.Priority, &task.Attempts,
		&task.MaxRetries, &task.RunAt, &task.StartedAt, &task.CompletedAt, &task.LastError, &task.CreatedAt, &task.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (ts *PostgresTaskStore) List(ctx context.Context, filter TaskListFilter) ([]Task, int, error) {
	// Build dynamic WHERE clauses.
	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, filter.Status)
		argIdx++
	}

	if filter.Type != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("type = $%d", argIdx))
		args = append(args, filter.Type)
		argIdx++
	}

	if filter.Retrying != nil && *filter.Retrying {
		whereClauses = append(whereClauses, "status = 'PENDING'", "attempts > 0")
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + whereClauses[0]
		for _, c := range whereClauses[1:] {
			whereSQL += " AND " + c
		}
	}

	// Count total matching rows.
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM tasks %s", whereSQL)
	var total int
	if err := ts.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count tasks: %w", err)
	}

	// Fetch paginated results (never include payload).
	selectQuery := fmt.Sprintf(`
	SELECT
		id, type, idempotency_key, status, priority, attempts, max_retries,
		run_at, started_at, completed_at, last_error, created_at, updated_at
	FROM tasks %s
	ORDER BY created_at DESC
	LIMIT $%d OFFSET $%d
	`, whereSQL, argIdx, argIdx+1)

	args = append(args, filter.Limit, filter.Offset)

	rows, err := ts.db.Query(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(
			&t.ID, &t.Type, &t.IdempotencyKey, &t.Status, &t.Priority, &t.Attempts,
			&t.MaxRetries, &t.RunAt, &t.StartedAt, &t.CompletedAt, &t.LastError, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan task row: %w", err)
		}
		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate task rows: %w", err)
	}

	return tasks, total, nil
}

func (ts *PostgresTaskStore) Cancel(ctx context.Context, id string) (*Task, error) {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE tasks SET status = 'CANCELED', updated_at = now()
        WHERE id = $1 AND status = 'PENDING'
        RETURNING id, type, status, priority, attempts, max_retries,
                  run_at, idempotency_key, started_at, completed_at,
                  last_error, created_at, updated_at
    `

	task := &Task{}
	err = tx.QueryRow(ctx, updateQuery, id).Scan(
		&task.ID, &task.Type, &task.Status, &task.Priority, &task.Attempts,
		&task.MaxRetries, &task.RunAt, &task.IdempotencyKey, &task.StartedAt,
		&task.CompletedAt, &task.LastError, &task.CreatedAt, &task.UpdatedAt,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("cancel task: %w", err) // read db error
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// not PENDING - no log needed, return current state
		current, err := ts.GetById(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrTaskNotFound // task not found
			}
			return nil, err
		}
		return current, nil
	}

	if err := ts.insertLog(ctx, tx, task.ID.String(), "CANCELED", "task canceled by user request"); err != nil {
		return nil, fmt.Errorf("insert cancel log: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit cancel: %w", err)
	}

	return task, nil
}

func (ts *PostgresTaskStore) Claim(ctx context.Context, workerID string) (*Task, error) {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	claimQuery := `
		UPDATE tasks
		SET
			status = 'RUNNING',
			locked_by = $1,
			locked_at = now(),
			started_at = now(),
			attempts = attempts + 1,
			updated_at = now()
		WHERE id = (
			SELECT id FROM tasks
			WHERE status = 'PENDING' AND run_at <= now()
			ORDER BY priority DESC, run_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, type, payload, attempts, max_retries, idempotency_key
	`

	task := &Task{}
	err = tx.QueryRow(ctx, claimQuery, workerID).Scan(
		&task.ID, &task.Type, &task.Payload,
		&task.Attempts, &task.MaxRetries, &task.IdempotencyKey,
	)
	if err == pgx.ErrNoRows {
		return nil, ErrTaskNotAvailable
	}
	if err != nil {
		return nil, err
	}

	claimLogMessage := fmt.Sprintf("claimed by worker %s (attempt %d/%d)", workerID, task.Attempts, task.MaxRetries)
	if err := ts.insertLog(ctx, tx, task.ID.String(), "RUNNING", claimLogMessage); err != nil {
		return nil, fmt.Errorf("insert claim log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return task, nil
}

func (ts *PostgresTaskStore) Heartbeat(ctx context.Context, taskID, workerID string) error {
	query := `
		UPDATE tasks
		SET
			locked_at = now(),
			updated_at = now()
		WHERE id = $1
			AND status = 'RUNNING'
			AND locked_by = $2
	`

	tag, err := ts.db.Exec(ctx, query, taskID, workerID)
	if err != nil {
		return fmt.Errorf("hearbeat task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskOwnershipLost
	}

	return nil
}

func (ts *PostgresTaskStore) MarkCompleted(ctx context.Context, taskID, workerID string, durationMs int64) error {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE tasks
		SET
			status = 'COMPLETED',
			completed_at = now(),
			locked_by = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1
			AND status = 'RUNNING'
			AND locked_by = $2
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, updateQuery, taskID, workerID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("mark complete: %w", err)
	}

	completedLogMessage := fmt.Sprintf("completed by worker %s after %dms", workerID, durationMs)
	if err := ts.insertLog(ctx, tx, taskID, "COMPLETED", completedLogMessage); err != nil {
		return fmt.Errorf("insert complete log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete: %w", err)
	}

	return nil
}

func (ts *PostgresTaskStore) MarkPending(ctx context.Context, task *Task, workerID, lastError string, nextRunAt time.Time) error {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		UPDATE tasks
		SET
			status = 'PENDING',
			last_error = $3,
			run_at = $4,
			locked_by = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1
			AND status = 'RUNNING'
			AND locked_by = $2
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, query, task.ID, workerID, lastError, nextRunAt).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("mark pending: %w", err)
	}

	pendingLogMessage := fmt.Sprintf("attempt %d/%d failed, retrying at %s: %s",
		task.Attempts, task.MaxRetries,
		nextRunAt.UTC().Format(time.RFC3339),
		lastError,
	)
	if err := ts.insertLog(ctx, tx, task.ID.String(), "PENDING", pendingLogMessage); err != nil {
		return fmt.Errorf("insert pending log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit pending: %w", err)
	}

	return nil
}

func (ts *PostgresTaskStore) MarkDead(ctx context.Context, taskID, workerID, lastError, reason string) error {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE tasks
		SET
			status = 'DEAD',
			last_error = $3,
			locked_by = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1
			AND status = 'RUNNING'
			AND locked_by = $2
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, updateQuery, taskID, workerID, lastError).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("mark dead: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO dead_letters(task_id, last_error)
		VALUES ($1, $2)
	`, taskID, lastError); err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}

	deadLogMessage := fmt.Sprintf("marked dead: %s", reason)
	if err := ts.insertLog(ctx, tx, taskID, "DEAD", deadLogMessage); err != nil {
		return fmt.Errorf("insert dead log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead: %w", err)
	}
	return nil
}

func (ts *PostgresTaskStore) RetryDead(ctx context.Context, taskID string) (*Task, error) {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE tasks
		SET
			status = 'PENDING',
			attempts = 0,
			run_at = now(),
			locked_by = NULL,
			locked_at = NULL,
			started_at = NULL,
			completed_at = NULL,
			last_error = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'DEAD'
		RETURNING id, type, idempotency_key, status, priority, attempts, max_retries,
		          run_at, started_at, completed_at, last_error, created_at, updated_at
	`

	task := &Task{}
	err = tx.QueryRow(ctx, updateQuery, taskID).Scan(
		&task.ID, &task.Type, &task.IdempotencyKey, &task.Status, &task.Priority, &task.Attempts,
		&task.MaxRetries, &task.RunAt, &task.StartedAt, &task.CompletedAt, &task.LastError, &task.CreatedAt, &task.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Determine whether the task doesn't exist or isn't DEAD.
			existing, lookupErr := ts.GetById(ctx, taskID)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if existing == nil {
				return nil, ErrTaskNotFound
			}
			return nil, ErrTaskNotDead
		}
		return nil, fmt.Errorf("retry dead task: %w", err)
	}

	// Remove from dead_letters since the task is being retried.
	if _, err := tx.Exec(ctx, `DELETE FROM dead_letters WHERE task_id = $1`, taskID); err != nil {
		return nil, fmt.Errorf("delete dead letter: %w", err)
	}

	if err := ts.insertLog(ctx, tx, taskID, "PENDING", "manual retry requested"); err != nil {
		return nil, fmt.Errorf("insert retry log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit retry dead: %w", err)
	}

	return task, nil
}

func (ts *PostgresTaskStore) getByTypeAndIdempotencyKey(ctx context.Context, taskType, idempotencyKey string) (*Task, error) {
	query := `
	SELECT
		id, type, payload, payload_hash, idempotency_key, status, priority, attempts, max_retries,
		run_at, started_at, completed_at, last_error, created_at, updated_at
    FROM tasks WHERE type = $1 AND idempotency_key = $2
	`

	task := &Task{}
	err := ts.db.QueryRow(ctx, query, taskType, idempotencyKey).Scan(
		&task.ID, &task.Type, &task.Payload, &task.PayloadHash, &task.IdempotencyKey, &task.Status, &task.Priority, &task.Attempts,
		&task.MaxRetries, &task.RunAt, &task.StartedAt, &task.CompletedAt, &task.LastError, &task.CreatedAt, &task.UpdatedAt,
	)

	return task, err
}

// RequeueStaleRunning finds tasks that have been in RUNNING state with a locked_at timestamp older than the staleThreshold.
// For each such task, if attempts >= max_retries, it marks the task as DEAD; otherwise, it resets it to PENDING for retry.
// It returns the count of tasks that were requeued or marked dead.
func (ts *PostgresTaskStore) RequeueStaleRunning(ctx context.Context, staleThreshold time.Duration, reaperID string) (int64, error) {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		WITH stale AS (
			SELECT id, attempts, max_retries, locked_by
			FROM tasks
			WHERE
				status = 'RUNNING'
				AND locked_at < (now() - $1::interval)
			FOR UPDATE SKIP LOCKED
		),
		to_dead AS (
			UPDATE tasks t
			SET
				status = 'DEAD',
				last_error = 'reaped: stale heartbeat (max retries exhausted)',
				locked_by = NULL,
				locked_at = NULL,
				updated_at = now()
			FROM stale s
			WHERE
				t.id = s.id
				AND s.attempts >= s.max_retries
			RETURNING t.id, t.status, t.last_error, s.attempts, s.max_retries, s.locked_by
		),
		insert_dead_letters AS (
			INSERT INTO dead_letters(task_id, last_error)
			SELECT id, last_error
			FROM to_dead
			RETURNING task_id
		),
		to_pending AS (
			UPDATE tasks t
			SET
				status = 'PENDING',
				last_error = 'reaped: stale heartbeat',
				run_at = now(),
				locked_by = NULL,
				locked_at = NULL,
				updated_at = now()
			FROM stale s
			WHERE
				t.id = s.id
				AND s.attempts < s.max_retries
			RETURNING t.id, t.status, s.attempts, s.max_retries, s.locked_by
		)
		SELECT id, status, attempts, max_retries, COALESCE(locked_by, '')
		FROM to_dead
		UNION ALL
		SELECT id, status, attempts, max_retries, COALESCE(locked_by, '')
		FROM to_pending
	`

	rows, err := tx.Query(ctx, query, staleThreshold.String())
	if err != nil {
		return 0, fmt.Errorf("requeue stale running: %w", err)
	}
	defer rows.Close()

	var changed int64
	for rows.Next() {
		var (
			taskID     uuid.UUID
			newStatus  string
			attempts   int
			maxRetries int
			lockedBy   string
		)
		if err := rows.Scan(&taskID, &newStatus, &attempts, &maxRetries, &lockedBy); err != nil {
			return 0, fmt.Errorf("scan reaped row: %w", err)
		}

		var msg string
		switch newStatus {
		case "PENDING":
			msg = fmt.Sprintf(
				"reaped by %s: stale heartbeat; previous worker=%s; attempt %d/%d requeued",
				reaperID, lockedBy, attempts, maxRetries,
			)
		case "DEAD":
			msg = fmt.Sprintf(
				"reaped by %s: stale heartbeat; previous worker=%s; attempt exhausted (%d/%d)",
				reaperID, lockedBy, attempts, maxRetries,
			)
		default:
			msg = fmt.Sprintf("reaped by %s: stale heartbeat", reaperID)
		}

		if err := ts.insertLog(ctx, tx, taskID.String(), newStatus, msg); err != nil {
			return 0, fmt.Errorf("insert reaper log: %w", err)
		}

		changed++
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate reaped rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit requeue stale running: %w", err)
	}

	return changed, nil
}

func (ts *PostgresTaskStore) insertLog(ctx context.Context, tx pgx.Tx, taskID, status, message string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_logs(task_id, status, message)
		VALUES($1, $2, $3)
	`, taskID, status, message,
	)
	return err
}
