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

type TaskStore interface {
	Create(ctx context.Context, task *Task) (bool, error)
	GetById(ctx context.Context, id string) (*Task, error)
	Cancel(ctx context.Context, id string) (*Task, error)
	Claim(ctx context.Context, workerID string) (*Task, error)
	MarkCompleted(ctx context.Context, taskID, workerID string, durationMS int64) error
	MarkPending(ctx context.Context, task *Task, lastError string, runAt time.Time) error
	MarkDead(ctx context.Context, taskID, lastError, reason string) error
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

	if _, err = tx.Exec(ctx, `
		INSERT INTO task_logs(task_id, status, message)
		VALUES($1, $2, $3)
	`, task.ID, task.Status, "task canceled by user request"); err != nil {
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
			status = 'RUNNING'
			locked_by = $1
			locked_at = now()
			started_at = now()
			attempts = attempts + 1
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
		WHERE id = $1 AND status = 'RUNNING'
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, updateQuery, taskID).Scan(&id); err != nil {
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

func (ts *PostgresTaskStore) MarkPending(ctx context.Context, task *Task, lastError string, nextRunAt time.Time) error {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		UPDATE tasks
		SET
			status = 'PENDING',
			last_error = $2,
			run_at = $3,
			locked_by = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'RUNNING'
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, query, task.ID, lastError, nextRunAt).Scan(&id); err != nil {
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

func (ts *PostgresTaskStore) MarkDead(ctx context.Context, taskID, lastError, reason string) error {
	tx, err := ts.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE tasks
		SET
			status = 'DEAD',
			last_error = $2,
			locked_by = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1 AND status = 'RUNNING'
		RETURNING id
	`

	var id uuid.UUID
	if err := tx.QueryRow(ctx, updateQuery, taskID, lastError).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTaskNotFound
		}
		return fmt.Errorf("mark dead: %w", err)
	}

	deadLogMessage := fmt.Sprintf("makred dead: %s", lastError)
	if err := ts.insertLog(ctx, tx, taskID, "DEAD", deadLogMessage); err != nil {
		return fmt.Errorf("insert dead log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead: %w", err)
	}
	return nil
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

func (ts *PostgresTaskStore) insertLog(ctx context.Context, tx pgx.Tx, taskID, status, message string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task_logs(task_id, status, message)
		VALUES($1, $2, $3)
	`, taskID, status, message,
	)
	return err
}
