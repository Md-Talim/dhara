package playground

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StoredTask is the internal read model needed by the playground. Dhara's
// public Task deliberately omits payload and lease fields; the playground
// reads those directly from its own isolated schema.
type StoredTask struct {
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
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

type TaskView struct {
	ID             string     `json:"id"`
	Type           string     `json:"type"`
	Status         string     `json:"status"`
	Priority       int        `json:"priority"`
	Attempts       int        `json:"attempts"`
	MaxRetries     int        `json:"max_retries"`
	RunAt          time.Time  `json:"run_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	LockedBy       *string    `json:"locked_by,omitempty"`
	LockedAt       *time.Time `json:"locked_at,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DurationMS     *int64     `json:"duration_ms,omitempty"`
	IdempotencyKey *string    `json:"-"`
}

type TaskLog struct {
	Status    string    `json:"status"`
	Message   *string   `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type WorkerLease struct {
	WorkerID string     `json:"worker_id"`
	TaskID   string     `json:"task_id"`
	TaskType string     `json:"task_type"`
	Attempts int        `json:"attempts"`
	LockedAt *time.Time `json:"locked_at,omitempty"`
}

type Throughput struct {
	EnqueuedLastMinute  int64 `json:"enqueued_last_minute"`
	CompletedLastMinute int64 `json:"completed_last_minute"`
	DeadLastMinute      int64 `json:"dead_last_minute"`
}

// Store provides read-only projections plus the one retention operation the
// playground owns. It never mutates task state; enqueue/cancel/retry go
// through dhara.Client so Dhara's semantics stay authoritative.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) ListRecent(ctx context.Context, session, status string, limit int) ([]TaskView, error) {
	if limit < 1 {
		limit = 100
	}
	if limit > 200 {
		limit = 200
	}

	query := `
		SELECT id, type, idempotency_key, status, priority, attempts, max_retries,
		       run_at, started_at, completed_at, locked_at, locked_by, last_error,
		       created_at, updated_at
		FROM tasks
		WHERE ($1 = '' OR idempotency_key LIKE 'demo:' || $1 || ':%')
		  AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC
		LIMIT $3`

	rows, err := s.pool.Query(ctx, query, session, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent tasks: %w", err)
	}
	defer rows.Close()

	out := make([]TaskView, 0, limit)
	for rows.Next() {
		var t StoredTask
		if err := rows.Scan(
			&t.ID, &t.Type, &t.IdempotencyKey, &t.Status, &t.Priority, &t.Attempts,
			&t.MaxRetries, &t.RunAt, &t.StartedAt, &t.CompletedAt, &t.LockedAt,
			&t.LockedBy, &t.LastError, &t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan recent task: %w", err)
		}
		out = append(out, toTaskView(t, time.Now()))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent tasks: %w", err)
	}
	return out, nil
}

func (s *Store) ListRunning(ctx context.Context, limit int) ([]WorkerLease, error) {
	if limit < 1 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT locked_by, id, type, attempts, locked_at
		FROM tasks
		WHERE status = 'RUNNING' AND locked_by IS NOT NULL
		ORDER BY locked_at DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list running tasks: %w", err)
	}
	defer rows.Close()

	out := make([]WorkerLease, 0, limit)
	for rows.Next() {
		var lease WorkerLease
		var id uuid.UUID
		if err := rows.Scan(&lease.WorkerID, &id, &lease.TaskType, &lease.Attempts, &lease.LockedAt); err != nil {
			return nil, fmt.Errorf("scan running task: %w", err)
		}
		lease.TaskID = id.String()
		out = append(out, lease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate running tasks: %w", err)
	}
	return out, nil
}

func (s *Store) GetStoredTask(ctx context.Context, id string) (*StoredTask, error) {
	var t StoredTask
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, idempotency_key, status, priority, attempts, max_retries,
		       run_at, started_at, completed_at, locked_at, locked_by, last_error,
		       created_at, updated_at
		FROM tasks WHERE id = $1`, id).Scan(
		&t.ID, &t.Type, &t.Payload, &t.IdempotencyKey, &t.Status, &t.Priority, &t.Attempts,
		&t.MaxRetries, &t.RunAt, &t.StartedAt, &t.CompletedAt, &t.LockedAt, &t.LockedBy,
		&t.LastError, &t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get stored task: %w", err)
	}
	return &t, nil
}

func (s *Store) TaskLogs(ctx context.Context, id string, limit int) ([]TaskLog, error) {
	if limit < 1 || limit > 200 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT status, message, created_at
		FROM task_logs
		WHERE task_id = $1
		ORDER BY created_at ASC, id ASC
		LIMIT $2`, id, limit)
	if err != nil {
		return nil, fmt.Errorf("list task logs: %w", err)
	}
	defer rows.Close()

	out := make([]TaskLog, 0, limit)
	for rows.Next() {
		var l TaskLog
		if err := rows.Scan(&l.Status, &l.Message, &l.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan task log: %w", err)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task logs: %w", err)
	}
	return out, nil
}

func (s *Store) Throughput(ctx context.Context) (*Throughput, error) {
	var t Throughput
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE created_at > now() - interval '60 seconds'),
			COUNT(*) FILTER (WHERE completed_at > now() - interval '60 seconds'),
			COUNT(*) FILTER (WHERE status = 'DEAD' AND updated_at > now() - interval '60 seconds')
		FROM tasks`).Scan(&t.EnqueuedLastMinute, &t.CompletedLastMinute, &t.DeadLastMinute)
	if err != nil {
		return nil, fmt.Errorf("throughput query: %w", err)
	}
	return &t, nil
}

// DeleteOlderThan removes rows created before cutoff from this store's
// dedicated schema. RUNNING tasks are deliberately excluded. Dependent log
// and dead-letter rows are removed first because the schema's FKs do not
// cascade. The store's pool has search_path pinned to the demo schema.
func (s *Store) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin cleanup tx: %w", err)
	}
	defer tx.Rollback(ctx)

	oldTasks := `SELECT id FROM tasks WHERE created_at < $1 AND status <> 'RUNNING'`
	if _, err := tx.Exec(ctx, `DELETE FROM dead_letters WHERE task_id IN (`+oldTasks+`)`, cutoff); err != nil {
		return 0, fmt.Errorf("delete old dead letters: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM task_logs WHERE task_id IN (`+oldTasks+`)`, cutoff); err != nil {
		return 0, fmt.Errorf("delete old task logs: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM tasks WHERE created_at < $1 AND status <> 'RUNNING'`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete old tasks: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit cleanup: %w", err)
	}
	return tag.RowsAffected(), nil
}

func toTaskView(t StoredTask, now time.Time) TaskView {
	v := TaskView{
		ID:             t.ID.String(),
		Type:           t.Type,
		Status:         t.Status,
		Priority:       t.Priority,
		Attempts:       t.Attempts,
		MaxRetries:     t.MaxRetries,
		RunAt:          t.RunAt,
		StartedAt:      t.StartedAt,
		CompletedAt:    t.CompletedAt,
		LockedBy:       t.LockedBy,
		LockedAt:       t.LockedAt,
		LastError:      t.LastError,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
		IdempotencyKey: t.IdempotencyKey,
	}
	if t.StartedAt != nil {
		switch t.Status {
		case "COMPLETED", "DEAD", "CANCELED":
			if t.CompletedAt != nil {
				ms := t.CompletedAt.Sub(*t.StartedAt).Milliseconds()
				v.DurationMS = &ms
			}
		case "RUNNING":
			ms := now.Sub(*t.StartedAt).Milliseconds()
			v.DurationMS = &ms
		}
	}
	return v
}
