package dhara

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara/dharatype"
	"github.com/md-talim/dhara/internal/store"
)

type TaskListFilter = dharatype.TaskListFilter

type HandlerFunc = dharatype.HandlerFunc

// Task is the public representation of a task. It intentionally omits the
// payload: the API never returns it, and callers that inserted the task
// already know what they put in.
type Task struct {
	ID             uuid.UUID  `json:"id"`
	Type           string     `json:"type"`
	Status         TaskStatus `json:"status"`
	Priority       int        `json:"priority"`
	Attempts       int        `json:"attempts"`
	MaxRetries     int        `json:"max_retries"`
	RunAt          time.Time  `json:"run_at"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// TaskStatus is the lifecycle state of a task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusCanceled  TaskStatus = "CANCELED"
	TaskStatusDead      TaskStatus = "DEAD"
)

// newTask converts an internal store row into the public representation.
func newTask(t *store.TaskRow) *Task {
	task := &Task{
		ID:             t.ID,
		Type:           t.Type,
		IdempotencyKey: t.IdempotencyKey,
		Status:         TaskStatus(t.Status),
		Priority:       t.Priority,
		Attempts:       t.Attempts,
		MaxRetries:     t.MaxRetries,
		RunAt:          t.RunAt,
		StartedAt:      t.StartedAt,
		CompletedAt:    t.CompletedAt,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}

	if t.LastError != nil {
		switch TaskStatus(t.Status) {
		case TaskStatusDead, TaskStatusRunning:
			task.LastError = t.LastError
		case TaskStatusPending:
			if t.Attempts > 0 {
				task.LastError = t.LastError
			}
		}
	}

	return task
}

// EnqueueResult is returned by every insert method.
type EnqueueResult struct {
	// Task is the created task, or the existing task on an idempotent replay.
	Task *Task

	// Duplicate is true if an existing task with the same
	// (type, idempotency_key) was returned instead of inserting a new one.
	Duplicate bool
}

// TaskListResult is returned by list operations.
type TaskListResult struct {
	Tasks []Task
	Total int
}

// InsertParams is the shared input for creating a task. Pointer fields
// distinguish "not provided" (default applied) from an explicit zero value
// (e.g. MaxRetries: 0 means "never retry", while nil means the default 5).
type InsertParams struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey *string
	Priority       *int
	MaxRetries     *int
	RunAt          *time.Time
}

// Defaults applied by Normalize when the caller leaves a field unset.
const (
	DefaultPriority   = 0
	DefaultMaxRetries = 5
)

// Normalize fills in defaults for unset fields: priority 0, max_retries 5,
// run_at now, payload {}.
func (p *InsertParams) Normalize(now time.Time) {
	if p.Payload == nil {
		p.Payload = json.RawMessage(`{}`)
	}
	if p.Priority == nil {
		def := DefaultPriority
		p.Priority = &def
	}
	if p.MaxRetries == nil {
		def := DefaultMaxRetries
		p.MaxRetries = &def
	}
	if p.RunAt == nil {
		p.RunAt = &now
	}
}

// Validate checks the (already normalized) params.
func (p *InsertParams) Validate(now time.Time) error {
	if p.Type == "" {
		return errors.New("type is required")
	}
	if len(p.Type) > 100 {
		return errors.New("type must be 100 characters or fewer")
	}
	if *p.Priority < 0 || *p.Priority > 100 {
		return errors.New("priority must be between 0 and 100")
	}
	if *p.MaxRetries < 0 || *p.MaxRetries > 20 {
		return errors.New("max_retries must be between 0 and 20")
	}
	if p.IdempotencyKey != nil && len(*p.IdempotencyKey) > 255 {
		return errors.New("idempotency_key must be 255 characters or fewer")
	}
	if len(p.Payload) > 64*1024 {
		return errors.New("payload must not exceed 64KB")
	}
	if p.RunAt.Before(now.Add(-5 * time.Minute)) {
		return errors.New("run_at cannot be in the past")
	}
	if p.RunAt.After(now.Add(30 * 24 * time.Hour)) {
		return errors.New("run_at cannot be more than 30 days in the future")
	}

	return nil
}

// toStoreTask converts validated insert params into the internal store row.
// The canonical params live in dharatypes, which must not depend on the
// engine's internal row type, so the (small) mapping happens here.
func (p *InsertParams) toStoreTask() *store.TaskRow {
	return &store.TaskRow{
		Type:           p.Type,
		Payload:        p.Payload,
		IdempotencyKey: p.IdempotencyKey,
		Priority:       *p.Priority,
		MaxRetries:     *p.MaxRetries,
		RunAt:          *p.RunAt,
	}
}

type EnqueueOption func(*InsertParams)

func WithIdempotencyKey(key string) EnqueueOption {
	return func(p *InsertParams) { p.IdempotencyKey = &key }
}

func WithPriority(priority int) EnqueueOption {
	return func(p *InsertParams) { p.Priority = &priority }
}

func WithMaxRetries(maxRetries int) EnqueueOption {
	return func(p *InsertParams) { p.MaxRetries = &maxRetries }
}

func WithRunAt(runAt time.Time) EnqueueOption {
	return func(p *InsertParams) { p.RunAt = &runAt }
}

// QueueMetrics is a point-in-time snapshot of task counts by state.
type QueueMetrics struct {
	TasksPending        int64
	TasksRunning        int64
	TasksCompleted      int64
	TasksCanceled       int64
	TasksDead           int64
	TasksPendingReady   int64
	TasksPendingDelayed int64
}
