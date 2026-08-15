package dhara

import (
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara/internal/store"
)

type Task struct {
	ID             uuid.UUID
	Type           string
	IdempotencyKey *string
	Status         TaskStatus
	Priority       int
	Attempts       int
	MaxRetries     int
	RunAt          time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusCanceled  TaskStatus = "CANCELED"
	TaskStatusDead      TaskStatus = "DEAD"
)

func newTask(t *store.Task) *Task {
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
		switch t.Status {
		case "DEAD", "RUNNING":
			task.LastError = t.LastError
		case "PENDING":
			if t.Attempts > 0 {
				task.LastError = t.LastError
			}
		}
	}

	return task
}
