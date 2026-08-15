package api

import (
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
)

type taskResponse struct {
	ID             uuid.UUID  `json:"id"`
	Type           string     `json:"type"`
	Status         string     `json:"status"`
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

func newTaskResponse(t *dhara.Task) taskResponse {
	return taskResponse{
		ID:             t.ID,
		Type:           t.Type,
		Status:         string(t.Status),
		Priority:       t.Priority,
		Attempts:       t.Attempts,
		MaxRetries:     t.MaxRetries,
		RunAt:          t.RunAt,
		IdempotencyKey: t.IdempotencyKey,
		StartedAt:      t.StartedAt,
		CompletedAt:    t.CompletedAt,
		LastError:      t.LastError,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}
