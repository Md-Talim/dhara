package api

import (
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara/internal/store"
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

func newTaskResponse(t *store.Task) taskResponse {
	r := taskResponse{
		ID:          t.ID,
		Type:        t.Type,
		Status:      t.Status,
		Attempts:    t.Attempts,
		MaxRetries:  t.MaxRetries,
		RunAt:       t.RunAt,
		StartedAt:   t.StartedAt,
		CompletedAt: t.CompletedAt,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
	}

	if t.LastError != nil {
		switch t.Status {
		case "DEAD", "RUNNING":
			r.LastError = t.LastError
		case "PENDING":
			if t.Attempts > 0 {
				r.LastError = t.LastError
			}
		}
	}

	return r
}
