package api

import (
	"encoding/json"
	"time"

	"github.com/md-talim/dhara/internal/store"
)

type createTaskRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	RunAt          *time.Time      `json:"run_at"`
}

func (r *createTaskRequest) toInsertParams() *store.InsertParams {
	return &store.InsertParams{
		Type:           r.Type,
		Payload:        r.Payload,
		IdempotencyKey: r.IdempotencyKey,
		Priority:       r.Priority,
		MaxRetries:     r.MaxRetries,
		RunAt:          r.RunAt,
	}
}
