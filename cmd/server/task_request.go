package main

import (
	"encoding/json"
	"time"

	"github.com/md-talim/dhara"
)

type createTaskRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	RunAt          *time.Time      `json:"run_at"`
}

func (r *createTaskRequest) toInsertParams() *dhara.InsertParams {
	return &dhara.InsertParams{
		Type:           r.Type,
		Payload:        r.Payload,
		IdempotencyKey: r.IdempotencyKey,
		Priority:       r.Priority,
		MaxRetries:     r.MaxRetries,
		RunAt:          r.RunAt,
	}
}
