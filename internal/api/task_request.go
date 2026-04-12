package api

import (
	"encoding/json"
	"errors"
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

func (r *createTaskRequest) toTask() *store.Task {
	return &store.Task{
		Type:           r.Type,
		Payload:        r.Payload,
		IdempotencyKey: r.IdempotencyKey,
		Priority:       *r.Priority,
		MaxRetries:     *r.MaxRetries,
		RunAt:          *r.RunAt,
	}
}

func (r *createTaskRequest) normalize(now time.Time) {
	if r.Payload == nil {
		r.Payload = json.RawMessage(`{}`)
	}
	if r.Priority == nil {
		defaultPriority := 0
		r.Priority = &defaultPriority
	}
	if r.MaxRetries == nil {
		defaultRetries := 5
		r.MaxRetries = &defaultRetries
	}
	if r.RunAt == nil {
		r.RunAt = &now
	}
}

func (r *createTaskRequest) validate(now time.Time) error {
	if r.Type == "" {
		return errors.New("type is required")
	}
	if len(r.Type) > 100 {
		return errors.New("type must be 100 characters or fewer")
	}
	if *r.Priority < 0 || *r.Priority > 100 {
		return errors.New("priority must be between 0 and 100")
	}
	if *r.MaxRetries < 0 || *r.MaxRetries > 20 {
		return errors.New("max_retries must be between 0 and 20")
	}
	if r.IdempotencyKey != nil && len(*r.IdempotencyKey) > 255 {
		return errors.New("idempotency_key must be 255 characters or fewer")
	}
	if len(r.Payload) > 64*1024 {
		return errors.New("payload must not exceed 64KB")
	}
	if r.RunAt.Before(now.Add(-5 * time.Minute)) {
		return errors.New("run_at cannot be in the past")
	}
	if r.RunAt.After(now.Add(30 * 24 * time.Hour)) {
		return errors.New("run_at cannot be more than 30 days in the future")
	}

	return nil
}
