package store

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	DefaultPriority   = 0
	DefaultMaxRetries = 5
)

// InsertParams is the shared input for creating a task. Pointer fields
// distinguish "not provided" (default applied) from explicit zero value
// (e.g. MaxRetries: 0 means "never retry", while nil means the default 5)
type InsertParams struct {
	Type           string
	Payload        json.RawMessage
	IdempotencyKey *string
	Priority       *int
	MaxRetries     *int
	RunAt          *time.Time
}

// Normalize fills in defaults for unset fields: priority 0, max_retries 5,
// run_at now, payload {}
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

func (p *InsertParams) ToTask() *Task {
	return &Task{
		Type:           p.Type,
		Payload:        p.Payload,
		IdempotencyKey: p.IdempotencyKey,
		Priority:       *p.Priority,
		MaxRetries:     *p.MaxRetries,
		RunAt:          *p.RunAt,
	}
}
