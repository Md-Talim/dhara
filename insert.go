package dhara

import (
	"time"

	"github.com/md-talim/dhara/internal/store"
)

type InsertParams = store.InsertParams

type ListFilter = store.TaskListFilter

// EnqueueResult is returned by every insert method
type EnqueueResult struct {
	// Task is the created task, or the existing task on an idepotent replay
	Task *Task

	// Duplicate is true if an existing task with the same
	// (type, idempotency_key) was returned instead of inserting a new one
	Duplicate bool
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
