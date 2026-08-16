// Package dharatype holds the shared data types used across the dhara
// library, the core engine, and the API server. It has no dependencies on
// internal packages, so it can be imported freely by library users.
package dharatype

import (
	"context"
	"encoding/json"
)

// HandlerFunc is the signature of a task handler function.
// It receives a context and the raw JSON payload of the task, and returns an error if the task failed.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

// TaskListFilter holds the optional filters and pagination for listing tasks.
type TaskListFilter struct {
	Status   string
	Type     string
	Retrying *bool
	Limit    int
	Offset   int
}
