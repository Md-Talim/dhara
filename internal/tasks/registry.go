package tasks

import (
	"context"
	"encoding/json"
)

type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

type HandlerRegistry interface {
	Get(taskType string) (HandlerFunc, bool)
}

type MapRegistry struct {
	handlers map[string]HandlerFunc
}

func NewRegistry(handlers map[string]HandlerFunc) *MapRegistry {
	if handlers == nil {
		handlers = make(map[string]HandlerFunc)
	}
	return &MapRegistry{handlers: handlers}
}

func (r *MapRegistry) Get(taskType string) (HandlerFunc, bool) {
	handler, ok := r.handlers[taskType]
	return handler, ok
}
