package tasks

import "github.com/md-talim/dhara/dharatype"

type HandlerFunc = dharatype.HandlerFunc

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

// Add registers (or overwrites) a handler for a task type.
func (r *MapRegistry) Add(taskType string, handler HandlerFunc) {
	r.handlers[taskType] = handler
}

func (r *MapRegistry) Get(taskType string) (HandlerFunc, bool) {
	handler, ok := r.handlers[taskType]
	return handler, ok
}
