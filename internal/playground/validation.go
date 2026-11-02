package playground

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Task types the playground accepts through its public API. Adding to this
// list is the only way for a visitor to influence execution, and every type
// maps to an already-registered handler.
const (
	TypeEcho          = "echo"
	TypeSendEmail     = "send_email"
	TypeAlwaysFail    = "always_fail"
	TypeSlowTask      = "slow_task"
	TypeRealisticWork = "realistic_work"
	TypeFlaky         = "playground_flaky"
	TypeCrash         = "playground_crash"
)

var allowedTaskTypes = map[string]struct{}{
	TypeEcho:          {},
	TypeSendEmail:     {},
	TypeAlwaysFail:    {},
	TypeSlowTask:      {},
	TypeRealisticWork: {},
	TypeFlaky:         {},
}

func isAllowedType(taskType string) bool {
	_, ok := allowedTaskTypes[taskType]
	return ok
}

// normalizePayload checks size, JSON validity, and handler-specific fields.
func normalizePayload(taskType string, raw json.RawMessage, now time.Time, maxBytes int) (json.RawMessage, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if len(raw) > maxBytes {
		return nil, fmt.Errorf("payload must not exceed %d bytes", maxBytes)
	}
	if !json.Valid(raw) {
		return nil, errors.New("payload must be valid JSON")
	}

	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("payload must be a JSON object")
	}

	switch taskType {
	case TypeEcho:
		var p struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("invalid echo payload: %w", err)
		}
		if len(p.Message) > 1000 {
			return nil, errors.New("echo message must be 1000 characters or fewer")
		}
	case TypeSendEmail:
		var p struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("invalid send_email payload: %w", err)
		}
		if len(p.To) > 320 || len(p.Subject) > 200 {
			return nil, errors.New("send_email to/subject are too long")
		}
	case TypeFlaky:
		var p struct {
			SucceedAt int64 `json:"succeed_at"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("invalid playground_flaky payload: %w", err)
		}
		if p.SucceedAt <= 0 {
			return nil, errors.New("playground_flaky payload requires a positive succeed_at")
		}
		succeedAt := time.UnixMilli(p.SucceedAt)
		if succeedAt.Before(now.Add(-time.Hour)) || succeedAt.After(now.Add(time.Hour)) {
			return nil, errors.New("playground_flaky succeed_at must be within one hour of now")
		}
	case TypeAlwaysFail, TypeSlowTask, TypeRealisticWork:
		// Any bounded JSON object is accepted; handlers ignore or log it.
	default:
		return nil, fmt.Errorf("task type %q is not allowed", taskType)
	}
	return raw, nil
}

func validateCount(n, def, max int) (int, error) {
	if n == 0 {
		return def, nil
	}
	if n < 1 || n > max {
		return 0, fmt.Errorf("count must be between 1 and %d", max)
	}
	return n, nil
}
