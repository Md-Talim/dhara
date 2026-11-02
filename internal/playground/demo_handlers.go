package playground

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/md-talim/dhara/internal/ctxlog"
)

// FlakyWork fails until the absolute timestamp in the payload has passed. The
// absolute timestamp makes the handler deterministic across retries: the task
// starts failing, then succeeds on a later attempt without any hidden state.
func FlakyWork(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)

	var p struct {
		SucceedAt int64 `json:"succeed_at"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid flaky payload: %w", err)
	}
	if p.SucceedAt <= 0 {
		return errors.New("invalid flaky payload: succeed_at is required")
	}

	now := time.Now().UnixMilli()
	if now < p.SucceedAt {
		return fmt.Errorf("simulated transient failure: retrying in %dms", p.SucceedAt-now)
	}

	logger.Info("flaky task recovered", "succeed_at", p.SucceedAt)
	return nil
}

// CrashWork runs long enough to be visibly claimed and heartbeating. It is
// registered only in the isolated crash-demo worker.
func CrashWork(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)

	var p struct {
		DurationMS int64 `json:"duration_ms"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid crash-demo payload: %w", err)
	}
	if p.DurationMS < 0 || p.DurationMS > 60_000 {
		return errors.New("invalid crash-demo payload: duration_ms must be between 0 and 60000")
	}

	logger.Info("crash-demo work started", "duration_ms", p.DurationMS)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(p.DurationMS) * time.Millisecond):
		logger.Info("crash-demo work completed")
		return nil
	}
}
