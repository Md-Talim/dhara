package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/md-talim/dhara/internal/ctxlog"
)

func Echo(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)

	logger.Info("echo handler", "payload", string(payload))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return nil
	}
}

func SendEmail(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)

	var p struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	logger.Info("sending email", "to", p.To, "subject", p.Subject)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		logger.Info("email sent", "to", p.To)
		return nil
	}
}

func AlwaysFails(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)
	logger.Warn("intentional failure for testing retry behavior")
	return errors.New("intentional failure for testing retry behavior")
}

func SlowTask(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)
	logger.Info("slow task started")

	select {
	case <-ctx.Done():
		logger.Warn("slow task canceled")
		return ctx.Err()
	case <-time.After(10 * time.Minute):
		logger.Warn("slow task complete")
		return nil
	}
}

func RealisticWork(ctx context.Context, payload json.RawMessage) error {
	logger := ctxlog.From(ctx)
	logger.Info("realistic_work handler", "payload", string(payload))

	delay := 50*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}
