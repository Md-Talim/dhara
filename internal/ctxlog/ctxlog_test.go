package ctxlog

import (
	"context"
	"log/slog"
	"testing"
)

func TestFrom_ReturnsDefaultWhenAbsent(t *testing.T) {
	logger := From(context.Background())
	if logger == nil {
		t.Fatal("From() returned nil, want the default logger")
	}
}

func TestFrom_Roundtrip(t *testing.T) {
	custom := slog.New(slog.NewJSONHandler(&discardWriter{}, nil))
	ctx := WithLogger(context.Background(), custom)

	if got := From(ctx); got != custom {
		t.Error("From(ctx) did not return the logger stored by WithLogger")
	}
}

// discardWriter satisfies io.Writer while throwing the bytes away, so tests
// do not write log output to stdout.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
