package playground

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestCrashManagerDisabledDoesNotTouchDatabase(t *testing.T) {
	cfg := testConfig()
	cfg.EnableCrashDemo = false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	manager, err := NewCrashManager(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("NewCrashManager: %v", err)
	}
	defer manager.Close()

	if manager.Enabled() {
		t.Fatal("manager reports enabled")
	}
	if status := manager.Status(context.Background()); status == nil || status.Enabled {
		t.Fatalf("status = %+v, want disabled", status)
	}
	if _, err := manager.Start(context.Background(), "session"); !errors.Is(err, ErrCrashDisabled) {
		t.Fatalf("Start error = %v, want ErrCrashDisabled", err)
	}
}

func TestChildEnvOverridesWithoutDuplicates(t *testing.T) {
	t.Setenv("PLAYGROUND_DB_SCHEMA", "parent_schema")
	t.Setenv("PLAYGROUND_CRASH_SCHEMA", "parent_crash")

	env := childEnv(map[string]string{
		"PLAYGROUND_DB_SCHEMA":    "child_schema",
		"PLAYGROUND_CRASH_SCHEMA": "child_crash",
	})

	values := map[string][]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		values[key] = append(values[key], value)
	}
	for key, want := range map[string]string{
		"PLAYGROUND_DB_SCHEMA":    "child_schema",
		"PLAYGROUND_CRASH_SCHEMA": "child_crash",
	} {
		got := values[key]
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want exactly [%s]", key, got, want)
		}
	}
}
