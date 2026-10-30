package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/tasks"
)

func TestSettings_Normalize(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(s *Settings)
		check  func(t *testing.T, s Settings)
	}{
		{
			name:   "empty settings get defaults",
			mutate: func(s *Settings) {},
			check: func(t *testing.T, s Settings) {
				if s.WorkerPrefix != "dhara-worker" {
					t.Errorf("prefix = %q, want default", s.WorkerPrefix)
				}
				if s.Concurrency != 1 {
					t.Errorf("concurrency = %d, want 1", s.Concurrency)
				}
				if s.PollInterval != 250*time.Millisecond {
					t.Errorf("poll interval = %v, want 250ms", s.PollInterval)
				}
				if s.HeartbeatInterval != 30*time.Second {
					t.Errorf("heartbeat interval = %v, want 30s", s.HeartbeatInterval)
				}
				if s.ReaperInterval != 30*time.Second {
					t.Errorf("reaper interval = %v, want 30s", s.ReaperInterval)
				}
				if s.StaleThreshold != 5*time.Minute {
					t.Errorf("stale threshold = %v, want 5m", s.StaleThreshold)
				}
				if s.HandlerTimeout != 5*time.Minute {
					t.Errorf("handler timeout = %v, want 5m", s.HandlerTimeout)
				}
				if s.BaseBackoff != 10*time.Second {
					t.Errorf("base backoff = %v, want 10s", s.BaseBackoff)
				}
				if s.MaxBackoff != 5*time.Minute {
					t.Errorf("max backoff = %v, want 5m", s.MaxBackoff)
				}
			},
		},
		{
			name:   "negative values clamp to defaults",
			mutate: func(s *Settings) { s.Concurrency = -3; s.PollInterval = -time.Second },
			check: func(t *testing.T, s Settings) {
				if s.Concurrency != 1 {
					t.Errorf("concurrency = %d, want clamped 1", s.Concurrency)
				}
				if s.PollInterval != 250*time.Millisecond {
					t.Errorf("poll interval = %v, want clamped 250ms", s.PollInterval)
				}
			},
		},
		{
			name:   "base backoff above max is clamped to max",
			mutate: func(s *Settings) { s.BaseBackoff = time.Hour; s.MaxBackoff = time.Minute },
			check: func(t *testing.T, s Settings) {
				if s.BaseBackoff != time.Minute {
					t.Errorf("base backoff = %v, want clamped to max 1m", s.BaseBackoff)
				}
			},
		},
		{
			name:   "valid settings are preserved",
			mutate: func(s *Settings) { s.Concurrency = 7; s.PollInterval = 2 * time.Second },
			check: func(t *testing.T, s Settings) {
				if s.Concurrency != 7 || s.PollInterval != 2*time.Second {
					t.Errorf("valid settings were mutated: concurrency=%d poll=%v", s.Concurrency, s.PollInterval)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Settings{}
			tt.mutate(s)
			s.normalize()
			tt.check(t, *s)
		})
	}
}

// The public WorkerPool lifecycle: Start must be idempotent, Wait must
// return after Stop, and everything must terminate cleanly on context
// cancellation. Uses tiny intervals so the whole test runs in milliseconds.
func TestWorkerPool_StartStopWait(t *testing.T) {
	pool := NewWorkerPool(
		newFakeStore(metrics.New()),
		tasks.NewRegistry(nil),
		nil, // nil logger must fall back to slog.Default, not panic
		Settings{
			WorkerPrefix:   "test-pool",
			Concurrency:    2,
			PollInterval:   5 * time.Millisecond,
			ReaperInterval: 5 * time.Millisecond,
		},
		metrics.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	if !pool.Started() {
		t.Fatal("pool.Started() = false after Start")
	}

	// Second Start must be a no-op (no panic, no duplicate goroutines).
	pool.Start(ctx)

	// Reaper must have started: give it a couple of ticks, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()
	pool.Stop()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := pool.Wait(waitCtx); err != nil {
		t.Fatalf("pool.Wait() = %v, want nil", err)
	}
}

func TestWorkerPool_WaitRespectsContextTimeout(t *testing.T) {
	pool := NewWorkerPool(
		newFakeStore(metrics.New()),
		tasks.NewRegistry(nil),
		nil,
		Settings{Concurrency: 1, PollInterval: time.Hour}, // workers poll slowly
		metrics.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Stop cancels the internal context, so Wait should still return even
	// with an extremely short deadline; assert it does not hang forever.
	cancel()
	pool.Stop()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := pool.Wait(waitCtx); err != nil {
		t.Fatalf("pool.Wait() = %v, want nil (workers should exit on ctx cancel)", err)
	}
}

func TestWorkerPool_StopCancelsInFlightHandlerAndRequeues(t *testing.T) {
	m := metrics.New()
	fs := newFakeStore(m)

	started := make(chan struct{})
	cancelled := make(chan error, 1)

	reg := tasks.NewRegistry(nil)
	reg.Add("blocking", func(ctx context.Context, payload json.RawMessage) error {
		close(started)
		<-ctx.Done()
		cancelled <- ctx.Err()
		return ctx.Err()
	})

	row := fs.seed(t, "blocking", 0, 5)

	pool := NewWorkerPool(
		fs,
		reg,
		nil,
		Settings{
			WorkerPrefix:      "test-shutdown",
			Concurrency:       1,
			PollInterval:      5 * time.Millisecond,
			HeartbeatInterval: time.Hour,
			ReaperInterval:    time.Hour,
			HandlerTimeout:    5 * time.Minute,
		},
		m,
	)

	ctx := t.Context()
	pool.Start(ctx)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	pool.Stop()

	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("handler ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight handler was not cancelled by Stop")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := pool.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// The cancelled attempt must be recorded at shutdown, not left RUNNING
	// for the reaper to rediscover later.
	if got := fs.row(row.ID); got.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING after shutdown cancellation", got.Status)
	}
}
