package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/testutil"
)

// A handler that panics on every attempt must not take the worker down. Each
// poison task should consume its retries and land in DEAD, and the same worker
// must still be able to complete healthy work afterwards.
func TestPoisonPill_AllTasksDeadAndWorkerSurvives(t *testing.T) {
	testutil.SkipIfNoTestDB(t)

	ctx := context.Background()

	pool, err := db.Open(ctx, testutil.TestDBURL(), 10, 2)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := dhara.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	testutil.MustTruncate(ctx, t, pool)
	testutil.CleanupTruncate(ctx, t, pool)

	client := dhara.NewClient(pool)

	const (
		numPoison  = 5
		maxRetries = 2 // two attempts per task before DEAD
	)

	poisonIDs := make([]string, 0, numPoison)
	for i := range numPoison {
		res, err := client.Enqueue(ctx, "poison", map[string]int{"i": i},
			dhara.WithMaxRetries(maxRetries),
		)
		if err != nil {
			t.Fatalf("enqueue poison %d: %v", i, err)
		}
		poisonIDs = append(poisonIDs, res.Task.ID.String())
	}

	worker := dhara.NewWorker(pool,
		dhara.WithMaxWorkers(2),
		dhara.WithPollInterval(5*time.Millisecond),
		dhara.WithHeartbeatInterval(time.Second),
		dhara.WithHandlerTimeout(2*time.Second),
		dhara.WithBaseBackoff(time.Millisecond),
		dhara.WithMaxBackoff(2*time.Millisecond),
		// Keep the reaper out of this test: every task should be resolved by
		// the worker's own failure path, not by stale-heartbeat recovery.
		dhara.WithReaperInterval(time.Hour),
		dhara.WithStuckThreshold(time.Hour),
	)

	// Panics on every attempt. Without the recover guard this aborts the
	// process on the first poison task and the test binary dies with it.
	worker.RegisterHandler("poison", func(ctx context.Context, payload json.RawMessage) error {
		panic("poison pill: always panics")
	})
	worker.RegisterHandler("healthy", func(ctx context.Context, payload json.RawMessage) error {
		return nil
	})

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Start(runCtx) }()

	// Every poison task must exhaust its retries and reach DEAD.
	testutil.Eventually(t, 10*time.Second, 20*time.Millisecond, func() bool {
		var dead int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM tasks WHERE type = 'poison' AND status = 'DEAD'`,
		).Scan(&dead); err != nil {
			t.Logf("count dead: %v", err)
			return false
		}
		return dead == numPoison
	})

	// Nothing may be left mid-flight.
	var unfinished int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks
		 WHERE type = 'poison' AND status IN ('PENDING', 'RUNNING')`,
	).Scan(&unfinished); err != nil {
		t.Fatalf("count unfinished: %v", err)
	}
	if unfinished != 0 {
		t.Errorf("unfinished poison tasks = %d, want 0", unfinished)
	}

	// Each exhausted task must have produced a dead letter.
	var deadLetters int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM dead_letters dl
		 JOIN tasks t ON t.id = dl.task_id
		 WHERE t.type = 'poison'`,
	).Scan(&deadLetters); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if deadLetters != numPoison {
		t.Errorf("dead letters = %d, want %d", deadLetters, numPoison)
	}

	// Positive proof the worker survived: it must still complete new work
	// after the whole poison batch died.
	healthy, err := client.Enqueue(ctx, "healthy", map[string]string{"msg": "still alive"})
	if err != nil {
		t.Fatalf("enqueue healthy: %v", err)
	}
	testutil.Eventually(t, 5*time.Second, 20*time.Millisecond, func() bool {
		var status string
		if err := pool.QueryRow(ctx,
			`SELECT status FROM tasks WHERE id = $1`, healthy.Task.ID,
		).Scan(&status); err != nil {
			t.Logf("get healthy task: %v", err)
			return false
		}
		return status == "COMPLETED"
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker.Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}
}
