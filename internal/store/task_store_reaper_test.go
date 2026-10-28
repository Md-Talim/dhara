package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/testutil"
)

// seedClaimedTask creates a task, claims it, and backdates locked_at so the
// reaper sees it as stale. Returns the claimed row (attempts already bumped).
func seedClaimedTask(t *testing.T, ctx context.Context, taskType string, maxRetries int, staleBy time.Duration) *store.TaskRow {
	t.Helper()

	task := &store.TaskRow{
		Type:       taskType,
		Payload:    json.RawMessage(`{"seed":true}`),
		MaxRetries: maxRetries,
	}
	if _, err := testStore.Create(ctx, task); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	claimed, err := testStore.Claim(ctx, "worker-before-crash")
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, task.ID)
	}

	// Simulate the worker dying: it claimed the task, then stopped heartbeating.
	if _, err := testDB.Exec(ctx,
		`UPDATE tasks SET locked_at = now() - ($1 * interval '1 millisecond') WHERE id = $2`,
		staleBy.Milliseconds(), task.ID,
	); err != nil {
		t.Fatalf("backdate locked_at: %v", err)
	}

	return claimed
}

// A worker that claimed a task and died must have that task requeued, with
// the attempt it already consumed preserved.
func TestRequeueStaleRunning_RequeuesStaleTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	claimed := seedClaimedTask(t, ctx, "stale_task", 5, 10*time.Minute)
	if claimed.Attempts != 1 {
		t.Fatalf("precondition: attempts = %d, want 1 after claim", claimed.Attempts)
	}

	n, err := testStore.RequeueStaleRunning(ctx, 5*time.Minute, "reaper-test")
	if err != nil {
		t.Fatalf("RequeueStaleRunning: %v", err)
	}
	if n != 1 {
		t.Errorf("changed = %d, want 1", n)
	}

	got, err := testStore.GetById(ctx, claimed.ID.String())
	if err != nil {
		t.Fatalf("GetById: %v", err)
	}
	if got == nil {
		t.Fatal("task disappeared after reaping")
	}
	if got.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING", got.Status)
	}
	// The attempt was consumed at claim time; reaping must not reset or
	// double-count it, or max_retries no longer bounds the poison task.
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (preserved from the claim)", got.Attempts)
	}
	if got.LockedBy != nil || got.LockedAt != nil {
		t.Errorf("locked_by/locked_at = %v/%v, want both nil", got.LockedBy, got.LockedAt)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "reaped") {
		t.Errorf("last_error = %v, want it to mention reaping", got.LastError)
	}
	// run_at must be reset so the task is claimable immediately.
	if got.RunAt.After(time.Now().Add(time.Second)) {
		t.Errorf("run_at = %v, want now-ish (immediately claimable)", got.RunAt)
	}

	var logCount int
	if err := testDB.QueryRow(ctx,
		`SELECT count(*) FROM task_logs WHERE task_id = $1 AND status = 'PENDING'`,
		claimed.ID,
	).Scan(&logCount); err != nil {
		t.Fatalf("count task_logs: %v", err)
	}
	if logCount != 1 {
		t.Errorf("task_logs rows = %d, want 1 reaper log entry", logCount)
	}
}

// A stale task that already used its last attempt must go DEAD, not loop
// forever. This is the crash-looping poison pill: it consumes an attempt,
// kills its worker, gets reaped, and must eventually stop.
func TestRequeueStaleRunning_ExhaustedRetries_MarksDead(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	// max_retries = 1, so the single claim already exhausts the budget.
	claimed := seedClaimedTask(t, ctx, "doomed_stale_task", 1, 10*time.Minute)
	if claimed.Attempts != 1 {
		t.Fatalf("precondition: attempts = %d, want 1", claimed.Attempts)
	}

	n, err := testStore.RequeueStaleRunning(ctx, 5*time.Minute, "reaper-test")
	if err != nil {
		t.Fatalf("RequeueStaleRunning: %v", err)
	}
	if n != 1 {
		t.Errorf("changed = %d, want 1", n)
	}

	got, err := testStore.GetById(ctx, claimed.ID.String())
	if err != nil {
		t.Fatalf("GetById: %v", err)
	}
	if got.Status != "DEAD" {
		t.Fatalf("status = %q, want DEAD (retries exhausted)", got.Status)
	}

	// DEAD must be accompanied by a dead-letter row, or the API's retry
	// endpoint and any external alerting lose the task.
	var deadLetters int
	if err := testDB.QueryRow(ctx,
		`SELECT count(*) FROM dead_letters WHERE task_id = $1`, claimed.ID,
	).Scan(&deadLetters); err != nil {
		t.Fatalf("count dead_letters: %v", err)
	}
	if deadLetters != 1 {
		t.Errorf("dead_letters rows = %d, want 1", deadLetters)
	}
}

// The negative control: a task whose worker is alive and heartbeating must
// never be reaped, no matter how often the reaper runs. Without this, a
// reaper bug that requeues everything would still pass the two tests above.
func TestRequeueStaleRunning_NeverReapsHealthyTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	const workerID = "worker-alive"
	task := &store.TaskRow{
		Type:       "healthy_task",
		Payload:    json.RawMessage(`{"seed":true}`),
		MaxRetries: 3,
	}
	if _, err := testStore.Create(ctx, task); err != nil {
		t.Fatalf("create: %v", err)
	}
	claimed, err := testStore.Claim(ctx, workerID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Heartbeat well inside the stale window, like a real running worker.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = testStore.Heartbeat(ctx, claimed.ID.String(), workerID)
			}
		}
	}()

	// Run the reaper repeatedly for far longer than the stale threshold.
	// Never asserts the task stays RUNNING the whole time.
	testutil.Never(t, 400*time.Millisecond, 25*time.Millisecond, func() bool {
		if _, err := testStore.RequeueStaleRunning(ctx, 100*time.Millisecond, "reaper-test"); err != nil {
			t.Errorf("RequeueStaleRunning: %v", err)
			return false
		}
		got, err := testStore.GetById(ctx, claimed.ID.String())
		if err != nil {
			t.Errorf("GetById: %v", err)
			return false
		}
		return got.Status != "RUNNING"
	})
}
