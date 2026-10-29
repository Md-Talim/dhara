package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/md-talim/dhara/dharatype"
	"github.com/md-talim/dhara/internal/store"
)

func TestClaim_ConcurrentWorkers_NeverDoubleClaim(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	const (
		numWorkers = 8
		numTasks   = 200
	)

	for i := range numTasks {
		task := &store.TaskRow{
			Type:       "contended_task",
			Payload:    json.RawMessage(`{"seed":true}`),
			MaxRetries: 3,
		}
		if _, err := testStore.Create(ctx, task); err != nil {
			t.Fatalf("seed task %d: %v", i, err)
		}
	}

	var (
		mu       sync.Mutex
		claims   = make(map[string]int) // task id -> times claimed
		total    int
		claimErr error
	)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			<-start

			for {
				task, err := testStore.Claim(ctx, workerID)
				if errors.Is(err, dharatype.ErrTaskNotAvailable) {
					return // queue drained
				}
				if err != nil {
					mu.Lock()
					if claimErr == nil {
						claimErr = err
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				claims[task.ID.String()]++
				total++
				mu.Unlock()
			}
		}(fmt.Sprintf("contending-worker-%d", w))
	}

	close(start)
	wg.Wait()

	if claimErr != nil {
		t.Fatalf("claim error: %v", claimErr)
	}
	if total != numTasks {
		t.Errorf("total claims = %d, want %d (every task claimed exactly once)", total, numTasks)
	}
	if len(claims) != numTasks {
		t.Errorf("distinct claimed tasks = %d, want %d", len(claims), numTasks)
	}
	for id, n := range claims {
		if n > 1 {
			t.Errorf("task %s claimed %d times: SKIP LOCKED failed", id, n)
		}
	}

	// Assert against the database, not just the workers' map: a double-claim
	// would leave attempts > 1 even if the map bookkeeping looked clean.
	var pending, running int
	if err := testDB.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE status = 'PENDING'),
				count(*) FILTER (WHERE status = 'RUNNING')
			FROM tasks
		`).Scan(&pending, &running); err != nil {
		t.Fatalf("count task states: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0 (all tasks should be claimed)", pending)
	}
	if running != numTasks {
		t.Errorf("running = %d, want %d", running, numTasks)
	}

	var overAttempted int
	if err := testDB.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE attempts <> 1`,
	).Scan(&overAttempted); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if overAttempted != 0 {
		t.Errorf("%d task(s) have attempts != 1, claimed more than once", overAttempted)
	}
}
