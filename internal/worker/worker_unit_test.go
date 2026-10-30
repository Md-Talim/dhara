package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/md-talim/dhara/dharatype"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/dhara/internal/tasks"
)

/**
 * fakeStore: a store.TaskStore that records calls and returns canned rows.
 *
 * This is why TaskStore is an interface: the worker loop (retry/dead/
 * ownership-lost policy) can be tested exhaustively without a database,
 * in microseconds, with zero flakiness. The Mark* methods bump the same
 * counters the real PostgresTaskStore bumps, so metric assertions hold.
 */

type fakeStore struct {
	mu sync.Mutex

	metrics    *metrics.Metrics          // shared with the worker, like production
	rows       map[string]*store.TaskRow // id -> row, mutated by Mark* calls
	claimQueue []*store.TaskRow          // popped by Claim
	claimed    []string                  // ids successfully claimed, in order

	calls []string // "Method" log of every store call, in order
}

func newFakeStore(m *metrics.Metrics) *fakeStore {
	return &fakeStore{rows: map[string]*store.TaskRow{}, metrics: m}
}

func (f *fakeStore) log(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, method)
}

func (f *fakeStore) row(id uuid.UUID) *store.TaskRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.rows[id.String()]; ok {
		cp := *r
		return &cp
	}
	return nil
}

func (f *fakeStore) seed(t *testing.T, typ string, attempts, maxRetries int) *store.TaskRow {
	t.Helper()
	row := &store.TaskRow{
		ID:         uuid.New(),
		Type:       typ,
		Payload:    json.RawMessage(`{}`),
		Status:     "PENDING",
		Attempts:   attempts,
		MaxRetries: maxRetries,
		RunAt:      time.Now(),
	}
	f.mu.Lock()
	f.rows[row.ID.String()] = row
	f.claimQueue = append(f.claimQueue, row)
	f.mu.Unlock()
	return row
}

func (f *fakeStore) Create(ctx context.Context, task *store.TaskRow) (bool, error) {
	f.log("Create")
	return true, nil
}

func (f *fakeStore) CreateTx(ctx context.Context, tx pgx.Tx, task *store.TaskRow) (bool, error) {
	f.log("CreateTx")
	return true, nil
}

func (f *fakeStore) GetById(ctx context.Context, id string) (*store.TaskRow, error) {
	f.log("GetById")
	return nil, nil
}

func (f *fakeStore) List(ctx context.Context, filter dharatype.TaskListFilter) ([]store.TaskRow, int, error) {
	f.log("List")
	return nil, 0, nil
}

func (f *fakeStore) Cancel(ctx context.Context, id string) (*store.TaskRow, error) {
	f.log("Cancel")
	return nil, nil
}

func (f *fakeStore) Claim(ctx context.Context, workerID string) (*store.TaskRow, error) {
	f.log("Claim")
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.claimQueue) == 0 {
		return nil, dharatype.ErrTaskNotAvailable
	}
	row := f.claimQueue[0]
	f.claimQueue = f.claimQueue[1:]

	row.Status = "RUNNING"
	row.Attempts++
	row.LockedBy = &workerID
	now := time.Now()
	row.LockedAt = &now
	f.claimed = append(f.claimed, row.ID.String())
	return row, nil
}

func (f *fakeStore) Heartbeat(ctx context.Context, taskID, workerID string) error {
	f.log("Heartbeat")
	return nil
}

func (f *fakeStore) MarkCompleted(ctx context.Context, taskID, workerID string, durationMS int64) error {
	f.log("MarkCompleted")
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[taskID]
	if !ok || row.LockedBy == nil || *row.LockedBy != workerID {
		return dharatype.ErrTaskOwnershipLost
	}
	row.Status = "COMPLETED"
	f.metrics.TasksCompletedTotal.Add(1)
	return nil
}

func (f *fakeStore) MarkPending(ctx context.Context, task *store.TaskRow, workerID, lastError string, runAt time.Time) error {
	f.log("MarkPending")
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[task.ID.String()]
	if !ok || row.LockedBy == nil || *row.LockedBy != workerID {
		return dharatype.ErrTaskOwnershipLost
	}
	row.Status = "PENDING"
	row.LastError = &lastError
	row.RunAt = runAt
	row.LockedBy = nil
	row.LockedAt = nil
	f.metrics.TaskAttemptFailuresTotal.Add(1)
	f.metrics.TasksRetriedTotal.Add(1)
	return nil
}

func (f *fakeStore) MarkDead(ctx context.Context, taskID, workerID, lastError, reason string) error {
	f.log("MarkDead")
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[taskID]
	if !ok || row.LockedBy == nil || *row.LockedBy != workerID {
		return dharatype.ErrTaskOwnershipLost
	}
	row.Status = "DEAD"
	row.LastError = &lastError
	f.metrics.TasksDeadTotal.Add(1)
	return nil
}

func (f *fakeStore) RetryDead(ctx context.Context, taskID string) (*store.TaskRow, error) {
	f.log("RetryDead")
	return nil, nil
}

func (f *fakeStore) RequeueStaleRunning(ctx context.Context, staleThreshold time.Duration, reaperID string) (int64, error) {
	f.log("RequeueStaleRunning")
	return 0, nil
}

// Compile-time proof the fake satisfies the real interface.
var _ store.TaskStore = (*fakeStore)(nil)

/**
 * Worker construction helpers. Worker and fakeStore share one metrics
 * instance, mirroring how production wires a single metrics object through
 * both the store and the worker pool.
 */

func buildWorker(s store.TaskStore, m *metrics.Metrics, handlers map[string]tasks.HandlerFunc) *Worker {
	reg := tasks.NewRegistry(nil)
	for typ, h := range handlers {
		reg.Add(typ, h)
	}
	return &Worker{
		workerID:          "test-worker-1",
		store:             s,
		registry:          reg,
		logger:            slog.New(slog.NewTextHandler(&discardWriter{}, nil)),
		metrics:           m,
		pollInterval:      time.Hour, // irrelevant: the fake decides what Claim returns
		heartbeatInterval: time.Hour,
		handlerTimeout:    2 * time.Second,
		baseBackoff:       50 * time.Millisecond,
		maxBackoff:        200 * time.Millisecond,
	}
}

// newTestWorker returns a Worker backed by a fresh fakeStore sharing its
// metrics. Tests grab the fake via fs := w.store.(*fakeStore).
func newTestWorker(handlers map[string]tasks.HandlerFunc) *Worker {
	m := metrics.New()
	return buildWorker(newFakeStore(m), m, handlers)
}

// discardWriter satisfies io.Writer while throwing bytes away so tests do
// not spam stdout with worker logs.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// runOneCycle drives a single claim + execute round synchronously.
func runOneCycle(t *testing.T, w *Worker) {
	t.Helper()
	task := claimOne(t, w)
	executeOne(t, w, task)
}

// claimOne performs a single claim and returns the claimed row.
func claimOne(t *testing.T, w *Worker) *store.TaskRow {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, err := w.claim(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return task
}

// executeOne runs the execute half of the loop with a watchdog so a blocking
// handler fails the test instead of hanging the whole run.
func executeOne(t *testing.T, w *Worker, task *store.TaskRow) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()
	if err := w.execute(ctx, task); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

/**
 * Handler-success path
 */

func TestExecute_HandlerSuccess_MarksCompleted(t *testing.T) {
	var ranWithPayload json.RawMessage
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"ok_task": func(ctx context.Context, payload json.RawMessage) error {
			ranWithPayload = payload
			return nil
		},
	})
	fs := w.store.(*fakeStore)
	row := fs.seed(t, "ok_task", 0, 5)

	runOneCycle(t, w)

	if got := fs.row(row.ID); got.Status != "COMPLETED" {
		t.Errorf("status = %q, want COMPLETED", got.Status)
	}
	if string(ranWithPayload) != `{}` {
		t.Errorf("handler payload = %s, want {}", ranWithPayload)
	}
	if w.metrics.TasksCompletedTotal.Load() != 1 {
		t.Errorf("TasksCompletedTotal = %d, want 1", w.metrics.TasksCompletedTotal.Load())
	}
}

/**
 * Handler-failure paths: retry vs dead
 */

func TestExecute_HandlerFails_WithinRetries_RequeuesWithBackoff(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"flaky_task": func(ctx context.Context, payload json.RawMessage) error {
			return errors.New("smtp: connection refused")
		},
	})
	fs := w.store.(*fakeStore)
	// attempts will become 1 after Claim; max 5 => attempt 1 must retry.
	row := fs.seed(t, "flaky_task", 0, 5)

	runOneCycle(t, w)

	got := fs.row(row.ID)
	if got.Status != "PENDING" {
		t.Fatalf("status = %q, want PENDING (retry)", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "connection refused") {
		t.Errorf("last_error = %v, want the handler error", got.LastError)
	}
	if !got.RunAt.After(time.Now().Add(-time.Second)) {
		t.Errorf("run_at = %v, want pushed into the future by backoff", got.RunAt)
	}
	if got.RunAt.After(time.Now().Add(w.maxBackoff + time.Second)) {
		t.Errorf("run_at = %v, want within maxBackoff window", got.RunAt)
	}
	if w.metrics.TaskAttemptFailuresTotal.Load() != 1 || w.metrics.TasksRetriedTotal.Load() != 1 {
		t.Errorf("failure/retry metrics = %d/%d, want 1/1",
			w.metrics.TaskAttemptFailuresTotal.Load(), w.metrics.TasksRetriedTotal.Load())
	}
}

func TestExecute_HandlerFails_ExhaustsRetries_MarksDead(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"doomed_task": func(ctx context.Context, payload json.RawMessage) error {
			return errors.New("permanent failure")
		},
	})
	fs := w.store.(*fakeStore)
	// Seed attempts already at the cap: this attempt (2/2) failing => DEAD.
	row := fs.seed(t, "doomed_task", 1, 2)

	runOneCycle(t, w)

	got := fs.row(row.ID)
	if got.Status != "DEAD" {
		t.Fatalf("status = %q, want DEAD after exhausting retries", got.Status)
	}
	if w.metrics.TasksDeadTotal.Load() != 1 {
		t.Errorf("TasksDeadTotal = %d, want 1", w.metrics.TasksDeadTotal.Load())
	}
}

/**
 * Missing-handler path
 */

func TestExecute_NoHandlerRegistered_MarksDead(t *testing.T) {
	w := newTestWorker(nil)
	fs := w.store.(*fakeStore)
	row := fs.seed(t, "unknown_type", 0, 5)

	runOneCycle(t, w)

	got := fs.row(row.ID)
	if got.Status != "DEAD" {
		t.Fatalf("status = %q, want DEAD for unregistered type", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no handler registered") {
		t.Errorf("last_error = %v, want 'no handler registered' reason", got.LastError)
	}
}

/**
 * Ownership-lost paths: the reaper requeued the task while we were working.
 * The worker must swallow ErrTaskOwnershipLost and leave the new owner's
 * state untouched. The theft happens between claim and execute: exactly
 * where a real reaper would race the worker.
 */

func TestExecute_OwnershipLostOnComplete_Swallowed(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"stolen_task": func(ctx context.Context, payload json.RawMessage) error { return nil },
	})
	fs := w.store.(*fakeStore)
	row := fs.seed(t, "stolen_task", 0, 5)

	task := claimOne(t, w)

	// Reaper resets ownership to a "new worker" mid-flight.
	fs.mu.Lock()
	other := "rescuing-worker"
	fs.rows[row.ID.String()].LockedBy = &other
	fs.mu.Unlock()

	executeOne(t, w, task) // must not fail: MarkCompleted returns ErrTaskOwnershipLost

	got := fs.row(row.ID)
	if got.Status != "RUNNING" {
		t.Errorf("status = %q, want RUNNING (the new owner's state must be untouched)", got.Status)
	}
	if w.metrics.TasksCompletedTotal.Load() != 0 {
		t.Errorf("TasksCompletedTotal = %d, want 0 (stale worker must not count)", w.metrics.TasksCompletedTotal.Load())
	}
}

func TestExecute_OwnershipLostOnFailure_Swallowed(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"stolen_failing_task": func(ctx context.Context, payload json.RawMessage) error {
			return errors.New("boom")
		},
	})
	fs := w.store.(*fakeStore)
	row := fs.seed(t, "stolen_failing_task", 0, 5)

	task := claimOne(t, w)

	fs.mu.Lock()
	other := "rescuing-worker"
	fs.rows[row.ID.String()].LockedBy = &other
	fs.mu.Unlock()

	executeOne(t, w, task)

	got := fs.row(row.ID)
	if got.Status != "RUNNING" {
		t.Errorf("status = %q, want RUNNING (untouched by the stale worker)", got.Status)
	}
	if got.LastError != nil {
		t.Errorf("last_error = %v, want nil (stale worker must not write)", got.LastError)
	}
	if w.metrics.TasksRetriedTotal.Load() != 0 {
		t.Errorf("TasksRetriedTotal = %d, want 0 (stale worker must not count)", w.metrics.TasksRetriedTotal.Load())
	}
}

/**
 * Handler timeout: handlerTimeout bounds a hanging handler via context.
 */

func TestExecute_HandlerTimeout_CancelsHandlerContext(t *testing.T) {
	handlerCtxErr := make(chan error, 1)
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"hanging_task": func(ctx context.Context, payload json.RawMessage) error {
			<-ctx.Done()
			handlerCtxErr <- ctx.Err()
			return ctx.Err()
		},
	})
	fs := w.store.(*fakeStore)
	row := fs.seed(t, "hanging_task", 0, 5)
	w.handlerTimeout = 50 * time.Millisecond // override the 2s default

	start := time.Now()
	task := claimOne(t, w)
	executeOne(t, w, task)
	elapsed := time.Since(start)

	select {
	case err := <-handlerCtxErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("handler ctx err = %v, want DeadlineExceeded", err)
		}
	default:
		t.Fatal("handler context was never canceled")
	}
	if elapsed > 2*time.Second {
		t.Errorf("execute took %v, want ~handlerTimeout (50ms)", elapsed)
	}
	// ctx.Err() is non-nil => MarkPending path with attempts 1/5.
	if got := fs.row(row.ID); got.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING (timed-out attempt retried)", got.Status)
	}
}

// flakyHeartbeatStore fails the first N heartbeat calls, then succeeds.
// It embeds fakeStore so every other TaskStore method is unchanged.
type flakyHeartbeatStore struct {
	*fakeStore
	mu       sync.Mutex
	calls    int
	failures int
}

func (f *flakyHeartbeatStore) Heartbeat(ctx context.Context, taskID, workerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failures {
		return errors.New("transient: connection reset by peer")
	}
	return nil
}

func (f *flakyHeartbeatStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ store.TaskStore = (*flakyHeartbeatStore)(nil)

// One transient heartbeat error must not end the loop for the rest of the
// task's run; otherwise the task goes stale and gets reaped while still
// executing, which is duplicate execution.
func TestStartHeartbeat_TransientErrorDoesNotStopBeating(t *testing.T) {
	m := metrics.New()
	fs := newFakeStore(m)
	flaky := &flakyHeartbeatStore{fakeStore: fs, failures: 1}

	w := buildWorker(flaky, m, map[string]tasks.HandlerFunc{
		"long_task": func(ctx context.Context, payload json.RawMessage) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	w.heartbeatInterval = 5 * time.Millisecond
	w.handlerTimeout = 150 * time.Millisecond

	fs.seed(t, "long_task", 0, 5)
	task := claimOne(t, w)

	if err := w.execute(context.Background(), task); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if got := flaky.callCount(); got < 3 {
		t.Errorf("heartbeat calls = %d, want >= 3; the loop stopped after a transient error", got)
	}
}

// Backoff policy: full jitter, bounded by [0, cap] where cap grows
// exponentially per attempt.

func TestComputeBackoff_BoundedByCap(t *testing.T) {
	w := &Worker{baseBackoff: time.Second, maxBackoff: 5 * time.Minute}

	caps := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for attempt := 1; attempt <= 4; attempt++ {
		wantCap := caps[attempt-1]
		for range 500 {
			d := w.computeBackoff(attempt)
			if d < 0 || d > wantCap {
				t.Fatalf("attempt %d: backoff %v outside [0, %v]", attempt, d, wantCap)
			}
		}
	}
}

func TestComputeBackoff_NeverExceedsMaxBackoff(t *testing.T) {
	w := &Worker{baseBackoff: 100 * time.Millisecond, maxBackoff: 300 * time.Millisecond}
	for range 1000 {
		if d := w.computeBackoff(30); d > w.maxBackoff {
			t.Fatalf("backoff %v exceeds maxBackoff %v", d, w.maxBackoff)
		}
	}
}

func TestComputeBackoff_HasJitter(t *testing.T) {
	// Full jitter should produce varied values; 200 draws landing on fewer
	// than 10 distinct values would indicate jitter is broken.
	w := &Worker{baseBackoff: 10 * time.Millisecond, maxBackoff: time.Minute}
	distinct := map[time.Duration]struct{}{}
	for i := 0; i < 200; i++ {
		distinct[w.computeBackoff(3)] = struct{}{}
	}
	if len(distinct) < 10 {
		t.Errorf("only %d distinct backoff values in 200 draws; jitter looks broken", len(distinct))
	}
}

/**
 * Start-loop smoke tests
 */

func TestStart_EmptyQueue_StopsOnContextCancel(t *testing.T) {
	w := newTestWorker(nil)
	w.pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Start(ctx)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// Proves claim -> execute -> mark-completed loops until the queue drains.
func TestStart_ProcessesAllQueuedTasks(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"bulk": func(ctx context.Context, payload json.RawMessage) error { return nil },
	})
	fs := w.store.(*fakeStore)
	w.pollInterval = time.Millisecond

	const n = 25
	for range n {
		fs.seed(t, "bulk", 0, 3)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Start(ctx)
	}()

	eventually(t, 2*time.Second, func() bool {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return len(fs.claimed) == n
	})
	cancel()
	<-done

	if w.metrics.TasksCompletedTotal.Load() != n {
		t.Errorf("TasksCompletedTotal = %d, want %d", w.metrics.TasksCompletedTotal.Load(), n)
	}
}

// eventually is a tiny local poller so these DB-free tests do not import the
// (DB-oriented) testutil package.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// Fingerprint test: if someone adds a Worker field and forgets to wire it in
// WorkerPool.newWorker, this fails before production silently runs with a
// zero-value timing field.
func TestWorker_FieldsWired(t *testing.T) {
	w := newTestWorker(nil)

	if w.workerID == "" {
		t.Error("workerID must be set by the pool")
	}
	if w.store == nil {
		t.Error("store not wired")
	}
	if w.registry == nil {
		t.Error("registry not wired")
	}
	if w.logger == nil {
		t.Error("logger not wired")
	}
	if w.metrics == nil {
		t.Error("metrics not wired")
	}
	if w.pollInterval <= 0 || w.heartbeatInterval <= 0 || w.handlerTimeout <= 0 {
		t.Error("timing fields must be positive")
	}
	if w.baseBackoff <= 0 || w.maxBackoff <= 0 {
		t.Error("backoff fields must be positive")
	}
}

// panic within the retry budget requeues the task as PENDING and records
// the panic in last_error
func TestExecute_HandlerPanics_RequeuesInsteadOfCrashing(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"panicky_task": func(ctx context.Context, payload json.RawMessage) error {
			panic("boom: nil map write")
		},
	})

	fs := w.store.(*fakeStore)
	row := fs.seed(t, "panicky_task", 0, 5)

	// If the panic escapes execute, this line crashes the test binary.
	runOneCycle(t, w)

	got := fs.row(row.ID)
	if got.Status != "PENDING" {
		t.Fatalf("status = %q, want PENDING (panic must be retried, not crash the worker)", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "panicked") {
		t.Errorf("last_error = %v, want it to mention the panic", got.LastError)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "boom: nil map write") {
		t.Errorf("last_error = %v, want the panic value preserved", got.LastError)
	}
}

// panic with attempts exhausted transitions the task to DEAD
func TestExecute_HandlerPanics_ExhaustsRetries_MarksDead(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"panicky_task": func(ctx context.Context, payload json.RawMessage) error {
			panic("always fails")
		},
	})

	fs := w.store.(*fakeStore)
	// Seeded at 1/2, so this attempt is 2/2 => DEAD.
	row := fs.seed(t, "panicky_task", 1, 2)

	runOneCycle(t, w)

	if got := fs.row(row.ID); got.Status != "DEAD" {
		t.Fatalf("status = %q, want DEAD after max retries", got.Status)
	}
}

// the start loop keeps processing healthy tasks while a poison task
// panics and dies
func TestStart_PanickingTaskDoesNotStopWorker(t *testing.T) {
	w := newTestWorker(map[string]tasks.HandlerFunc{
		"poison": func(ctx context.Context, payload json.RawMessage) error {
			panic("poison payload")
		},
		"ok": func(ctx context.Context, payload json.RawMessage) error { return nil },
	})

	fs := w.store.(*fakeStore)
	poison := fs.seed(t, "poison", 0, 1) // one attempt => DEAD
	good := fs.seed(t, "ok", 0, 3)
	w.pollInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Start(ctx)
	}()

	// The poison task must die; the healthy task must still complete.
	eventually(t, 2*time.Second, func() bool {
		return fs.row(poison.ID).Status == "DEAD" &&
			fs.row(good.ID).Status == "COMPLETED"
	})

	cancel()
	<-done

	if w.metrics.TasksDeadTotal.Load() != 1 {
		t.Errorf("TasksDeadTotal = %d, want 1", w.metrics.TasksDeadTotal.Load())
	}
	if w.metrics.TasksCompletedTotal.Load() != 1 {
		t.Errorf("TasksCompletedTotal = %d, want 1 (worker must survive the panic)",
			w.metrics.TasksCompletedTotal.Load())
	}
}

// ctxAwareStore fails terminal writes on an expired context, matching
// PostgresTaskStore (and pgx). The plain fake ignores ctx, which is exactly
// why reusing the handler's dead context went unnoticed.
type ctxAwareStore struct {
	*fakeStore
}

func (s *ctxAwareStore) MarkCompleted(ctx context.Context, taskID, workerID string, durationMS int64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	return s.fakeStore.MarkCompleted(ctx, taskID, workerID, durationMS)
}

func (s *ctxAwareStore) MarkPending(ctx context.Context, task *store.TaskRow, workerID, lastError string, runAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mark pending: %w", err)
	}
	return s.fakeStore.MarkPending(ctx, task, workerID, lastError, runAt)
}

func (s *ctxAwareStore) MarkDead(ctx context.Context, taskID, workerID, lastError, reason string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}
	return s.fakeStore.MarkDead(ctx, taskID, workerID, lastError, reason)
}

var _ store.TaskStore = (*ctxAwareStore)(nil)

func TestExecute_HandlerTimeout_PersistsFailure(t *testing.T) {
	m := metrics.New()
	fs := newFakeStore(m)
	aware := &ctxAwareStore{fakeStore: fs}

	w := buildWorker(aware, m, map[string]tasks.HandlerFunc{
		"hanging_task": func(ctx context.Context, payload json.RawMessage) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	w.handlerTimeout = 50 * time.Millisecond

	row := fs.seed(t, "hanging_task", 0, 5)
	task := claimOne(t, w)

	if err := w.execute(context.Background(), task); err != nil {
		t.Fatalf("execute: %v", err)
	}

	got := fs.row(row.ID)
	if got.Status != "PENDING" {
		t.Fatalf("status = %q, want PENDING (timed-out attempt must be persisted)", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "deadline exceeded") {
		t.Errorf("last_error = %v, want the timeout recorded", got.LastError)
	}
}

type claimErrorStore struct {
	*fakeStore
	mu    sync.Mutex
	calls int
}

func (s *claimErrorStore) Claim(ctx context.Context, workerID string) (*store.TaskRow, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return nil, errors.New("db unreachable")
}

func (s *claimErrorStore) claimCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

var _ store.TaskStore = (*claimErrorStore)(nil)

// the worker waits a poll interval between attempts instead of hot-looping
func TestStart_ClaimErrorsBackOffInsteadOfSpinning(t *testing.T) {
	m := metrics.New()
	es := &claimErrorStore{fakeStore: newFakeStore(m)}

	w := buildWorker(es, m, nil)
	w.pollInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Start(ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	calls := es.claimCalls()
	if calls < 3 {
		t.Errorf("claim attempts = %d in 300ms, want >= 3 (must keep retrying)", calls)
	}
	if calls > 25 {
		t.Errorf("claim attempts = %d in 300ms at a 20ms poll: hot-looping, not backing off", calls)
	}
}
