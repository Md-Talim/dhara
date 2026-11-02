package playground

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/testutil"
)

func TestPlaygroundRealWorkerLifecycle(t *testing.T) {
	testutil.SkipIfNoTestDB(t)
	cfg := testConfig()
	cfg.DatabaseURL = testutil.TestDBURL()
	cfg.WorkerCount = 2
	cfg.PollInterval = 25 * time.Millisecond
	cfg.HeartbeatInterval = 200 * time.Millisecond
	cfg.HandlerTimeout = 5 * time.Second
	cfg.StuckThreshold = 2 * time.Second
	cfg.ReaperInterval = 500 * time.Millisecond
	cfg.BaseBackoff = 100 * time.Millisecond
	cfg.MaxBackoff = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := "pgtest_worker_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	pool, err := PrepareSchema(ctx, cfg, schema)
	if err != nil {
		t.Fatalf("prepare schema: %v", err)
	}
	defer pool.Close()
	defer dropSchemas(t, cfg.DatabaseURL, schema)

	client := dhara.NewClient(pool)
	store := NewStore(pool)
	worker := dhara.NewWorker(pool,
		dhara.WithWorkerPrefix("integration-worker"),
		dhara.WithMaxWorkers(2),
		dhara.WithPollInterval(cfg.PollInterval),
		dhara.WithHeartbeatInterval(cfg.HeartbeatInterval),
		dhara.WithHandlerTimeout(cfg.HandlerTimeout),
		dhara.WithStuckThreshold(cfg.StuckThreshold),
		dhara.WithReaperInterval(cfg.ReaperInterval),
		dhara.WithBaseBackoff(cfg.BaseBackoff),
		dhara.WithMaxBackoff(cfg.MaxBackoff),
		dhara.WithShutdownTimeout(5*time.Second),
	)
	worker.RegisterHandler(TypeFlaky, FlakyWork)

	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Start(ctx) }()
	defer func() {
		cancel()
		select {
		case <-workerDone:
		case <-time.After(5 * time.Second):
			t.Error("worker did not stop")
		}
	}()

	svc := NewService(cfg, client, store, worker, nil)
	session := uuid.NewString()

	t.Run("flaky task eventually completes", func(t *testing.T) {
		result, err := svc.EnqueueFlaky(ctx, session, FlakyRequest{Count: 1, Mode: "recover", SucceedAfterMS: 1200})
		if err != nil {
			t.Fatalf("EnqueueFlaky: %v", err)
		}
		id := result.TaskIDs[0]

		testutil.Eventually(t, 12*time.Second, 100*time.Millisecond, func() bool {
			task, err := store.GetStoredTask(ctx, id)
			return err == nil && task != nil && task.Status == "COMPLETED"
		})

		detail, err := svc.TaskDetail(ctx, session, id)
		if err != nil {
			t.Fatalf("TaskDetail: %v", err)
		}
		if !detail.Owned || len(detail.Payload) == 0 {
			t.Errorf("owner detail = %+v, want payload", detail)
		}
		if len(detail.Logs) == 0 {
			t.Error("expected task logs")
		}

		other, err := svc.TaskDetail(ctx, uuid.NewString(), id)
		if err != nil {
			t.Fatalf("other TaskDetail: %v", err)
		}
		if other.Owned || len(other.Payload) != 0 || len(other.Logs) != 0 {
			t.Errorf("other session leaked detail: %+v", other)
		}

		snap, err := svc.Snapshot(ctx, session, "mine", "", 100)
		if err != nil {
			t.Fatalf("Snapshot mine: %v", err)
		}
		if len(snap.Tasks) == 0 || snap.Tasks[0].ID != id {
			t.Errorf("snapshot tasks = %+v, want %s", snap.Tasks, id)
		}
		if snap.Overview.CompletedTotal == 0 {
			t.Error("snapshot worker counters are empty")
		}

		global, err := svc.Snapshot(ctx, "", "all", "", 100)
		if err != nil {
			t.Fatalf("Snapshot all: %v", err)
		}
		for _, task := range global.Tasks {
			if task.LastError != nil {
				t.Errorf("global snapshot leaked last error for %s", task.ID)
			}
		}
	})

	t.Run("dead task can be retried to completion", func(t *testing.T) {
		succeedAt := time.Now().Add(1500 * time.Millisecond).UnixMilli()
		key := ownedKey(session)
		maxRetries := 0
		res, err := client.Insert(ctx, dhara.InsertParams{
			Type:           TypeFlaky,
			Payload:        []byte(`{"succeed_at":` + strconv.FormatInt(succeedAt, 10) + `}`),
			IdempotencyKey: &key,
			MaxRetries:     &maxRetries,
		})
		if err != nil {
			t.Fatalf("insert flaky: %v", err)
		}
		id := res.Task.ID.String()

		testutil.Eventually(t, 8*time.Second, 100*time.Millisecond, func() bool {
			task, err := store.GetStoredTask(ctx, id)
			return err == nil && task != nil && task.Status == "DEAD"
		})

		time.Sleep(time.Until(time.UnixMilli(succeedAt)) + 100*time.Millisecond)

		task, err := svc.RetryOwned(ctx, session, id)
		if err != nil {
			t.Fatalf("RetryOwned: %v", err)
		}
		if task.Status != dhara.TaskStatusPending {
			t.Errorf("retry status = %s, want PENDING", task.Status)
		}

		testutil.Eventually(t, 8*time.Second, 100*time.Millisecond, func() bool {
			task, err := store.GetStoredTask(ctx, id)
			return err == nil && task != nil && task.Status == "COMPLETED"
		})

		if _, err := svc.RetryOwned(ctx, uuid.NewString(), id); err == nil {
			t.Error("another session could retry the task")
		}
	})
}
