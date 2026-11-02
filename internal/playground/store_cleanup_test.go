package playground

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/testutil"
)

func TestStoreCleanupIsSchemaScoped(t *testing.T) {
	testutil.SkipIfNoTestDB(t)
	ctx := context.Background()

	cfg := testConfig()
	cfg.DatabaseURL = testutil.TestDBURL()
	suffix := time.Now().UnixNano()
	schemaA := "pgtest_cleanup_" + strconv.FormatInt(suffix, 10)
	schemaB := "pgtest_other_" + strconv.FormatInt(suffix, 10)

	poolA, err := PrepareSchema(ctx, cfg, schemaA)
	if err != nil {
		t.Fatalf("prepare schema A: %v", err)
	}
	defer poolA.Close()
	poolB, err := PrepareSchema(ctx, cfg, schemaB)
	if err != nil {
		t.Fatalf("prepare schema B: %v", err)
	}
	defer poolB.Close()
	defer dropSchemas(t, cfg.DatabaseURL, schemaA, schemaB)

	clientA := dhara.NewClient(poolA)
	clientB := dhara.NewClient(poolB)
	storeA := NewStore(poolA)
	now := time.Now()

	oldTask := enqueueTestTask(t, clientA, "echo")
	newTask := enqueueTestTask(t, clientA, "echo")
	runningTask := enqueueTestTask(t, clientA, "slow_task")
	otherTask := enqueueTestTask(t, clientB, "echo")

	mustExec(t, poolA, `UPDATE tasks SET created_at = now() - interval '2 hours' WHERE id = $1`, oldTask)
	mustExec(t, poolA, `UPDATE tasks SET created_at = now() - interval '2 hours', status = 'RUNNING', locked_by = 'worker-x', locked_at = now() WHERE id = $1`, runningTask)
	mustExec(t, poolA, `INSERT INTO task_logs(task_id, status, message) VALUES ($1, 'PENDING', 'old log')`, oldTask)
	mustExec(t, poolA, `INSERT INTO task_logs(task_id, status, message) VALUES ($1, 'RUNNING', 'kept log')`, newTask)
	mustExec(t, poolA, `INSERT INTO dead_letters(task_id, last_error) VALUES ($1, 'old error')`, oldTask)

	deleted, err := storeA.DeleteOlderThan(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1 (RUNNING task must be preserved)", deleted)
	}

	assertCount(t, poolA, `SELECT count(*) FROM tasks WHERE id = $1`, oldTask, 0)
	assertCount(t, poolA, `SELECT count(*) FROM tasks WHERE id = $1`, newTask, 1)
	assertCount(t, poolA, `SELECT count(*) FROM tasks WHERE id = $1`, runningTask, 1)
	assertCount(t, poolA, `SELECT count(*) FROM task_logs WHERE task_id = $1`, oldTask, 0)
	assertCount(t, poolA, `SELECT count(*) FROM task_logs WHERE task_id = $1`, newTask, 1)
	assertCount(t, poolA, `SELECT count(*) FROM dead_letters WHERE task_id = $1`, oldTask, 0)

	// The unrelated schema must be untouched even though it contains a task.
	assertCount(t, poolB, `SELECT count(*) FROM tasks WHERE id = $1`, otherTask, 1)
}

func enqueueTestTask(t *testing.T, client *dhara.Client, taskType string) uuid.UUID {
	t.Helper()
	key := testutil.UniqueKey(t, "playground-cleanup")
	res, err := client.Insert(context.Background(), dhara.InsertParams{
		Type:           taskType,
		Payload:        []byte(`{}`),
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("enqueue %s: %v", taskType, err)
	}
	return res.Task.ID
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func assertCount(t *testing.T, pool *pgxpool.Pool, sql string, arg any, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), sql, arg).Scan(&got); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if got != want {
		t.Errorf("count = %d, want %d", got, want)
	}
}

func dropSchemas(t *testing.T, databaseURL string, schemas ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Logf("drop schemas: open pool: %v", err)
		return
	}
	defer pool.Close()
	for _, schema := range schemas {
		ident := pgx.Identifier{schema}.Sanitize()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	}
}
