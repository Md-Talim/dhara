package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/md-talim/dhara/internal/store"
)

func TestCreateTx_CommitPersistsTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	task := &store.Task{
		Type:       "send_email",
		Payload:    json.RawMessage(`{}`),
		Priority:   0,
		MaxRetries: 0,
		RunAt:      time.Now().Add(5 * time.Minute),
	}

	tx, err := testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	created, err := testStore.CreateTx(ctx, tx, task)
	if err != nil {
		t.Fatalf("CreateTx() returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("CreateTx() returned created=false, want true")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	got, err := testStore.GetById(ctx, task.ID.String())
	if err != nil {
		t.Fatalf("GetById() after commit returned error: %v", err)
	}
	if got == nil {
		t.Fatal("GetById() after committed CreateTx returned nil, want task")
	}
}

func TestCreateTx_RollbackDiscardsTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	task := &store.Task{
		Type:       "send_email",
		Payload:    json.RawMessage(`{}`),
		Priority:   0,
		MaxRetries: 0,
		RunAt:      time.Now().Add(5 * time.Minute),
	}

	tx, err := testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	created, err := testStore.CreateTx(ctx, tx, task)
	if err != nil {
		t.Fatalf("CreateTx() returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("CreateTx() returned created=false, want true")
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	got, err := testStore.GetById(ctx, task.ID.String())
	if err != nil {
		t.Fatalf("GetById() after rollback returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetById() after rollback returned task %v, want nil", got.ID)
	}
}

func TestCreateTx_IdempotencyKey_DuplicateReturnsExisting(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() { truncateTables(ctx, testDB) })

	key := "order_123"

	// First insert via Create (its own transaction).
	task1 := &store.Task{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}
	if _, err := testStore.Create(ctx, task1); err != nil {
		t.Fatalf("Create() returned unexpected error: %v", err)
	}

	// Duplicate via CreateTx: must replay the existing task, not insert.
	task2 := &store.Task{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	tx, err := testDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	created, err := testStore.CreateTx(ctx, tx, task2)
	if err != nil {
		t.Fatalf("CreateTx() returned unexpected error: %v", err)
	}
	if created {
		t.Fatal("CreateTx() returned created=true for duplicate idempotency key, want false")
	}
	if task2.ID != task1.ID {
		t.Errorf("CreateTx() returned id=%v for duplicate, want original id=%v", task2.ID, task1.ID)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}
