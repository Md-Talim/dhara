package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
	"github.com/md-talim/vow"
)

var testStore store.TaskStore
var testDB *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	db, err := setupTestDB(ctx)
	if err != nil {
		log.Fatalf("failed to setup test db: %v", err)
	}

	testStore = store.NewTaskStore(db, metrics.New())
	testDB = db

	code := m.Run()

	dropTables(ctx, db)
	db.Close()

	os.Exit(code)
}

func TestCreateTask_NewTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	task := &store.TaskRow{
		Type:       "send_email",
		Payload:    json.RawMessage(`{}`),
		Priority:   0,
		MaxRetries: 0,
		RunAt:      time.Now().Add(5 * time.Minute),
	}

	created, err := testStore.Create(ctx, task)
	if err != nil {
		t.Fatalf("Create() returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("Create() returned created=false, want true for new task")
	}

	// Check if DB fields populated
	if task.ID == (uuid.UUID{}) {
		t.Error("Create() did not populate ID")
	}
	if task.Status != "PENDING" {
		t.Errorf("Create() status = %q, want PENDING", task.Status)
	}
	if task.CreatedAt.IsZero() {
		t.Error("Create() did not populate CreatedAt")
	}
	if task.UpdatedAt.IsZero() {
		t.Error("Create() did not populate UpdatedAt")
	}
	if task.Attempts != 0 {
		t.Errorf("Create() attempts = %d, want 0", task.Attempts)
	}
}

func TestCreateTask_DelayedTask(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})
	future := time.Now().Add(1 * time.Hour).UTC().Truncate(time.Microsecond)

	task := &store.TaskRow{
		Type:       "send_email",
		Payload:    json.RawMessage(`{}`),
		Priority:   0,
		MaxRetries: 0,
		RunAt:      future,
	}

	_, err := testStore.Create(ctx, task)
	if err != nil {
		t.Fatalf("Create() delayed task returned unexpected error: %v", err)
	}
	if !task.RunAt.Equal(future) {
		t.Errorf("Create() run_at = %v, want %v", task.RunAt, future)
	}
}

func TestCreateTask_IdempotencyKey_ReturnExisting(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	key := "order_123"
	task1 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	task2 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	if _, err := testStore.Create(ctx, task1); err != nil {
		t.Fatalf("Create() first insert returned unexpected error: %v", err)
	}

	created, err := testStore.Create(ctx, task2)
	if err != nil {
		t.Fatalf("Create() duplicate idempotency key returned unexpected error: %v", err)
	}
	if created {
		t.Fatal("Create() returned created=true for duplicate idempotency key, want false")
	}
	if task2.ID != task1.ID {
		t.Errorf("Create() returned id=%v for duplicate, want original id=%v", task2.ID, task1.ID)
	}
}

func TestCreateTask_IdempotencyKey_SameKeyDifferentType(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	key := "order_123"
	task1 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	task2 := &store.TaskRow{
		Type:           "process_payment",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	if _, err := testStore.Create(ctx, task1); err != nil {
		t.Fatalf("Create() send_email returned unexpected error: %v", err)
	}

	created, err := testStore.Create(ctx, task2)
	if err != nil {
		t.Fatalf("Create() process_payment with same key returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("Create() returned created=false for same key but different type, want true")
	}
}

func TestCreateTask_NilIdempotencyKey_NoDuplicateConstraint(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	task1 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: nil,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}
	task2 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: nil,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	if _, err := testStore.Create(ctx, task1); err != nil {
		t.Fatalf("Create() first nil key returned unexpected error: %v", err)
	}
	created, err := testStore.Create(ctx, task2)
	if err != nil {
		t.Fatalf("Create() second nil key returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("Create() returned created=false for nil idempotency key, want true (nulls are not unique-constrained)")
	}
}

func TestCreateTask_IdempotencyKey_DifferentPayload_ReturnsConflictError(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	key := "order_123"
	task1 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{}`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	task2 := &store.TaskRow{
		Type:           "send_email",
		IdempotencyKey: &key,
		Payload:        json.RawMessage(`{ "to": "dummyemail@example.com", "subject": "test email" }`),
		Priority:       0,
		MaxRetries:     0,
		RunAt:          time.Now().Add(5 * time.Minute),
	}

	if _, err := testStore.Create(ctx, task1); err != nil {
		t.Fatalf("Create() first insert returned unexpected error: %v", err)
	}

	_, err := testStore.Create(ctx, task2)
	if err == nil || !errors.Is(err, store.ErrTaskConflict) {
		t.Fatalf("expected ErrTaskConflict, got %v", err)
	}
}

func TestGetTaskById(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	task := &store.TaskRow{
		Type:       "send_email",
		Payload:    json.RawMessage(`{}`),
		Priority:   0,
		MaxRetries: 0,
		RunAt:      time.Now().Add(5 * time.Minute),
	}

	created, err := testStore.Create(ctx, task)
	if err != nil {
		t.Fatalf("Create() returned unexpected error: %v", err)
	}
	if !created {
		t.Fatal("Create() returned created=false, want true for new task")
	}

	existingTask, err := testStore.GetById(ctx, task.ID.String())
	if err != nil {
		t.Fatalf("GetById() returned unexpected error: %v", err)
	}
	if existingTask == nil {
		t.Fatalf("GetById() returned nil, expected the created task")
	}
	if existingTask.ID != task.ID {
		t.Errorf("GetById() returned wrong ID: got %v, want %v", existingTask.ID, task.ID)
	}
	if existingTask.Type != task.Type {
		t.Errorf("GetById() returned wrong Type: got %v, want %v", existingTask.Type, task.Type)
	}
	if existingTask.Status != "PENDING" {
		t.Errorf("GetById() returned wrong Status: got %v, want \"PENDING\"", existingTask.Type)
	}
}

func TestGetTaskById_NotFound(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		truncateTables(ctx, testDB)
	})

	nonExistentID := uuid.New().String()
	task, err := testStore.GetById(ctx, nonExistentID)
	if err != nil {
		t.Fatalf("GetById() with non-existent ID returned unexpected error: %v", err)
	}
	if task != nil {
		t.Fatalf("GetById() with non-existent ID returned a task, want nil")
	}
}

func setupTestDB(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := db.Open(ctx, "postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable")
	if err != nil {
		return nil, err
	}

	migrator, err := vow.New(pool, os.DirFS("../../migrations"),
		vow.WithTableName("dhara_vow_migrations"),
		vow.WithLockName("dhara_vow_lock"),
	)
	if err != nil {
		return nil, err
	}

	if _, err = migrator.Up(ctx); err != nil {
		return nil, err
	}

	return pool, nil
}

func dropTables(ctx context.Context, db *pgxpool.Pool) {
	db.Exec(ctx, `DROP TABLE IF EXISTS dead_letters, task_logs, tasks, dhara_vow_migrations CASCADE`)
}

func truncateTables(ctx context.Context, db *pgxpool.Pool) {
	db.Exec(ctx, `TRUNCATE TABLE dead_letters, task_logs, tasks RESTART IDENTITY CASCADE`)
}
