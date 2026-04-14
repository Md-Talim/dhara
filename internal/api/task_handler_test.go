package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara/internal/store"
)

func TestCreateTask_InvalidJSON(t *testing.T) {
	store := &fakeTaskStore{}
	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{invalid}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got	%d", rr.Code)
	}
}

func TestCreateTask_ValidationError(t *testing.T) {
	store := &fakeTaskStore{}
	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": ""}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got	%d", rr.Code)
	}
}

func TestCreateTask_Created(t *testing.T) {
	store := &fakeTaskStore{
		createFn: func(ctx context.Context, task *store.Task) (bool, error) {
			task.ID = uuid.New()
			task.Status = "PENDING"
			task.CreatedAt = time.Now()
			task.UpdatedAt = time.Now()
			return true, nil
		},
	}
	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got	%d", rr.Code)
	}

	resp := decodeResponse(t, rr)
	if resp.Type != "send_email" {
		t.Fatalf("unexpected type: %s", resp.Type)
	}
}

func TestCreateTask_IdempotentReplay(t *testing.T) {
	store := &fakeTaskStore{
		createFn: func(ctx context.Context, task *store.Task) (bool, error) {
			task.ID = uuid.New()
			return false, nil
		},
	}

	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestCreateTask_Conflict(t *testing.T) {
	store := &fakeTaskStore{
		createFn: func(ctx context.Context, task *store.Task) (bool, error) {
			return false, store.ErrTaskConflict
		},
	}

	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestCreateTask_InternalError(t *testing.T) {
	store := &fakeTaskStore{
		createFn: func(ctx context.Context, task *store.Task) (bool, error) {
			return false, errors.New("db down")
		},
	}

	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestCreateTask_DefaultsApplied(t *testing.T) {
	store := &fakeTaskStore{
		createFn: func(ctx context.Context, task *store.Task) (bool, error) {
			if task.Priority != 0 {
				t.Fatalf("expected default priority 0")
			}
			if task.MaxRetries != 5 {
				t.Fatalf("expected default retries 5")
			}
			return true, nil
		},
	}

	h := NewTaskHandler(store, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)
}

func TestCreateTask_RunAtPast(t *testing.T) {
	past := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)

	body := fmt.Sprintf(`{
		"type": "send_email",
		"run_at": "%s"
	}`, past)

	store := &fakeTaskStore{}
	h := NewTaskHandler(store, slog.Default())

	req := newRequest(body)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

type fakeTaskStore struct {
	createFn            func(ctx context.Context, task *store.Task) (bool, error)
	getById             func(ctx context.Context, id string) (*store.Task, error)
	listFn              func(ctx context.Context, filter store.TaskListFilter) (tasks []store.Task, total int, err error)
	cancelById          func(ctx context.Context, id string) (*store.Task, error)
	claim               func(ctx context.Context, workerID string) (*store.Task, error)
	heartbeat           func(ctx context.Context, taskID, workerID string) error
	markCompleted       func(ctx context.Context, taskID string) error
	markPending         func(ctx context.Context, task *store.Task, lastError string, runAt time.Time) error
	markDead            func(ctx context.Context, taskID, workerID, lastError string) error
	requeueStaleRunning func(ctx context.Context, staleThreshold time.Duration, reaperID string) (int64, error)
}

func (f *fakeTaskStore) Create(ctx context.Context, task *store.Task) (bool, error) {
	return f.createFn(ctx, task)
}

func (f *fakeTaskStore) GetById(ctx context.Context, id string) (*store.Task, error) {
	return f.getById(ctx, id)
}

func (f *fakeTaskStore) List(ctx context.Context, filter store.TaskListFilter) ([]store.Task, int, error) {
	if f.listFn != nil {
		return f.listFn(ctx, filter)
	}
	return nil, 0, nil
}

func (f *fakeTaskStore) Cancel(ctx context.Context, id string) (*store.Task, error) {
	return f.cancelById(ctx, id)
}

func (f *fakeTaskStore) Claim(ctx context.Context, workerID string) (*store.Task, error) {
	return f.claim(ctx, workerID)
}

func (f *fakeTaskStore) Heartbeat(ctx context.Context, taskID, workerID string) error {
	return f.heartbeat(ctx, taskID, workerID)
}

func (f *fakeTaskStore) MarkCompleted(ctx context.Context, taskID, workerID string, durationMS int64) error {
	return f.markCompleted(ctx, taskID)
}

func (f *fakeTaskStore) MarkPending(ctx context.Context, task *store.Task, workerID, lastError string, runAt time.Time) error {
	return f.markPending(ctx, task, lastError, runAt)
}

func (f *fakeTaskStore) MarkDead(ctx context.Context, taskID, workerID, lastError, reason string) error {
	return f.markDead(ctx, taskID, workerID, lastError)
}

func (f *fakeTaskStore) RequeueStaleRunning(ctx context.Context, staleThreshold time.Duration, reaperID string) (int64, error) {
	return f.requeueStaleRunning(ctx, staleThreshold, reaperID)
}

func newRequest(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
}

func decodeResponse(t *testing.T, rr *httptest.ResponseRecorder) taskResponse {
	var resp taskResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return resp
}
