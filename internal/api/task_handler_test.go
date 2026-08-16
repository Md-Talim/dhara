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
	"github.com/md-talim/dhara"
)

func TestCreateTask_InvalidJSON(t *testing.T) {
	h := NewTaskHandler(&fakeTaskClient{}, slog.Default())

	req := newRequest(`{invalid}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestCreateTask_ValidationError(t *testing.T) {
	h := NewTaskHandler(&fakeTaskClient{}, slog.Default())

	req := newRequest(`{"type": ""}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestCreateTask_Created(t *testing.T) {
	client := &fakeTaskClient{
		insertFn: func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
			now := time.Now()
			return &dhara.EnqueueResult{
				Task: &dhara.Task{
					ID:         uuid.New(),
					Type:       params.Type,
					Status:     dhara.TaskStatusPending,
					Priority:   *params.Priority,
					MaxRetries: *params.MaxRetries,
					RunAt:      *params.RunAt,
					CreatedAt:  now,
					UpdatedAt:  now,
				},
				Duplicate: false,
			}, nil
		},
	}
	h := NewTaskHandler(client, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}

	resp := decodeResponse(t, rr)
	if resp.Type != "send_email" {
		t.Fatalf("unexpected type: %s", resp.Type)
	}
}

func TestCreateTask_IdempotentReplay(t *testing.T) {
	client := &fakeTaskClient{
		insertFn: func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
			return &dhara.EnqueueResult{
				Task: &dhara.Task{
					ID:        uuid.New(),
					Type:      params.Type,
					Status:    dhara.TaskStatusPending,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				Duplicate: true,
			}, nil
		},
	}

	h := NewTaskHandler(client, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestCreateTask_Conflict(t *testing.T) {
	client := &fakeTaskClient{
		insertFn: func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
			return nil, dhara.ErrTaskConflict
		},
	}

	h := NewTaskHandler(client, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestCreateTask_InternalError(t *testing.T) {
	client := &fakeTaskClient{
		insertFn: func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
			return nil, errors.New("db down")
		},
	}

	h := NewTaskHandler(client, slog.Default())

	req := newRequest(`{"type": "send_email"}`)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestCreateTask_DefaultsApplied(t *testing.T) {
	client := &fakeTaskClient{
		insertFn: func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
			if *params.Priority != 0 {
				t.Fatalf("expected default priority 0")
			}
			if *params.MaxRetries != 5 {
				t.Fatalf("expected default retries 5")
			}
			return &dhara.EnqueueResult{
				Task:      &dhara.Task{ID: uuid.New(), Type: params.Type},
				Duplicate: false,
			}, nil
		},
	}

	h := NewTaskHandler(client, slog.Default())

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

	h := NewTaskHandler(&fakeTaskClient{}, slog.Default())

	req := newRequest(body)
	rr := httptest.NewRecorder()

	h.HandleCreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

type fakeTaskClient struct {
	insertFn    func(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error)
	getTaskFn   func(ctx context.Context, id string) (*dhara.Task, error)
	listTasksFn func(ctx context.Context, filter dhara.TaskListFilter) ([]dhara.Task, int, error)
	cancelFn    func(ctx context.Context, id string) (*dhara.Task, error)
	retryFn     func(ctx context.Context, id string) (*dhara.Task, error)
}

func (f *fakeTaskClient) Insert(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error) {
	if f.insertFn != nil {
		return f.insertFn(ctx, params)
	}
	// Mirror the real client's Insert: normalize + validate, then succeed.
	now := time.Now()
	params.Normalize(now)
	if err := params.Validate(now); err != nil {
		return nil, &dhara.ValidationError{Err: err}
	}
	return &dhara.EnqueueResult{
		Task: &dhara.Task{
			ID:        uuid.New(),
			Type:      params.Type,
			Status:    dhara.TaskStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Duplicate: false,
	}, nil
}

func (f *fakeTaskClient) GetTask(ctx context.Context, id string) (*dhara.Task, error) {
	if f.getTaskFn != nil {
		return f.getTaskFn(ctx, id)
	}
	return nil, dhara.ErrTaskNotFound
}

func (f *fakeTaskClient) ListTasks(ctx context.Context, filter dhara.TaskListFilter) ([]dhara.Task, int, error) {
	if f.listTasksFn != nil {
		return f.listTasksFn(ctx, filter)
	}
	return nil, 0, nil
}

func (f *fakeTaskClient) CancelTask(ctx context.Context, id string) (*dhara.Task, error) {
	if f.cancelFn != nil {
		return f.cancelFn(ctx, id)
	}
	return nil, dhara.ErrTaskNotFound
}

func (f *fakeTaskClient) RetryTask(ctx context.Context, id string) (*dhara.Task, error) {
	if f.retryFn != nil {
		return f.retryFn(ctx, id)
	}
	return nil, dhara.ErrTaskNotFound
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
