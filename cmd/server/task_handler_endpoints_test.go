package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/dharatype"
)

// decodeEnvelope decodes a JSON object response body generically.
func decodeEnvelope(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var env map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&env); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	return env
}

func newTask() *dhara.Task {
	now := time.Now()
	return &dhara.Task{
		ID:        uuid.New(),
		Type:      "send_email",
		Status:    dhara.TaskStatusPending,
		Priority:  3,
		Attempts:  1,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

/**
 * GET /api/v1/tasks (list)
 **/

func TestListTasks_DefaultLimitAndOffset(t *testing.T) {
	var gotFilter dharatype.TaskListFilter
	client := &fakeTaskClient{
		listTasksFn: func(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error) {
			gotFilter = filter
			return []dhara.Task{*newTask()}, 1, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rr := httptest.NewRecorder()
	h.handleListTasks(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotFilter.Limit != 20 {
		t.Errorf("default limit = %d, want 20", gotFilter.Limit)
	}
	if gotFilter.Offset != 0 {
		t.Errorf("default offset = %d, want 0", gotFilter.Offset)
	}

	env := decodeEnvelope(t, rr)
	if env["total"].(float64) != 1 {
		t.Errorf("total = %v, want 1", env["total"])
	}
	if env["limit"].(float64) != 20 {
		t.Errorf("limit echoed = %v, want 20", env["limit"])
	}
	tasksInBody, ok := env["tasks"].([]any)
	if !ok || len(tasksInBody) != 1 {
		t.Fatalf("tasks = %v, want one element", env["tasks"])
	}
}

func TestListTasks_LimitClampedTo100(t *testing.T) {
	var gotFilter dharatype.TaskListFilter
	client := &fakeTaskClient{
		listTasksFn: func(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error) {
			gotFilter = filter
			return nil, 0, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks?limit=9999", nil)
	rr := httptest.NewRecorder()
	h.handleListTasks(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotFilter.Limit != 100 {
		t.Errorf("limit = %d, want clamped 100", gotFilter.Limit)
	}
}

func TestListTasks_InvalidParams(t *testing.T) {
	tests := []struct {
		name, query, wantErr string
	}{
		{"bad limit", "limit=abc", "limit must be a positive integer"},
		{"zero limit", "limit=0", "limit must be a positive integer"},
		{"negative limit", "limit=-5", "limit must be a positive integer"},
		{"bad offset", "offset=xyz", "offset must be a non-negative integer"},
		{"negative offset", "offset=-1", "offset must be a non-negative integer"},
		{"bad retrying", "retrying=maybe", "retrying must be true or false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTaskHandler(&fakeTaskClient{}, slogDiscard(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks?"+tt.query, nil)
			rr := httptest.NewRecorder()
			h.handleListTasks(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}
			env := decodeEnvelope(t, rr)
			if env["error"] != tt.wantErr {
				t.Errorf("error = %v, want %q", env["error"], tt.wantErr)
			}
		})
	}
}

func TestListTasks_FiltersPassedThrough(t *testing.T) {
	var gotFilter dharatype.TaskListFilter
	client := &fakeTaskClient{
		listTasksFn: func(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error) {
			gotFilter = filter
			return nil, 0, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks?status=PENDING&type=send_email&retrying=true&limit=5&offset=10", nil)
	rr := httptest.NewRecorder()
	h.handleListTasks(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if gotFilter.Status != "PENDING" || gotFilter.Type != "send_email" {
		t.Errorf("filters = %+v, want status/type passed through", gotFilter)
	}
	if gotFilter.Retrying == nil || !*gotFilter.Retrying {
		t.Errorf("retrying = %v, want true", gotFilter.Retrying)
	}
	if gotFilter.Limit != 5 || gotFilter.Offset != 10 {
		t.Errorf("pagination = %d/%d, want 5/10", gotFilter.Limit, gotFilter.Offset)
	}
}

func TestListTasks_ClientError500(t *testing.T) {
	client := &fakeTaskClient{
		listTasksFn: func(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error) {
			return nil, 0, errors.New("db down")
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rr := httptest.NewRecorder()
	h.handleListTasks(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

/**
 * GET /api/v1/tasks/{id}
 **/

func TestGetTaskById_InvalidUUID(t *testing.T) {
	h := newTaskHandler(&fakeTaskClient{}, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/not-a-uuid", nil)
	req.SetPathValue("id", "not-a-uuid")
	rr := httptest.NewRecorder()
	h.handleGetTaskById(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestGetTaskById_NotFound(t *testing.T) {
	client := &fakeTaskClient{
		getTaskFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, dhara.ErrTaskNotFound
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+uuid.New().String(), nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleGetTaskById(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestGetTaskById_InternalError(t *testing.T) {
	client := &fakeTaskClient{
		getTaskFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, errors.New("db down")
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+uuid.New().String(), nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleGetTaskById(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

func TestGetTaskById_Success(t *testing.T) {
	want := newTask()
	client := &fakeTaskClient{
		getTaskFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return want, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+want.ID.String(), nil)
	req.SetPathValue("id", want.ID.String())
	rr := httptest.NewRecorder()
	h.handleGetTaskById(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var got dhara.Task
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != want.ID || got.Status != want.Status {
		t.Errorf("got %+v, want %+v", got, *want)
	}
}

/**
 * DELETE /api/v1/tasks/{id} (cancel)
 **/

func TestCancelTask_NotFound(t *testing.T) {
	client := &fakeTaskClient{
		cancelFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, dhara.ErrTaskNotFound
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/"+uuid.New().String(), nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleDeleteTask(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestCancelTask_RunningTaskIs409(t *testing.T) {
	// Pins current v0.1.0 behavior: cancel only applies to PENDING tasks.
	// The store returns the untouched RUNNING task and the API maps it to
	// 409 Conflict.
	running := newTask()
	running.Status = dhara.TaskStatusRunning
	client := &fakeTaskClient{
		cancelFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return running, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/"+running.ID.String(), nil)
	req.SetPathValue("id", running.ID.String())
	rr := httptest.NewRecorder()
	h.handleDeleteTask(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 for RUNNING task, got %d", rr.Code)
	}
	env := decodeEnvelope(t, rr)
	if env["status"] != string(dhara.TaskStatusRunning) {
		t.Errorf("body status = %v, want RUNNING", env["status"])
	}
}

func TestCancelTask_PendingCanceled(t *testing.T) {
	canceled := newTask()
	canceled.Status = dhara.TaskStatusCanceled
	client := &fakeTaskClient{
		cancelFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return canceled, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/"+canceled.ID.String(), nil)
	req.SetPathValue("id", canceled.ID.String())
	rr := httptest.NewRecorder()
	h.handleDeleteTask(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestCancelTask_TerminalStatusesAreIdempotent200(t *testing.T) {
	for _, status := range []dhara.TaskStatus{dhara.TaskStatusCompleted, dhara.TaskStatusCanceled, dhara.TaskStatusDead} {
		t.Run(string(status), func(t *testing.T) {
			task := newTask()
			task.Status = status
			client := &fakeTaskClient{
				cancelFn: func(ctx context.Context, id string) (*dhara.Task, error) {
					return task, nil
				},
			}
			h := newTaskHandler(client, slogDiscard(t))

			req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/"+task.ID.String(), nil)
			req.SetPathValue("id", task.ID.String())
			rr := httptest.NewRecorder()
			h.handleDeleteTask(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200 for terminal status %s, got %d", status, rr.Code)
			}
		})
	}
}

func TestCancelTask_InvalidUUID(t *testing.T) {
	h := newTaskHandler(&fakeTaskClient{}, slogDiscard(t))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/nope", nil)
	req.SetPathValue("id", "nope")
	rr := httptest.NewRecorder()
	h.handleDeleteTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

/**
 * POST /api/v1/tasks/{id}/retry (retry DEAD task)
 */

func TestRetryTask_Success(t *testing.T) {
	retried := newTask()
	retried.Status = dhara.TaskStatusPending
	retried.Attempts = 0
	client := &fakeTaskClient{
		retryFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return retried, nil
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+retried.ID.String()+"/retry", nil)
	req.SetPathValue("id", retried.ID.String())
	rr := httptest.NewRecorder()
	h.handleRetryDeadTask(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestRetryTask_NotFound(t *testing.T) {
	client := &fakeTaskClient{
		retryFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, dhara.ErrTaskNotFound
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+uuid.New().String()+"/retry", nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleRetryDeadTask(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestRetryTask_NotDead(t *testing.T) {
	client := &fakeTaskClient{
		retryFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, dhara.ErrTaskNotDead
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+uuid.New().String()+"/retry", nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleRetryDeadTask(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
}

func TestRetryTask_InternalError(t *testing.T) {
	client := &fakeTaskClient{
		retryFn: func(ctx context.Context, id string) (*dhara.Task, error) {
			return nil, errors.New("db down")
		},
	}
	h := newTaskHandler(client, slogDiscard(t))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+uuid.New().String()+"/retry", nil)
	req.SetPathValue("id", uuid.New().String())
	rr := httptest.NewRecorder()
	h.handleRetryDeadTask(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
}

/**
 * writeJSON failure path (encoding errors are best-effort suppressed)
 */

func TestWriteJSON_AlwaysSetsContentType(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, http.StatusOK, envelope{"ok": true})
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}
}

// slogDiscard returns a logger that throws output away, keeping test output
// readable.
func slogDiscard(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
