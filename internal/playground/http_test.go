package playground

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
)

type fakePlaygroundService struct {
	snapshot      *Snapshot
	snapshotErr   error
	createResult  *dhara.EnqueueResult
	createErr     error
	createSession string
	createInput   CreateTaskInput
	detail        *TaskDetail
	detailErr     error
	scenario      *ScenarioResult
	scenarioErr   error
	retryTask     *dhara.Task
	retryErr      error
	cancelTask    *dhara.Task
	cancelErr     error
	crash         *CrashStatus
	crashErr      error
}

func (f *fakePlaygroundService) Snapshot(ctx context.Context, session, scope, status string, limit int) (*Snapshot, error) {
	if f.snapshot != nil {
		return f.snapshot, f.snapshotErr
	}
	return &Snapshot{}, f.snapshotErr
}
func (f *fakePlaygroundService) TaskDetail(ctx context.Context, session, id string) (*TaskDetail, error) {
	return f.detail, f.detailErr
}
func (f *fakePlaygroundService) CreateTask(ctx context.Context, session string, in CreateTaskInput) (*dhara.EnqueueResult, error) {
	f.createSession = session
	f.createInput = in
	return f.createResult, f.createErr
}
func (f *fakePlaygroundService) EnqueueBurst(ctx context.Context, session string, req BurstRequest) (*ScenarioResult, error) {
	return f.scenario, f.scenarioErr
}
func (f *fakePlaygroundService) EnqueueFlaky(ctx context.Context, session string, req FlakyRequest) (*ScenarioResult, error) {
	return f.scenario, f.scenarioErr
}
func (f *fakePlaygroundService) EnqueueSlow(ctx context.Context, session string, req SlowRequest) (*ScenarioResult, error) {
	return f.scenario, f.scenarioErr
}
func (f *fakePlaygroundService) RetryOwned(ctx context.Context, session, id string) (*dhara.Task, error) {
	return f.retryTask, f.retryErr
}
func (f *fakePlaygroundService) CancelOwned(ctx context.Context, session, id string) (*dhara.Task, error) {
	return f.cancelTask, f.cancelErr
}
func (f *fakePlaygroundService) StartCrashDemo(ctx context.Context, session string) (*CrashStatus, error) {
	return f.crash, f.crashErr
}

func testHTTPHandler(t *testing.T, cfg *Config, api *fakeTaskClient, play *fakePlaygroundService) http.Handler {
	t.Helper()
	if cfg == nil {
		cfg = testConfig()
	}
	if api == nil {
		api = &fakeTaskClient{}
	}
	if play == nil {
		play = &fakePlaygroundService{}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewHTTPServer(cfg, api, play, nil,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		logger,
	)
	return srv.Routes()
}

func doRequest(handler http.Handler, method, path, body, session string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "203.0.113.9:4567"
	if session != "" {
		req.Header.Set(sessionHeader, session)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func TestHTTPMutationRequiresSession(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, nil)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo"}`, "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestHTTPCreateTaskMalformedAndOversized(t *testing.T) {
	session := uuid.NewString()

	handler := testHTTPHandler(t, nil, nil, nil)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":`, session)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed status = %d, want 400", rr.Code)
	}

	rr = doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo","unknown":true}`, session)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", rr.Code)
	}

	cfg := testConfig()
	cfg.MaxBodyBytes = 40
	handler = testHTTPHandler(t, cfg, nil, nil)
	rr = doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo","payload":{"message":"`+strings.Repeat("x", 100)+`"}}`, session)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d, want 413", rr.Code)
	}
}

func TestHTTPCreateTaskSuccess(t *testing.T) {
	session := uuid.NewString()
	play := &fakePlaygroundService{
		createResult: &dhara.EnqueueResult{Task: &dhara.Task{ID: uuid.New(), Type: "echo", Status: dhara.TaskStatusPending}},
	}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo","payload":{"message":"hi"}}`, session)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if play.createSession != session {
		t.Errorf("session = %q, want %q", play.createSession, session)
	}
	if play.createInput.Type != "echo" {
		t.Errorf("type = %q", play.createInput.Type)
	}
}

func TestHTTPCreateTaskValidationError(t *testing.T) {
	play := &fakePlaygroundService{createErr: badRequestf("task type %q is not allowed", "shell")}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"shell"}`, uuid.NewString())
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "not allowed") {
		t.Errorf("body = %s", rr.Body.String())
	}
}

func TestHTTPCreateTaskConflict(t *testing.T) {
	play := &fakePlaygroundService{createErr: dhara.ErrTaskConflict}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo"}`, uuid.NewString())
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
}

func TestHTTPSnapshotBadStatus(t *testing.T) {
	play := &fakePlaygroundService{snapshotErr: badRequestf("status must be a valid task status")}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodGet, "/api/v1/playground/snapshot?status=NOPE", "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestHTTPMutationRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.RateLimitPerMinute = 1
	cfg.RateLimitBurst = 1
	handler := testHTTPHandler(t, cfg, nil, nil)
	session := uuid.NewString()

	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo"}`, session)
	if rr.Code == http.StatusTooManyRequests {
		t.Fatalf("first request rate limited: %s", rr.Body.String())
	}
	rr = doRequest(handler, http.MethodPost, "/api/v1/tasks", `{"type":"echo"}`, session)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
}

func TestHTTPSnapshotMineRequiresSession(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, nil)
	rr := doRequest(handler, http.MethodGet, "/api/v1/playground/snapshot?scope=mine", "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestHTTPDeadTaskRetryOwnership(t *testing.T) {
	play := &fakePlaygroundService{retryErr: ErrNotOwned}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodPost, "/api/v1/tasks/"+uuid.NewString()+"/retry", "", uuid.NewString())
	if rr.Code != http.StatusNotFound {
		t.Fatalf("not-owned status = %d, want 404", rr.Code)
	}

	play = &fakePlaygroundService{retryErr: dhara.ErrTaskNotDead}
	handler = testHTTPHandler(t, nil, nil, play)
	rr = doRequest(handler, http.MethodPost, "/api/v1/tasks/"+uuid.NewString()+"/retry", "", uuid.NewString())
	if rr.Code != http.StatusConflict {
		t.Fatalf("not-dead status = %d, want 409", rr.Code)
	}
}

func TestHTTPCancelRunningTask(t *testing.T) {
	play := &fakePlaygroundService{cancelTask: &dhara.Task{Status: dhara.TaskStatusRunning}}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodDelete, "/api/v1/tasks/"+uuid.NewString(), "", uuid.NewString())
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
}

func TestHTTPCrashDemoErrors(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, &fakePlaygroundService{crashErr: ErrCrashBusy})
	rr := doRequest(handler, http.MethodPost, "/api/v1/playground/scenarios/crash", `{}`, uuid.NewString())
	if rr.Code != http.StatusConflict {
		t.Fatalf("busy status = %d, want 409", rr.Code)
	}

	handler = testHTTPHandler(t, nil, nil, &fakePlaygroundService{crashErr: ErrCrashDisabled})
	rr = doRequest(handler, http.MethodPost, "/api/v1/playground/scenarios/crash", `{}`, uuid.NewString())
	if rr.Code != http.StatusNotFound {
		t.Fatalf("disabled status = %d, want 404", rr.Code)
	}
}

func TestHTTPListTasksInvalidLimit(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, nil)
	rr := doRequest(handler, http.MethodGet, "/api/v1/tasks?limit=abc", "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}

	rr = doRequest(handler, http.MethodGet, "/api/v1/tasks?status=NOPE", "", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad status = %d, want 400", rr.Code)
	}
}

func TestHTTPGlobalTaskResponsesHideOwnershipAndErrors(t *testing.T) {
	key := "demo:00000000-0000-4000-8000-000000000000:secret"
	errMsg := "payload-specific detail"
	task := dhara.Task{
		ID:             uuid.New(),
		Type:           "echo",
		Status:         dhara.TaskStatusDead,
		IdempotencyKey: &key,
		LastError:      &errMsg,
	}
	api := &fakeTaskClient{listTasks: []dhara.Task{task}, getTask: &task}
	handler := testHTTPHandler(t, nil, api, nil)

	rr := doRequest(handler, http.MethodGet, "/api/v1/tasks", "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "idempotency_key") || strings.Contains(rr.Body.String(), "secret") || strings.Contains(rr.Body.String(), "payload-specific") {
		t.Fatalf("list leaked ownership/error: %s", rr.Body.String())
	}

	rr = doRequest(handler, http.MethodGet, "/api/v1/tasks/"+task.ID.String(), "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "idempotency_key") || strings.Contains(rr.Body.String(), "secret") || strings.Contains(rr.Body.String(), "payload-specific") {
		t.Fatalf("get leaked ownership/error: %s", rr.Body.String())
	}
}

func TestHTTPTaskDetailNotFound(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, &fakePlaygroundService{detailErr: dhara.ErrTaskNotFound})
	rr := doRequest(handler, http.MethodGet, "/api/v1/playground/tasks/"+uuid.NewString(), "", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
}

func TestHTTPScenarioSuccess(t *testing.T) {
	play := &fakePlaygroundService{scenario: &ScenarioResult{Scenario: "burst", Count: 2, TaskIDs: []string{"a", "b"}}}
	handler := testHTTPHandler(t, nil, nil, play)
	rr := doRequest(handler, http.MethodPost, "/api/v1/playground/scenarios/burst", `{"size":2}`, uuid.NewString())
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	var result ScenarioResult
	if err := json.NewDecoder(bytes.NewReader(rr.Body.Bytes())).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("count = %d", result.Count)
	}
}

func TestHTTPHealthEndpointsWithoutReadyDB(t *testing.T) {
	handler := testHTTPHandler(t, nil, nil, nil)
	rr := doRequest(handler, http.MethodGet, "/api/v1/livez", "", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("livez status = %d", rr.Code)
	}
}
