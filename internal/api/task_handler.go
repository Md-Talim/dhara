package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/dharatype"
)

// TaskClient is the subset of *dhara.Client the API needs. It exists so the
// handlers stay unit-testable; production wires in the real client, so every
// task-state code path in the server goes through the library.
type TaskClient interface {
	Insert(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error)
	GetTask(ctx context.Context, id string) (*dhara.Task, error)
	ListTasks(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error)
	CancelTask(ctx context.Context, id string) (*dhara.Task, error)
	RetryTask(ctx context.Context, id string) (*dhara.Task, error)
}

type TaskHandler struct {
	client TaskClient
	logger *slog.Logger
}

func NewTaskHandler(client TaskClient, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{client: client, logger: logger}
}

func (h *TaskHandler) HandleListTasks(w http.ResponseWriter, r *http.Request) {
	logger := h.reqLogger(r)
	start := time.Now()
	status := http.StatusOK
	defer func() {
		h.logRequestDone(logger, "task.list", status, start)
	}()

	q := r.URL.Query()
	filter := dharatype.TaskListFilter{
		Status: q.Get("status"),
		Type:   q.Get("type"),
	}

	// Parse retrying filter.
	if v := q.Get("retrying"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			status = http.StatusBadRequest
			logger.Debug("invalid retrying query param", "retrying", v)
			writeError(w, status, "retrying must be true or false")
			return
		}
		filter.Retrying = &b
	}

	// Parse and clamp limit.
	filter.Limit = 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			status = http.StatusBadRequest
			logger.Debug("invalid limit query param", "limit", v)
			writeError(w, status, "limit must be a positive integer")
			return
		}
		filter.Limit = min(n, 100)
	}

	// Parse offset.
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			status = http.StatusBadRequest
			logger.Debug("invalid offset query param", "offset", v)
			writeError(w, status, "offset must be a non-negative integer")
			return
		}
		filter.Offset = n
	}

	tasks, total, err := h.client.ListTasks(r.Context(), filter)
	if err != nil {
		status = http.StatusInternalServerError
		logger.Error("failed to list tasks",
			"err", err,
			"status_filter", filter.Status,
			"type_filter", filter.Type,
			"limit", filter.Limit,
			"offset", filter.Offset,
		)
		writeError(w, status, "failed to list tasks")
		return
	}

	items := make([]taskResponse, len(tasks))
	for i := range tasks {
		items[i] = newTaskResponse(&tasks[i])
	}

	logger.Debug("tasks listed",
		"returned_count", len(items),
		"total", total,
		"status_filter", filter.Status,
		"type_filter", filter.Type,
		"limit", filter.Limit,
		"offset", filter.Offset,
	)

	writeJSON(w, http.StatusOK, envelope{
		"tasks":  items,
		"limit":  filter.Limit,
		"offset": filter.Offset,
		"total":  total,
	})
}

func (h *TaskHandler) HandleCreateTask(w http.ResponseWriter, r *http.Request) {
	logger := h.reqLogger(r)
	start := time.Now()
	status := http.StatusCreated
	defer func() {
		h.logRequestDone(logger, "task.create", status, start)
	}()

	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status = http.StatusBadRequest
		logger.Warn("invalid create task payload", "err", err)
		writeError(w, status, "request body must be valid JSON")
		return
	}

	now := time.Now()
	params := req.toInsertParams()
	params.Normalize(now)
	if err := params.Validate(now); err != nil {
		status = http.StatusBadRequest
		logger.Warn("create task validation failed", "err", err)
		writeError(w, status, err.Error())
		return
	}

	result, err := h.client.Insert(r.Context(), *params)
	if err != nil {
		if errors.Is(err, dhara.ErrTaskConflict) {
			status = http.StatusConflict
			logger.Warn("idempotency key conflict", "task_type", params.Type, "idempotency_key", params.IdempotencyKey)
			writeError(w, status, "idempotency key reused with different payload")
			return
		}
		status = http.StatusInternalServerError
		logger.Error("failed to create task", "err", err, "task_type", params.Type, "idempotency_key", params.IdempotencyKey)
		writeError(w, status, "failed to create task")
		return
	}

	if result.Duplicate {
		status = http.StatusOK
		logger.Info("task create idempotent replay", "task_id", result.Task.ID.String(), "task_type", result.Task.Type)
	} else {
		status = http.StatusCreated
		logger.Info("task created", "task_id", result.Task.ID.String(), "task_type", result.Task.Type)
	}
	writeJSON(w, status, newTaskResponse(result.Task))
}

func (h *TaskHandler) HandleGetTaskById(w http.ResponseWriter, r *http.Request) {
	logger := h.reqLogger(r)
	start := time.Now()
	status := http.StatusOK
	defer func() {
		h.logRequestDone(logger, "task.get", status, start)
	}()

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		status = http.StatusBadRequest
		writeError(w, status, "invalid task id")
		return
	}

	task, err := h.client.GetTask(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, dhara.ErrTaskNotFound) {
			status = http.StatusNotFound
			writeError(w, status, "task not found")
			return
		}
		status = http.StatusInternalServerError
		logger.Error("failed to get task by id", "err", err, "task_id", id.String())
		writeError(w, status, "failed to get task")
		return
	}

	logger.Debug("task fetched",
		"task_id", task.ID.String(),
		"task_status", task.Status,
		"attempts", task.Attempts,
	)
	writeJSON(w, http.StatusOK, newTaskResponse(task))
}

func (h *TaskHandler) HandleDeleteTask(w http.ResponseWriter, r *http.Request) {
	logger := h.reqLogger(r)
	start := time.Now()
	status := http.StatusOK
	defer func() {
		h.logRequestDone(logger, "task.cancel", status, start)
	}()

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		status = http.StatusBadRequest
		writeError(w, status, "invalid task id")
		return
	}

	task, err := h.client.CancelTask(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, dhara.ErrTaskNotFound) {
			status = http.StatusNotFound
			writeError(w, status, "task not found")
			return
		}
		status = http.StatusInternalServerError
		logger.Error("failed to cancel task", "err", err, "task_id", id.String())
		writeError(w, status, "failed to cancel task")
		return
	}

	switch task.Status {
	case "RUNNING":
		status = http.StatusConflict
		logger.Warn("cancel rejected for running task", "task_id", task.ID.String(), "task_status", task.Status)
		writeJSON(w, status, envelope{
			"error":  "task is currently running and cannot be canceled",
			"status": "RUNNING",
		})
	case "CANCELED", "COMPLETED", "DEAD":
		status = http.StatusOK
		logger.Info("task canceled or already terminal", "task_id", task.ID.String(), "task_status", task.Status)
		writeJSON(w, status, newTaskResponse(task))
	default:
		status = http.StatusInternalServerError
		logger.Error("unexpected task status after cancel", "task_id", task.ID.String(), "task_status", task.Status)
		writeError(w, status, "unexpected task state")
	}
}

func (h *TaskHandler) HandleRetryDeadTask(w http.ResponseWriter, r *http.Request) {
	logger := h.reqLogger(r)
	start := time.Now()
	status := http.StatusOK
	defer func() {
		h.logRequestDone(logger, "task.retry", status, start)
	}()

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		status = http.StatusBadRequest
		writeError(w, status, "invalid task id")
		return
	}

	task, err := h.client.RetryTask(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, dhara.ErrTaskNotFound) {
			status = http.StatusNotFound
			writeError(w, status, "task not found")
			return
		}
		if errors.Is(err, dhara.ErrTaskNotDead) {
			status = http.StatusConflict
			logger.Warn("task rejected for non-dead task", "task_id", id.String())
			writeError(w, status, "task is not in DEAD status")
			return
		}
		status = http.StatusInternalServerError
		logger.Error("failed to retry dead task", "err", err, "task_id", id)
		writeError(w, status, "failed to retry task")
		return
	}

	logger.Info("task retry requested", "task_id", task.ID.String(), "task_status", task.Status, "attempts", task.Attempts)
	writeJSON(w, http.StatusOK, newTaskResponse(task))
}

func (h *TaskHandler) reqLogger(r *http.Request) *slog.Logger {
	l := h.logger.With(
		"component", "api",
		"handler", "task",
		"method", r.Method,
		"path", r.URL.Path,
	)
	if rid := r.Header.Get("X-Request-Id"); rid != "" {
		l = l.With("request_id", rid)
	}
	return l
}

func (h *TaskHandler) logRequestDone(log *slog.Logger, event string, status int, start time.Time, attrs ...any) {
	base := []any{
		"event", event,
		"http_status", status,
		"duration_ms", time.Since(start).Milliseconds(),
	}
	log.Info("request completed", append(base, attrs...)...)
}
