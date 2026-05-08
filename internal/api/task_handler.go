package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/md-talim/dhara/internal/store"
)

type TaskHandler struct {
	taskStore store.TaskStore
	logger    *slog.Logger
}

func NewTaskHandler(taskStore store.TaskStore, logger *slog.Logger) *TaskHandler {
	return &TaskHandler{taskStore: taskStore, logger: logger}
}

func (h *TaskHandler) HandleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	filter := store.TaskListFilter{
		Status: q.Get("status"),
		Type:   q.Get("type"),
	}

	// Parse retrying filter.
	if v := q.Get("retrying"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "retrying must be true or false")
			return
		}
		filter.Retrying = &b
	}

	// Parse and clamp limit.
	filter.Limit = 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		filter.Limit = min(n, 100)
	}

	// Parse offset.
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		filter.Offset = n
	}

	tasks, total, err := h.taskStore.List(r.Context(), filter)
	if err != nil {
		h.logger.Error("failed to list tasks", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list tasks")
		return
	}

	items := make([]taskResponse, len(tasks))
	for i := range tasks {
		items[i] = newTaskResponse(&tasks[i])
	}

	writeJSON(w, http.StatusOK, envelope{
		"tasks":  items,
		"limit":  filter.Limit,
		"offset": filter.Offset,
		"total":  total,
	})
}

func (h *TaskHandler) HandleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Warn("decode create task request", "err", err)
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	now := time.Now()

	req.normalize(now)

	if err := req.validate(now); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	task := req.toTask()

	created, err := h.taskStore.Create(r.Context(), task)
	if err != nil {
		if errors.Is(err, store.ErrTaskConflict) {
			writeError(w, http.StatusConflict, "idempotency key reused with different payload")
			return
		}
		h.logger.Error("failed to create task", "err", err, "task_type", task.Type)
		writeError(w, http.StatusInternalServerError, "failed to create task")
		return
	}

	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, newTaskResponse(task))
}

func (h *TaskHandler) HandleGetTaskById(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := h.taskStore.GetById(r.Context(), id.String())
	if err != nil {
		h.logger.Error("failed to get task by id", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get task")
		return
	}
	if task == nil {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}

	writeJSON(w, http.StatusOK, newTaskResponse(task))
}

func (h *TaskHandler) HandleDeleteTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := h.taskStore.Cancel(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		h.logger.Error("failed to cancel task", "err", err, "task_id", id)
		writeError(w, http.StatusInternalServerError, "failed to cancel task")
		return
	}

	switch task.Status {
	case "RUNNING":
		writeJSON(w, http.StatusConflict, envelope{
			"error":  "task is currently running and cannot be canceled",
			"status": "RUNNING",
		})
	case "CANCELED", "COMPLETED", "DEAD":
		writeJSON(w, http.StatusOK, newTaskResponse(task))
	}
}

func (h *TaskHandler) HandleRetryDeadTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := h.taskStore.RetryDead(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if errors.Is(err, store.ErrTaskNotDead) {
			writeError(w, http.StatusConflict, "task is not in DEAD status")
			return
		}
		h.logger.Error("failed to retry dead task", "err", err, "task_id", id)
		writeError(w, http.StatusInternalServerError, "failed to retry task")
		return
	}

	writeJSON(w, http.StatusOK, newTaskResponse(task))
}
