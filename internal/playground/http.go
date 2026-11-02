package playground

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/dharatype"
)

//go:embed web/*
var webFiles embed.FS

type PlaygroundService interface {
	Snapshot(ctx context.Context, session, scope, status string, limit int) (*Snapshot, error)
	TaskDetail(ctx context.Context, session, id string) (*TaskDetail, error)
	CreateTask(ctx context.Context, session string, in CreateTaskInput) (*dhara.EnqueueResult, error)
	EnqueueBurst(ctx context.Context, session string, req BurstRequest) (*ScenarioResult, error)
	EnqueueFlaky(ctx context.Context, session string, req FlakyRequest) (*ScenarioResult, error)
	EnqueueSlow(ctx context.Context, session string, req SlowRequest) (*ScenarioResult, error)
	RetryOwned(ctx context.Context, session, id string) (*dhara.Task, error)
	CancelOwned(ctx context.Context, session, id string) (*dhara.Task, error)
	StartCrashDemo(ctx context.Context, session string) (*CrashStatus, error)
}

type HTTPServer struct {
	cfg           *Config
	api           TaskClient
	play          PlaygroundService
	pool          *pgxpool.Pool
	clientMetrics http.Handler
	workerMetrics http.Handler
	logger        *slog.Logger
	startedAt     time.Time

	readLimiter     *RateLimiter
	mutationLimiter *RateLimiter
	sessionLimiter  *RateLimiter
}

func NewHTTPServer(cfg *Config, api TaskClient, play PlaygroundService, pool *pgxpool.Pool, clientMetrics, workerMetrics http.Handler, logger *slog.Logger) *HTTPServer {
	return &HTTPServer{
		cfg:             cfg,
		api:             api,
		play:            play,
		pool:            pool,
		clientMetrics:   clientMetrics,
		workerMetrics:   workerMetrics,
		logger:          logger,
		startedAt:       time.Now(),
		readLimiter:     NewRateLimiter(cfg.ReadRateLimitPerMin, cfg.ReadRateLimitBurst),
		mutationLimiter: NewRateLimiter(cfg.RateLimitPerMinute, cfg.RateLimitBurst),
		sessionLimiter:  NewRateLimiter(cfg.SessionRatePerMinute, cfg.SessionRateBurst),
	}
}

func (s *HTTPServer) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/livez", s.handleLive)
	mux.HandleFunc("GET /api/v1/readyz", s.handleReady)
	mux.HandleFunc("GET /api/v1/health", s.handleReady)

	mux.HandleFunc("GET /api/v1/tasks", s.handleListTasks)
	mux.HandleFunc("POST /api/v1/tasks", s.handleCreateTask)
	mux.HandleFunc("GET /api/v1/tasks/{id}", s.handleGetTask)
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", s.handleDeleteTask)
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", s.handleRetryTask)

	mux.HandleFunc("GET /api/v1/playground/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /api/v1/playground/tasks/{id}", s.handleTaskDetail)
	mux.HandleFunc("POST /api/v1/playground/scenarios/burst", s.handleBurst)
	mux.HandleFunc("POST /api/v1/playground/scenarios/flaky", s.handleFlaky)
	mux.HandleFunc("POST /api/v1/playground/scenarios/slow", s.handleSlow)
	mux.HandleFunc("POST /api/v1/playground/scenarios/crash", s.handleCrash)

	mux.Handle("GET /metrics", s.limitedRead(s.clientMetrics))
	mux.Handle("GET /api/v1/metrics", s.limitedRead(s.clientMetrics))
	mux.Handle("GET /worker/metrics", s.limitedRead(s.workerMetrics))

	sub, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	files := http.FileServer(http.FS(sub))
	mux.Handle("GET /", files)

	return s.recoverPanics(s.securityHeaders(mux))
}

func (s *HTTPServer) handleLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"uptime_s": int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *HTTPServer) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := s.pool.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "fail",
			"checks": map[string]any{"db": map[string]any{"status": "fail"}},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"uptime_s": int64(time.Since(s.startedAt).Seconds()),
		"checks": map[string]any{
			"db": map[string]any{"status": "ok", "latency_ms": time.Since(start).Milliseconds()},
		},
	})
}

func (s *HTTPServer) handleListTasks(w http.ResponseWriter, r *http.Request) {
	if !s.allowRead(w, r) {
		return
	}

	q := r.URL.Query()
	filter := dharatype.TaskListFilter{Status: q.Get("status"), Type: q.Get("type")}
	if filter.Status != "" {
		filter.Status = strings.ToUpper(strings.TrimSpace(filter.Status))
		switch filter.Status {
		case "PENDING", "RUNNING", "COMPLETED", "CANCELED", "DEAD":
		default:
			writeError(w, http.StatusBadRequest, "status must be a valid task status")
			return
		}
	}
	if v := q.Get("retrying"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "retrying must be true or false")
			return
		}
		filter.Retrying = &b
	}
	filter.Limit = 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		filter.Limit = min(n, 100)
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		filter.Offset = n
	}

	tasks, total, err := s.api.ListTasks(r.Context(), filter)
	if err != nil {
		s.logger.Error("playground list tasks failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list tasks")
		return
	}
	// The core task model includes the idempotency key and last error. In the
	// playground the key doubles as the correlation/ownership marker, so it
	// must never appear in a global listing. Errors are hidden for the same
	// reason: they are only shown in the owning session's detail view.
	for i := range tasks {
		tasks[i] = sanitizeTask(tasks[i])
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": tasks, "limit": filter.Limit, "offset": filter.Offset, "total": total,
	})
}

func (s *HTTPServer) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}

	var in CreateTaskInput
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &in); err != nil {
		writeStatusError(w, err)
		return
	}

	result, err := s.play.CreateTask(r.Context(), session, in)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	status := http.StatusCreated
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, result.Task)
}

func (s *HTTPServer) handleGetTask(w http.ResponseWriter, r *http.Request) {
	if !s.allowRead(w, r) {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}
	task, err := s.api.GetTask(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, dhara.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		s.logger.Error("playground get task failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get task")
		return
	}
	writeJSON(w, http.StatusOK, sanitizeTask(*task))
}

func (s *HTTPServer) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := s.play.CancelOwned(r.Context(), session, id.String())
	if err != nil {
		if errors.Is(err, ErrNotOwned) || errors.Is(err, dhara.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		s.logger.Error("playground cancel failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel task")
		return
	}

	if task.Status == dhara.TaskStatusRunning {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "task is currently running and cannot be canceled", "status": task.Status,
		})
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *HTTPServer) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}

	task, err := s.play.RetryOwned(r.Context(), session, id.String())
	if err != nil {
		switch {
		case errors.Is(err, ErrNotOwned), errors.Is(err, dhara.ErrTaskNotFound):
			writeError(w, http.StatusNotFound, "task not found")
		case errors.Is(err, dhara.ErrTaskNotDead):
			writeError(w, http.StatusConflict, "task is not in DEAD status")
		default:
			s.logger.Error("playground retry failed", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to retry task")
		}
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.allowRead(w, r) {
		return
	}
	scope := r.URL.Query().Get("scope")
	session := ""
	if scope == "mine" {
		var err error
		session, err = SessionFromRequest(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 200")
			return
		}
		limit = n
	}

	snap, err := s.play.Snapshot(r.Context(), session, scope, r.URL.Query().Get("status"), limit)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *HTTPServer) handleTaskDetail(w http.ResponseWriter, r *http.Request) {
	if !s.allowRead(w, r) {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id")
		return
	}
	session, _ := SessionFromRequest(r)
	detail, err := s.play.TaskDetail(r.Context(), session, id.String())
	if err != nil {
		if errors.Is(err, dhara.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		s.logger.Error("playground task detail failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to load task")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *HTTPServer) handleBurst(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	var req BurstRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeStatusError(w, err)
		return
	}
	result, err := s.play.EnqueueBurst(r.Context(), session, req)
	s.writeScenarioResult(w, result, err)
}

func (s *HTTPServer) handleFlaky(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	var req FlakyRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeStatusError(w, err)
		return
	}
	result, err := s.play.EnqueueFlaky(r.Context(), session, req)
	s.writeScenarioResult(w, result, err)
}

func (s *HTTPServer) handleSlow(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	var req SlowRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeStatusError(w, err)
		return
	}
	result, err := s.play.EnqueueSlow(r.Context(), session, req)
	s.writeScenarioResult(w, result, err)
}

func (s *HTTPServer) handleCrash(w http.ResponseWriter, r *http.Request) {
	session, ok := s.checkMutation(w, r)
	if !ok {
		return
	}
	if !s.drainBoundedBody(w, r) {
		return
	}
	status, err := s.play.StartCrashDemo(r.Context(), session)
	if err != nil {
		switch {
		case errors.Is(err, ErrCrashDisabled):
			writeError(w, http.StatusNotFound, "crash demo is disabled")
		case errors.Is(err, ErrCrashBusy):
			writeError(w, http.StatusConflict, err.Error())
		default:
			s.logger.Error("playground crash demo failed", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to start crash demo")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, status)
}

func (s *HTTPServer) writeScenarioResult(w http.ResponseWriter, result *ScenarioResult, err error) {
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *HTTPServer) writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueFull):
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, ErrSessionRequired):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, dhara.ErrTaskConflict):
		writeError(w, http.StatusConflict, "idempotency key reused with a different payload")
	case errors.Is(err, ErrBadRequest):
		msg := strings.TrimPrefix(err.Error(), ErrBadRequest.Error()+": ")
		writeError(w, http.StatusBadRequest, msg)
	default:
		var verr *dhara.ValidationError
		if errors.As(err, &verr) {
			writeError(w, http.StatusBadRequest, verr.Error())
			return
		}
		var serr *statusError
		if errors.As(err, &serr) {
			writeError(w, serr.code, serr.message)
			return
		}
		s.logger.Error("playground service error", "err", err)
		writeError(w, http.StatusInternalServerError, "request failed")
	}
}

func (s *HTTPServer) checkMutation(w http.ResponseWriter, r *http.Request) (string, bool) {
	if ok, wait := s.mutationLimiter.Allow("ip:" + requestIP(r, s.cfg.TrustProxy)); !ok {
		writeRetryAfter(w, wait)
		return "", false
	}
	session, err := SessionFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	if ok, wait := s.sessionLimiter.Allow("session:" + session); !ok {
		writeRetryAfter(w, wait)
		return "", false
	}
	return session, true
}

func (s *HTTPServer) allowRead(w http.ResponseWriter, r *http.Request) bool {
	if ok, wait := s.readLimiter.Allow("ip:" + requestIP(r, s.cfg.TrustProxy)); !ok {
		writeRetryAfter(w, wait)
		return false
	}
	return true
}

func (s *HTTPServer) limitedRead(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.allowRead(w, r) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

// drainBoundedBody consumes a request body that is not otherwise parsed. It
// keeps unauthenticated endpoints from accepting unbounded payloads.
func (s *HTTPServer) drainBoundedBody(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must not exceed %d bytes", s.cfg.MaxBodyBytes))
			return false
		}
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return false
	}
	return true
}

func (s *HTTPServer) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.logger.Error("playground handler panic", "panic", v)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusError struct {
	code    int
	message string
}

func (e *statusError) Error() string { return e.message }

func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return &statusError{code: http.StatusRequestEntityTooLarge, message: fmt.Sprintf("request body must not exceed %d bytes", maxBytes)}
		}
		if errors.Is(err, io.EOF) {
			return &statusError{code: http.StatusBadRequest, message: "request body must be valid JSON"}
		}
		return &statusError{code: http.StatusBadRequest, message: "request body must be valid JSON"}
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return &statusError{code: http.StatusBadRequest, message: "request body must contain a single JSON object"}
	}
	return nil
}

func writeStatusError(w http.ResponseWriter, err error) {
	var serr *statusError
	if errors.As(err, &serr) {
		writeError(w, serr.code, serr.message)
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func writeRetryAfter(w http.ResponseWriter, wait time.Duration) {
	secs := int(wait.Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeJSON(w, http.StatusTooManyRequests, map[string]any{
		"error":           "rate limit exceeded; slow down and try again",
		"retry_after_sec": secs,
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Response is already partially written; nothing useful remains.
		_ = err
	}
}

func sanitizeTask(t dhara.Task) dhara.Task {
	t.IdempotencyKey = nil
	t.LastError = nil
	return t
}
