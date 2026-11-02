package playground

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/dharatype"
)

var (
	ErrQueueFull  = errors.New("playground queue is full; wait for existing tasks to drain")
	ErrBadRequest = errors.New("invalid playground request")
)

func badRequestf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrBadRequest, fmt.Sprintf(format, args...))
}

// TaskClient is the subset of *dhara.Client used by the playground. Keeping it
// small makes the HTTP and orchestration layers unit-testable while production
// always wires the real client.
type TaskClient interface {
	Insert(ctx context.Context, params dhara.InsertParams) (*dhara.EnqueueResult, error)
	GetTask(ctx context.Context, id string) (*dhara.Task, error)
	ListTasks(ctx context.Context, filter dharatype.TaskListFilter) ([]dhara.Task, int, error)
	CancelTask(ctx context.Context, id string) (*dhara.Task, error)
	RetryTask(ctx context.Context, id string) (*dhara.Task, error)
	QueueMetrics(ctx context.Context) (*dhara.QueueMetrics, error)
	Metrics() dhara.MetricsSnapshot
}

type WorkerStats interface {
	Metrics() dhara.MetricsSnapshot
}

type CrashController interface {
	Enabled() bool
	Start(ctx context.Context, session string) (*CrashStatus, error)
	Status(ctx context.Context) *CrashStatus
}

type Overview struct {
	Pending             int64 `json:"pending"`
	PendingReady        int64 `json:"pending_ready"`
	PendingDelayed      int64 `json:"pending_delayed"`
	Running             int64 `json:"running"`
	Completed           int64 `json:"completed"`
	Dead                int64 `json:"dead"`
	Canceled            int64 `json:"canceled"`
	WorkersActive       int64 `json:"workers_active"`
	WorkersInflight     int64 `json:"workers_inflight"`
	EnqueuedLastMinute  int64 `json:"enqueued_last_minute"`
	CompletedLastMinute int64 `json:"completed_last_minute"`
	DeadLastMinute      int64 `json:"dead_last_minute"`
	TasksEnqueuedTotal  int64 `json:"tasks_enqueued_total"`
	CompletedTotal      int64 `json:"tasks_completed_total"`
	AttemptFailures     int64 `json:"task_attempt_failures_total"`
	RetriedTotal        int64 `json:"tasks_retried_total"`
	DeadTotal           int64 `json:"tasks_dead_total"`
	CanceledTotal       int64 `json:"tasks_canceled_total"`
	ReapedTotal         int64 `json:"tasks_reaped_total"`
}

type WorkerPool struct {
	Configured int           `json:"configured"`
	Prefix     string        `json:"prefix"`
	Active     int64         `json:"active"`
	Inflight   int64         `json:"inflight"`
	Leases     []WorkerLease `json:"leases"`
}

type Snapshot struct {
	GeneratedAt  time.Time    `json:"generated_at"`
	Scope        string       `json:"scope"`
	StatusFilter string       `json:"status_filter,omitempty"`
	Overview     Overview     `json:"overview"`
	WorkerPool   WorkerPool   `json:"worker_pool"`
	Tasks        []TaskView   `json:"tasks"`
	CrashEnabled bool         `json:"crash_enabled"`
	CrashDemo    *CrashStatus `json:"crash_demo,omitempty"`
}

type TaskDetail struct {
	Task    TaskView        `json:"task"`
	Owned   bool            `json:"owned"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Logs    []TaskLog       `json:"logs,omitempty"`
}

type BurstRequest struct {
	Size int `json:"size"`
}

type FlakyRequest struct {
	Count          int    `json:"count"`
	Mode           string `json:"mode"`
	SucceedAfterMS int    `json:"succeed_after_ms"`
}

type SlowRequest struct {
	Count int `json:"count"`
}

type ScenarioResult struct {
	Scenario string   `json:"scenario"`
	Count    int      `json:"count"`
	TaskIDs  []string `json:"task_ids"`
}

type CreateTaskInput struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	RunAt          *time.Time      `json:"run_at"`
}

type Service struct {
	cfg       *Config
	client    TaskClient
	store     *Store
	worker    WorkerStats
	startedAt time.Time
	crash     CrashController
	enqueueMu sync.Mutex
}

func NewService(cfg *Config, client TaskClient, store *Store, worker WorkerStats, crash CrashController) *Service {
	return &Service{
		cfg:       cfg,
		client:    client,
		store:     store,
		worker:    worker,
		startedAt: time.Now(),
		crash:     crash,
	}
}

func (s *Service) Snapshot(ctx context.Context, session, scope, status string, limit int) (*Snapshot, error) {
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" {
		scope = "all"
	}
	if scope != "all" && scope != "mine" {
		return nil, badRequestf("scope must be 'all' or 'mine'")
	}
	if scope == "mine" && session == "" {
		return nil, ErrSessionRequired
	}

	status = strings.ToUpper(strings.TrimSpace(status))
	switch status {
	case "", "PENDING", "RUNNING", "COMPLETED", "CANCELED", "DEAD":
	default:
		return nil, badRequestf("status must be a valid task status")
	}

	qm, err := s.client.QueueMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("queue metrics: %w", err)
	}
	tp, err := s.store.Throughput(ctx)
	if err != nil {
		return nil, err
	}

	querySession := ""
	if scope == "mine" {
		querySession = session
	}
	tasks, err := s.store.ListRecent(ctx, querySession, status, limit)
	if err != nil {
		return nil, err
	}
	if scope != "mine" {
		for i := range tasks {
			tasks[i].LastError = nil
		}
	}
	leases, err := s.store.ListRunning(ctx, 32)
	if err != nil {
		return nil, err
	}

	clientMetrics := s.client.Metrics()
	var workerMetrics dhara.MetricsSnapshot
	if s.worker != nil {
		workerMetrics = s.worker.Metrics()
	}

	snap := &Snapshot{
		GeneratedAt:  time.Now(),
		Scope:        scope,
		StatusFilter: status,
		Overview: Overview{
			Pending:             qm.TasksPending,
			PendingReady:        qm.TasksPendingReady,
			PendingDelayed:      qm.TasksPendingDelayed,
			Running:             qm.TasksRunning,
			Completed:           qm.TasksCompleted,
			Dead:                qm.TasksDead,
			Canceled:            qm.TasksCanceled,
			WorkersActive:       workerMetrics.WorkersActive,
			WorkersInflight:     workerMetrics.WorkersInflight,
			EnqueuedLastMinute:  tp.EnqueuedLastMinute,
			CompletedLastMinute: tp.CompletedLastMinute,
			DeadLastMinute:      tp.DeadLastMinute,
			TasksEnqueuedTotal:  clientMetrics.TasksEnqueuedTotal,
			CompletedTotal:      workerMetrics.TasksCompletedTotal,
			AttemptFailures:     workerMetrics.TaskAttemptFailuresTotal,
			RetriedTotal:        workerMetrics.TasksRetriedTotal,
			DeadTotal:           workerMetrics.TasksDeadTotal,
			CanceledTotal:       clientMetrics.TasksCanceledTotal,
			ReapedTotal:         workerMetrics.TasksReapedTotal,
		},
		WorkerPool: WorkerPool{
			Configured: s.cfg.WorkerCount,
			Prefix:     s.cfg.WorkerPrefix,
			Active:     workerMetrics.WorkersActive,
			Inflight:   workerMetrics.WorkersInflight,
			Leases:     leases,
		},
		Tasks:        tasks,
		CrashEnabled: s.crash != nil && s.crash.Enabled(),
	}
	if s.crash != nil && s.crash.Enabled() {
		snap.CrashDemo = s.crash.Status(ctx)
	}
	return snap, nil
}

func (s *Service) TaskDetail(ctx context.Context, session, id string) (*TaskDetail, error) {
	t, err := s.store.GetStoredTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, dhara.ErrTaskNotFound
	}

	owned := keyBelongsToSession(t.IdempotencyKey, session)
	view := toTaskView(*t, time.Now())
	detail := &TaskDetail{Task: view, Owned: owned}
	if !owned {
		detail.Task.LastError = nil
		return detail, nil
	}

	detail.Payload = t.Payload
	logs, err := s.store.TaskLogs(ctx, id, 100)
	if err != nil {
		return nil, err
	}
	detail.Logs = logs
	return detail, nil
}

func (s *Service) CreateTask(ctx context.Context, session string, in CreateTaskInput) (*dhara.EnqueueResult, error) {
	if !isAllowedType(in.Type) {
		return nil, badRequestf("task type %q is not allowed", in.Type)
	}
	payload, err := normalizePayload(in.Type, in.Payload, time.Now(), s.cfg.MaxPayloadBytes)
	if err != nil {
		return nil, badRequestf("%v", err)
	}
	if in.RunAt != nil && in.RunAt.After(time.Now().Add(s.cfg.Retention)) {
		return nil, badRequestf("run_at must be within the playground retention window (%s)", s.cfg.Retention)
	}

	key := ownedKey(session)
	if in.IdempotencyKey != nil {
		key, err = customKey(session, *in.IdempotencyKey)
		if err != nil {
			return nil, badRequestf("%v", err)
		}
	}

	// Serialize the capacity check with the insert. The in-memory gate is the
	// hard bound for this single playground process; PostgreSQL remains the
	// source of truth for the actual queue depth.
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if err := s.ensureCapacity(ctx, 1); err != nil {
		return nil, err
	}

	return s.client.Insert(ctx, dhara.InsertParams{
		Type:           in.Type,
		Payload:        payload,
		IdempotencyKey: &key,
		Priority:       in.Priority,
		MaxRetries:     in.MaxRetries,
		RunAt:          in.RunAt,
	})
}

func (s *Service) EnqueueBurst(ctx context.Context, session string, req BurstRequest) (*ScenarioResult, error) {
	size, err := validateCount(req.Size, 25, s.cfg.MaxBatch)
	if err != nil {
		return nil, badRequestf("%v", err)
	}

	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if err := s.ensureCapacity(ctx, size); err != nil {
		return nil, err
	}

	result := &ScenarioResult{Scenario: "burst", TaskIDs: make([]string, 0, size)}
	for i := 0; i < size; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"message":"burst job %d of %d"}`, i+1, size))
		task, err := s.insert(ctx, session, TypeEcho, payload, dhara.DefaultMaxRetries)
		if err != nil {
			return result, err
		}
		result.TaskIDs = append(result.TaskIDs, task.ID.String())
	}
	result.Count = len(result.TaskIDs)
	return result, nil
}

func (s *Service) EnqueueFlaky(ctx context.Context, session string, req FlakyRequest) (*ScenarioResult, error) {
	count, err := validateCount(req.Count, 3, 10)
	if err != nil {
		return nil, badRequestf("%v", err)
	}
	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "recover"
	}
	if mode != "recover" && mode != "dead" {
		return nil, badRequestf("mode must be 'recover' or 'dead'")
	}

	succeedAfter := req.SucceedAfterMS
	if succeedAfter == 0 {
		succeedAfter = 6000
	}
	if succeedAfter < 1000 || succeedAfter > 30000 {
		return nil, badRequestf("succeed_after_ms must be between 1000 and 30000")
	}

	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if err := s.ensureCapacity(ctx, count); err != nil {
		return nil, err
	}

	result := &ScenarioResult{Scenario: "flaky", TaskIDs: make([]string, 0, count)}
	for i := 0; i < count; i++ {
		var taskType string
		var payload json.RawMessage
		var maxRetries int

		if mode == "dead" {
			taskType = TypeAlwaysFail
			payload = json.RawMessage(fmt.Sprintf(`{"reason":"dead-letter demo %d"}`, i+1))
			maxRetries = 3
		} else {
			taskType = TypeFlaky
			succeedAt := time.Now().Add(time.Duration(succeedAfter) * time.Millisecond).UnixMilli()
			payload = json.RawMessage(fmt.Sprintf(`{"succeed_at":%d}`, succeedAt))
			maxRetries = 8
		}

		task, err := s.insert(ctx, session, taskType, payload, maxRetries)
		if err != nil {
			return result, err
		}
		result.TaskIDs = append(result.TaskIDs, task.ID.String())
	}
	result.Count = len(result.TaskIDs)
	return result, nil
}

func (s *Service) EnqueueSlow(ctx context.Context, session string, req SlowRequest) (*ScenarioResult, error) {
	count, err := validateCount(req.Count, 1, 3)
	if err != nil {
		return nil, badRequestf("%v", err)
	}
	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if err := s.ensureCapacity(ctx, count); err != nil {
		return nil, err
	}

	result := &ScenarioResult{Scenario: "slow", TaskIDs: make([]string, 0, count)}
	for i := 0; i < count; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"reason":"handler timeout demo %d"}`, i+1))
		task, err := s.insert(ctx, session, TypeSlowTask, payload, 2)
		if err != nil {
			return result, err
		}
		result.TaskIDs = append(result.TaskIDs, task.ID.String())
	}
	result.Count = len(result.TaskIDs)
	return result, nil
}

func (s *Service) RetryOwned(ctx context.Context, session, id string) (*dhara.Task, error) {
	t, err := s.store.GetStoredTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, dhara.ErrTaskNotFound
	}
	if err := requireOwned(t.IdempotencyKey, session); err != nil {
		return nil, err
	}

	s.enqueueMu.Lock()
	defer s.enqueueMu.Unlock()
	if t.Status == "DEAD" {
		if err := s.ensureCapacity(ctx, 1); err != nil {
			return nil, err
		}
	}
	return s.client.RetryTask(ctx, id)
}

func (s *Service) CancelOwned(ctx context.Context, session, id string) (*dhara.Task, error) {
	t, err := s.store.GetStoredTask(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, dhara.ErrTaskNotFound
	}
	if err := requireOwned(t.IdempotencyKey, session); err != nil {
		return nil, err
	}
	return s.client.CancelTask(ctx, id)
}

func (s *Service) StartCrashDemo(ctx context.Context, session string) (*CrashStatus, error) {
	if s.crash == nil || !s.crash.Enabled() {
		return nil, ErrCrashDisabled
	}
	return s.crash.Start(ctx, session)
}

func (s *Service) ensureCapacity(ctx context.Context, n int) error {
	qm, err := s.client.QueueMetrics(ctx)
	if err != nil {
		return fmt.Errorf("read queue depth: %w", err)
	}
	current := qm.TasksPending + qm.TasksRunning
	if current+int64(n) > int64(s.cfg.MaxPending) {
		return ErrQueueFull
	}
	return nil
}

func (s *Service) insert(ctx context.Context, session, taskType string, payload json.RawMessage, maxRetries int) (*dhara.Task, error) {
	payload, err := normalizePayload(taskType, payload, time.Now(), s.cfg.MaxPayloadBytes)
	if err != nil {
		return nil, badRequestf("%v", err)
	}
	key := ownedKey(session)
	priority := 0
	res, err := s.client.Insert(ctx, dhara.InsertParams{
		Type:           taskType,
		Payload:        payload,
		IdempotencyKey: &key,
		Priority:       &priority,
		MaxRetries:     &maxRetries,
	})
	if err != nil {
		return nil, err
	}
	return res.Task, nil
}
