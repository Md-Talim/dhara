package playground

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/metrics"
	dhstore "github.com/md-talim/dhara/internal/store"
)

var (
	ErrCrashDisabled = errors.New("crash demo is disabled")
	ErrCrashBusy     = errors.New("a crash demo is already running")
)

type CrashStatus struct {
	Enabled     bool       `json:"enabled"`
	Phase       string     `json:"phase"`
	Session     string     `json:"-"`
	TaskID      string     `json:"task_id,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	Task        *TaskView  `json:"task,omitempty"`
	ReapedTotal int64      `json:"reaped_total"`
}

type procHandle struct {
	cmd          *exec.Cmd
	done         chan struct{}
	expectedKill atomic.Bool
}

// CrashManager runs a genuinely isolated worker process for the heartbeat
// recovery demonstration. The child handles exactly one task type in a
// dedicated schema; killing it cannot affect normal playground traffic.
type CrashManager struct {
	cfg    *Config
	logger *slog.Logger
	pool   *pgxpool.Pool
	client *dhara.Client
	store  *Store
	reaper *dhstore.PostgresTaskStore
	rmet   *metrics.Metrics
	exe    string

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	status  CrashStatus
	running bool

	procMu sync.Mutex
	proc   *procHandle
}

func NewCrashManager(ctx context.Context, cfg *Config, logger *slog.Logger) (*CrashManager, error) {
	m := &CrashManager{
		cfg:    cfg,
		logger: logger.With("component", "crash-demo"),
		status: CrashStatus{Enabled: cfg.EnableCrashDemo, Phase: "idle"},
	}
	if !cfg.EnableCrashDemo {
		return m, nil
	}

	pool, err := PrepareSchema(ctx, cfg, cfg.CrashSchema)
	if err != nil {
		return nil, err
	}
	m.pool = pool
	m.client = dhara.NewClient(pool)
	m.store = NewStore(pool)
	m.rmet = metrics.New()
	m.reaper = dhstore.NewTaskStore(pool, m.rmet)
	m.exe, err = os.Executable()
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("resolve executable: %w", err)
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	go m.reaperLoop()
	return m, nil
}

func (m *CrashManager) Enabled() bool {
	return m != nil && m.cfg.EnableCrashDemo
}

func (m *CrashManager) Start(ctx context.Context, session string) (*CrashStatus, error) {
	if !m.Enabled() {
		return nil, ErrCrashDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil, ErrCrashBusy
	}
	now := time.Now()
	m.running = true
	m.status = CrashStatus{
		Enabled:   true,
		Phase:     "starting",
		Session:   session,
		StartedAt: &now,
		UpdatedAt: &now,
	}
	status := m.copyStatusLocked()
	m.mu.Unlock()

	go m.runDemo(session)
	return &status, nil
}

func (m *CrashManager) Status(ctx context.Context) *CrashStatus {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	status := m.copyStatusLocked()
	status.Enabled = m.Enabled()
	if m.rmet != nil {
		status.ReapedTotal = m.rmet.TasksReapedTotal.Load()
	}
	m.mu.Unlock()

	if status.TaskID != "" && m.store != nil {
		if t, err := m.store.GetStoredTask(ctx, status.TaskID); err == nil && t != nil {
			view := toTaskView(*t, time.Now())
			status.Task = &view
		}
	}
	return &status
}

func (m *CrashManager) Close() {
	if m == nil {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.killWorker()
	if m.pool != nil {
		m.pool.Close()
	}
}

// Cleanup applies the playground retention policy to the isolated crash
// schema. It uses the same schema-scoped delete as the main store.
func (m *CrashManager) Cleanup(ctx context.Context, cutoff time.Time) (int64, error) {
	if m == nil || m.store == nil {
		return 0, nil
	}
	return m.store.DeleteOlderThan(ctx, cutoff)
}

func (m *CrashManager) runDemo(session string) {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.CrashDemoTimeout)
	defer cancel()

	defer func() {
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
		// Leave a healthy worker ready for the next demonstration. If the
		// playground itself is shutting down, Close handles the child.
		if m.ctx.Err() == nil && !m.workerRunning() {
			if err := m.spawnWorker(); err != nil {
				m.logger.Error("failed to restart crash worker after demo", "err", err)
			}
		}
	}()

	if err := m.spawnWorker(); err != nil {
		m.fail("failed to start isolated worker: " + err.Error())
		return
	}

	m.setPhase("enqueuing task")
	key := ownedKey(session)
	maxRetries := 2
	priority := 0
	res, err := m.client.Insert(ctx, dhara.InsertParams{
		Type:           TypeCrash,
		Payload:        []byte(fmt.Sprintf(`{"duration_ms":%d}`, m.cfg.CrashTaskDuration.Milliseconds())),
		IdempotencyKey: &key,
		Priority:       &priority,
		MaxRetries:     &maxRetries,
	})
	if err != nil {
		m.fail("failed to enqueue crash task: " + err.Error())
		return
	}
	taskID := res.Task.ID.String()
	m.mu.Lock()
	m.status.TaskID = taskID
	m.mu.Unlock()

	m.setPhase("waiting for worker claim")
	task, err := m.waitFor(ctx, taskID, 8*time.Second, func(t *StoredTask) bool {
		return t.Status == "RUNNING" || t.Status == "COMPLETED" || t.Status == "DEAD"
	})
	if err != nil {
		m.fail("worker did not claim the task: " + err.Error())
		return
	}
	if task.Status != "RUNNING" {
		m.fail("task left RUNNING before the simulated crash: " + task.Status)
		return
	}

	m.setPhase("killing isolated worker process")
	if err := m.killWorker(); err != nil {
		m.fail("failed to kill isolated worker: " + err.Error())
		return
	}

	m.setPhase("waiting for stale heartbeat reaper")
	task, err = m.waitFor(ctx, taskID, m.cfg.CrashDemoTimeout, func(t *StoredTask) bool {
		return t.Status == "PENDING" || t.Status == "DEAD"
	})
	if err != nil {
		m.fail("reaper did not recover the task: " + err.Error())
		return
	}
	if task.Status == "DEAD" {
		m.fail("task was dead-lettered instead of requeued (attempt budget exhausted)")
		return
	}

	m.setPhase("restarting isolated worker")
	if err := m.spawnWorker(); err != nil {
		m.fail("failed to restart isolated worker: " + err.Error())
		return
	}

	m.setPhase("waiting for recovered task to complete")
	task, err = m.waitFor(ctx, taskID, m.cfg.CrashDemoTimeout, func(t *StoredTask) bool {
		return t.Status == "COMPLETED" || t.Status == "DEAD"
	})
	if err != nil {
		m.fail("recovered task did not finish: " + err.Error())
		return
	}
	if task.Status != "COMPLETED" {
		m.fail("recovered task ended in " + task.Status)
		return
	}

	now := time.Now()
	m.mu.Lock()
	m.status.Phase = "completed"
	m.status.UpdatedAt = &now
	m.status.FinishedAt = &now
	m.mu.Unlock()
	m.logger.Info("crash demo completed", "task_id", taskID)
}

func (m *CrashManager) waitFor(ctx context.Context, taskID string, timeout time.Duration, match func(*StoredTask) bool) (*StoredTask, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(m.cfg.CrashPollInterval)
	defer ticker.Stop()

	for {
		t, err := m.store.GetStoredTask(ctx, taskID)
		if err == nil && t != nil && match(t) {
			return t, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("timed out")
		case <-ticker.C:
		}
	}
}

func (m *CrashManager) setPhase(phase string) {
	now := time.Now()
	m.mu.Lock()
	m.status.Phase = phase
	m.status.UpdatedAt = &now
	m.status.Error = ""
	m.mu.Unlock()
	m.logger.Info("crash demo phase", "phase", phase)
}

func (m *CrashManager) fail(message string) {
	now := time.Now()
	m.mu.Lock()
	m.status.Phase = "failed"
	m.status.Error = message
	m.status.UpdatedAt = &now
	m.status.FinishedAt = &now
	m.mu.Unlock()
	m.logger.Warn("crash demo failed", "err", message)
}

func (m *CrashManager) copyStatusLocked() CrashStatus {
	s := m.status
	if s.StartedAt != nil {
		v := *s.StartedAt
		s.StartedAt = &v
	}
	if s.UpdatedAt != nil {
		v := *s.UpdatedAt
		s.UpdatedAt = &v
	}
	if s.FinishedAt != nil {
		v := *s.FinishedAt
		s.FinishedAt = &v
	}
	return s
}

func (m *CrashManager) workerRunning() bool {
	m.procMu.Lock()
	defer m.procMu.Unlock()
	return m.proc != nil
}

func childEnv(overrides map[string]string) []string {
	env := os.Environ()
	out := make([]string, 0, len(env)+len(overrides))
	for _, entry := range env {
		key := entry
		if i := strings.IndexByte(entry, '='); i >= 0 {
			key = entry[:i]
		}
		if _, replaced := overrides[key]; !replaced {
			out = append(out, entry)
		}
	}
	for key, value := range overrides {
		out = append(out, key+"="+value)
	}
	return out
}

func (m *CrashManager) spawnWorker() error {
	m.procMu.Lock()
	defer m.procMu.Unlock()
	if m.proc != nil {
		return nil
	}

	cmd := exec.Command(m.exe, "-crash-worker")
	cmd.Env = childEnv(map[string]string{
		"PLAYGROUND_DB_SCHEMA":                m.cfg.CrashSchema,
		"PLAYGROUND_CRASH_SCHEMA":             m.cfg.CrashSchema,
		"PLAYGROUND_CRASH_WORKER_PREFIX":      m.cfg.CrashWorkerPrefix,
		"PLAYGROUND_CRASH_POLL_INTERVAL":      m.cfg.CrashPollInterval.String(),
		"PLAYGROUND_CRASH_HEARTBEAT_INTERVAL": m.cfg.CrashHeartbeatInterval.String(),
		"PLAYGROUND_CRASH_STUCK_THRESHOLD":    m.cfg.CrashStuckThreshold.String(),
		"PLAYGROUND_CRASH_HANDLER_TIMEOUT":    m.cfg.CrashHandlerTimeout.String(),
	})
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	p := &procHandle{cmd: cmd, done: make(chan struct{})}
	m.proc = p
	m.logger.Info("isolated crash worker started", "pid", cmd.Process.Pid)

	go func() {
		err := cmd.Wait()
		close(p.done)
		m.procMu.Lock()
		if m.proc == p {
			m.proc = nil
		}
		m.procMu.Unlock()
		if err != nil && m.ctx.Err() == nil && !p.expectedKill.Load() {
			m.logger.Warn("isolated crash worker exited", "err", err)
		}
	}()
	return nil
}

func (m *CrashManager) killWorker() error {
	m.procMu.Lock()
	p := m.proc
	m.proc = nil
	m.procMu.Unlock()
	if p == nil {
		return errors.New("isolated worker is not running")
	}
	p.expectedKill.Store(true)

	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	select {
	case <-p.done:
		return nil
	case <-time.After(3 * time.Second):
		return errors.New("isolated worker did not exit after kill")
	}
}

func (m *CrashManager) reaperLoop() {
	ticker := time.NewTicker(m.cfg.CrashReaperInterval)
	defer ticker.Stop()
	m.logger.Info("crash-demo reaper started",
		"interval", m.cfg.CrashReaperInterval,
		"stale_threshold", m.cfg.CrashStuckThreshold,
		"schema", m.cfg.CrashSchema,
	)

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			n, err := m.reaper.RequeueStaleRunning(m.ctx, m.cfg.CrashStuckThreshold, "playground-crash-reaper")
			if err != nil {
				m.rmet.ReaperFailuresTotal.Add(1)
				m.logger.Error("crash-demo reaper failed", "err", err)
				continue
			}
			if n > 0 {
				m.logger.Info("crash-demo reaper recovered tasks", "count", n)
			}
		}
	}
}

var crashWorkerStarted atomic.Bool

// RunCrashWorker is the child-process entry point. It runs one real Dhara
// worker against the crash schema and registers only TypeCrash.
func RunCrashWorker(ctx context.Context, logger *slog.Logger) error {
	if !crashWorkerStarted.CompareAndSwap(false, true) {
		return errors.New("crash worker already started")
	}

	cfg := loadConfigRaw()
	if cfg.DatabaseURL == "" {
		return errors.New("DHARA_DATABASE_URL must be set")
	}
	if err := validateSchema(cfg.Schema); err != nil {
		return fmt.Errorf("crash worker schema: %w", err)
	}

	pool, err := PrepareSchema(ctx, cfg, cfg.Schema)
	if err != nil {
		return err
	}
	defer pool.Close()

	worker := dhara.NewWorker(pool,
		dhara.WithWorkerPrefix(cfg.CrashWorkerPrefix),
		dhara.WithMaxWorkers(1),
		dhara.WithPollInterval(cfg.CrashPollInterval),
		dhara.WithHeartbeatInterval(cfg.CrashHeartbeatInterval),
		dhara.WithHandlerTimeout(cfg.CrashHandlerTimeout),
		dhara.WithBaseBackoff(time.Second),
		dhara.WithMaxBackoff(5*time.Second),
		// The parent process owns the visible reaper so the recovery is
		// deterministic. The child still uses the same worker lifecycle.
		dhara.WithReaperInterval(time.Hour),
		dhara.WithStuckThreshold(cfg.CrashStuckThreshold),
		dhara.WithShutdownTimeout(5*time.Second),
		dhara.WithLogger(logger),
	)
	worker.RegisterHandler(TypeCrash, CrashWork)

	logger.Info("isolated crash worker starting",
		"schema", cfg.Schema,
		"worker_prefix", cfg.CrashWorkerPrefix,
		"heartbeat_interval", cfg.CrashHeartbeatInterval,
		"stuck_threshold", cfg.CrashStuckThreshold,
	)
	return worker.Start(ctx)
}
