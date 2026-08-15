package dhara

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/metrics"
	"github.com/md-talim/dhara/internal/store"
)

type Client struct {
	store   *store.PostgresTaskStore
	metrics *metrics.Metrics
}

func NewClient(pool *pgxpool.Pool) *Client {
	m := metrics.New()
	return &Client{
		store:   store.NewTaskStore(pool, m),
		metrics: m,
	}
}

// Insert enqueues a task, applying the shared defaults with Validation
func (c *Client) Insert(ctx context.Context, params InsertParams) (*EnqueueResult, error) {
	now := time.Now()
	params.Normalize(now)
	if err := params.Validate(now); err != nil {
		return nil, &ValidationError{Err: err}
	}

	task := params.ToTask()
	created, err := c.store.Create(ctx, task)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return &EnqueueResult{Task: newTask(task), Duplicate: !created}, nil
}

func (c *Client) InsertTx(ctx context.Context, tx pgx.Tx, params InsertParams) (*EnqueueResult, error) {
	now := time.Now()
	params.Normalize(now)
	if err := params.Validate(now); err != nil {
		return nil, &ValidationError{Err: err}
	}

	task := params.ToTask()
	created, err := c.store.CreateTx(ctx, tx, task)
	if err != nil {
		return nil, mapStoreError(err)
	}

	return &EnqueueResult{Task: newTask(task), Duplicate: !created}, nil
}

func (c *Client) Enqueue(ctx context.Context, taskType string, payload any, opts ...EnqueueOption) (*EnqueueResult, error) {
	params := InsertParams{Type: taskType}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("dhara: marshal payload: %w", err)
		}
		params.Payload = raw
	}
	for _, o := range opts {
		o(&params)
	}
	return c.Insert(ctx, params)
}

func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, taskType string, payload any, opts ...EnqueueOption) (*EnqueueResult, error) {
	params := InsertParams{Type: taskType}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("dhara: marshal payload: %w", err)
		}
		params.Payload = raw
	}
	for _, o := range opts {
		o(&params)
	}
	return c.InsertTx(ctx, tx, params)
}

func (c *Client) GetTask(ctx context.Context, id string) (*Task, error) {
	task, err := c.store.GetById(ctx, id)
	if err != nil {
		return nil, mapStoreError(err)
	}
	if task == nil {
		return nil, ErrTaskNotFound
	}
	return newTask(task), nil
}

// ListTasks return tasks matching the fitler, plus the total count.
func (c *Client) ListTasks(ctx context.Context, filter ListFilter) ([]Task, int, error) {
	tasks, total, err := c.store.List(ctx, filter)
	if err != nil {
		return nil, 0, mapStoreError(err)
	}
	out := make([]Task, len(tasks))
	for i := range tasks {
		out[i] = *newTask(&tasks[i])
	}
	return out, total, nil
}

// CancelTask cancels a PENDING task. Returns ErrTaskNotFound if it doesn't exists
func (c *Client) CancelTask(ctx context.Context, id string) (*Task, error) {
	task, err := c.store.Cancel(ctx, id)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return newTask(task), nil
}

// RetryTask moves a DEAD task back to PENDING.
func (c *Client) RetryTask(ctx context.Context, id string) (*Task, error) {
	task, err := c.store.RetryDead(ctx, id)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return newTask(task), nil
}

type QueueMetrics struct {
	TasksPending        int64
	TasksRunning        int64
	TasksCompleted      int64
	TasksCanceled       int64
	TasksDead           int64
	TasksPendingReady   int64
	TasksPendingDelayed int64
}

func (c *Client) QueueMetrics(ctx context.Context) (*QueueMetrics, error) {
	qm, err := c.store.GetQueueDBMetrics(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return &QueueMetrics{
		TasksPending:        qm.TasksPending,
		TasksRunning:        qm.TasksRunning,
		TasksCompleted:      qm.TasksCompleted,
		TasksCanceled:       qm.TasksCanceled,
		TasksDead:           qm.TasksDead,
		TasksPendingReady:   qm.TasksPendingReady,
		TasksPendingDelayed: qm.TasksPendingDelayed,
	}, nil
}

// MetricsSnapshot is a point-in-time read of the in-process counters.
type MetricsSnapshot struct {
	TasksEnqueuedTotal       int64
	TasksCompletedTotal      int64
	TaskAttemptFailuresTotal int64
	TasksRetriedTotal        int64
	TasksDeadTotal           int64
	TasksCanceledTotal       int64
	TasksReapedTotal         int64
	WorkersTotal             int64
	WorkersInflight          int64
}

func (c *Client) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		TasksEnqueuedTotal:       c.metrics.TasksEnqueuedTotal.Load(),
		TasksCompletedTotal:      c.metrics.TasksCompletedTotal.Load(),
		TaskAttemptFailuresTotal: c.metrics.TaskAttemptFailuresTotal.Load(),
		TasksRetriedTotal:        c.metrics.TasksRetriedTotal.Load(),
		TasksDeadTotal:           c.metrics.TasksDeadTotal.Load(),
		TasksCanceledTotal:       c.metrics.TasksCanceledTotal.Load(),
		TasksReapedTotal:         c.metrics.TasksReapedTotal.Load(),
		WorkersTotal:             c.metrics.WorkersTotal.Load(),
		WorkersInflight:          c.metrics.WorkersInflight.Load(),
	}
}
