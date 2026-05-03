package store

import "context"

type QueueDBMetrics struct {
	TasksPending   int64
	TasksRunning   int64
	TasksCompleted int64
	TasksCanceled  int64
	TasksDead      int64

	TasksPendingReady   int64
	TasksPendingDelayed int64
}

type MetricsReader interface {
	GetQueueDBMetrics(ctx context.Context) (*QueueDBMetrics, error)
}
