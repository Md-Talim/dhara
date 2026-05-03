package metrics

import "sync/atomic"

type Metrics struct {
	TasksEnqueuedTotal       atomic.Int64
	TasksCompletedTotal      atomic.Int64
	TaskAttemptFailuresTotal atomic.Int64
	TasksRetriedTotal        atomic.Int64
	TasksDeadTotal           atomic.Int64
	TasksCanceledTotal       atomic.Int64
	TasksReapedTotal         atomic.Int64

	WorkersTotal    atomic.Int64
	WorkersInflight atomic.Int64
}

func New() *Metrics {
	return &Metrics{}
}
