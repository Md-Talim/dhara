package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/md-talim/dhara/internal/store"
)

type reaper struct {
	store          store.TaskStore
	logger         *slog.Logger
	interval       time.Duration
	staleThreshold time.Duration
	reaperID       string
}

func newReaper(
	store store.TaskStore,
	logger *slog.Logger,
	interval time.Duration,
	staleThreshold time.Duration,
	reaperID string,
) *reaper {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if staleThreshold <= 0 {
		staleThreshold = 30 * time.Second
	}
	if reaperID == "" {
		reaperID = "dhara-reaper"
	}

	return &reaper{
		store:          store,
		logger:         logger.With("component", "reaper", "reaper_id", reaperID),
		interval:       interval,
		staleThreshold: staleThreshold,
		reaperID:       reaperID,
	}
}

func (r *reaper) start(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("reaper started", "interval_ms", r.interval.Milliseconds(), "stale_threshold_ms", r.staleThreshold.Milliseconds())

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopping")
			return
		case <-ticker.C:
			n, err := r.store.RequeueStaleRunning(ctx, r.staleThreshold, r.reaperID)
			if err != nil {
				r.logger.Error("reaper run failed", "err", err)
				continue
			}
			if n > 0 {
				r.logger.Info("reaper requeued stale tasks", "count", n)
			}
		}
	}
}
