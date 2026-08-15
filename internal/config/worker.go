package config

import (
	"fmt"
	"time"
)

// WorkerConfig holds the worker process / worker pool settings.
type WorkerConfig struct {
	WorkerPrefix      string
	WorkerCount       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	StuckThreshold    time.Duration
	ReaperInterval    time.Duration
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
}

func NewWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		WorkerPrefix:      getEnvString("WORKER_PREFIX", "dhara-worker"),
		WorkerCount:       getEnvInt("WORKER_COUNT", 5),
		PollInterval:      getEnvDuration("POLL_INTERVAL", time.Second),
		HeartbeatInterval: getEnvDuration("HEARTBEAT_INTERVAL", 30*time.Second),
		HandlerTimeout:    getEnvDuration("HANDLER_TIMEOUT", 5*time.Minute),
		StuckThreshold:    getEnvDuration("STUCK_THRESHOLD", 5*time.Minute),
		ReaperInterval:    getEnvDuration("REAPER_INTERVAL", 30*time.Second),
		BaseBackoff:       getEnvDuration("BASE_BACKOFF", time.Second),
		MaxBackoff:        getEnvDuration("MAX_BACKOFF", 5*time.Minute),
	}
}

func (c *WorkerConfig) Validate() error {
	if c.WorkerPrefix == "" {
		return fmt.Errorf("WORKER_PREFIX must not be empty")
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("WORKER_COUNT must be >= 1")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("POLL_INTERVAL must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("HEARTBEAT_INTERVAL must be > 0")
	}
	if c.HandlerTimeout <= 0 {
		return fmt.Errorf("HANDLER_TIMEOUT must be > 0")
	}
	if c.StuckThreshold <= c.HeartbeatInterval {
		return fmt.Errorf("STUCK_THRESHOLD must be more than HEARTBEAT_INTERVAL")
	}
	if c.ReaperInterval <= 0 {
		return fmt.Errorf("REAPER_INTERVAL must be > 0")
	}
	if c.ReaperInterval > c.StuckThreshold {
		return fmt.Errorf("REAPER_INTERVAL (%s) should be <= STUCK_THRESHOLD (%s)", c.ReaperInterval, c.StuckThreshold)
	}
	if c.BaseBackoff <= 0 || c.MaxBackoff <= 0 {
		return fmt.Errorf("BASE_BACKOFF and MAX_BACKOFF must be > 0")
	}
	if c.BaseBackoff > c.MaxBackoff {
		return fmt.Errorf("BASE_BACKOFF (%s) must be <= MAX_BACKOFF (%s)", c.BaseBackoff, c.MaxBackoff)
	}
	return nil
}
