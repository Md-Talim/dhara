package dhara

import (
	"log/slog"
	"time"
)

// Config configures a Client. Pass a *Config directly to NewClient, or use
// the dhara.With* option functions
type Config struct {
	Queues  map[string]QueueConfig
	Workers WorkerConfig
	Logger  *slog.Logger
}

// QueueConfig configures a single queue.
type QueueConfig struct {
	MaxWorkers int
}

// WorkerConfig holds worker pool timing and retry settings
type WorkerConfig struct {
	WorkerPrefix      string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	StaleThreshold    time.Duration
	ReaperInterval    time.Duration
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
}

// Option customizes a Client's Config
type Option interface {
	apply(*Config)
}

type optionFunc func(*Config)

func (f optionFunc) apply(c *Config) { f(c) }

func defaultConfig() *Config {
	return &Config{
		Queues: map[string]QueueConfig{
			"default": {MaxWorkers: 5},
		},
		Workers: WorkerConfig{
			WorkerPrefix:      "dhara-worker",
			PollInterval:      time.Second,
			HeartbeatInterval: 30 * time.Second,
			HandlerTimeout:    5 * time.Minute,
			BaseBackoff:       time.Second,
			MaxBackoff:        5 * time.Minute,
			ReaperInterval:    30 * time.Second,
			StaleThreshold:    5 * time.Minute,
		},
	}
}

func (c *Config) normalize() {
	if len(c.Queues) == 0 {
		c.Queues = map[string]QueueConfig{"default": {MaxWorkers: 5}}
	}
	w := &c.Workers
	if w.WorkerPrefix == "" {
		w.WorkerPrefix = "dhara-worker"
	}
	if w.PollInterval <= 0 {
		w.PollInterval = time.Second
	}
	if w.HeartbeatInterval <= 0 {
		w.HeartbeatInterval = 30 * time.Second
	}
	if w.HandlerTimeout <= 0 {
		w.HandlerTimeout = 5 * time.Minute
	}
	if w.BaseBackoff <= 0 {
		w.BaseBackoff = time.Second
	}
	if w.MaxBackoff <= 0 {
		w.MaxBackoff = 5 * time.Minute
	}
	if w.ReaperInterval <= 0 {
		w.ReaperInterval = 30 * time.Second
	}
	if w.StaleThreshold <= 0 {
		w.StaleThreshold = 5 * time.Minute
	}
}

// concurrency returns the total concurrency implied by the queue config
func (c *Config) concurrency() int {
	total := 0
	for _, q := range c.Queues {
		total += q.MaxWorkers
	}
	if total <= 0 {
		total = 5
	}
	return total
}

// --- functional options ---

func WithQueueConfig(name string, qc QueueConfig) Option {
	return optionFunc(func(c *Config) {
		if c.Queues == nil {
			c.Queues = make(map[string]QueueConfig)
		}
		c.Queues[name] = qc
	})
}
