package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	WorkerPrefix      string
	WorkerCount       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration

	StuckThreshold time.Duration
	ReaperInterval time.Duration

	ShutdownTimeout time.Duration
	Port            int

	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	LogLevel  string
	LogFormat string

	AutoMigrate   bool
	MigrationsDir string
}

func NewFromEnv() (*Config, error) {
	c := &Config{
		WorkerPrefix:      getEnvString("WORKER_PREFIX", "dhara-worker"),
		WorkerCount:       getEnvInt("WORKER_COUNT", 5),
		PollInterval:      getEnvDuration("POLL_INTERVAL", time.Second),
		HeartbeatInterval: getEnvDuration("HEARTBEAT_INTERVAL", 30*time.Second),
		HandlerTimeout:    getEnvDuration("HANDLER_TIMEOUT", 5*time.Minute),

		StuckThreshold: getEnvDuration("STUCK_THRESHOLD", 5*time.Minute),
		ReaperInterval: getEnvDuration("REAPER_INTERVAL", 30*time.Second),

		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		Port:            getEnvInt("PORT", 8080),

		BaseBackoff: getEnvDuration("BASE_BACKOFF", time.Second),
		MaxBackoff:  getEnvDuration("MAX_BACKOFF", 5*time.Minute),

		LogLevel:  getEnvString("LOG_LEVEL", "info"),
		LogFormat: getEnvString("LOG_FORMAT", "text"),

		AutoMigrate:   getEnvBool("AUTO_MIGRATE", true),
		MigrationsDir: getEnvString("MIGRATIONS_DIR", "internal/db/migrations"),
	}

	// basic validations
	if c.WorkerPrefix == "" {
		return nil, fmt.Errorf("WORKER_PREFIX must not be empty")
	}
	if c.WorkerCount < 1 {
		return nil, fmt.Errorf("WORKER_COUNT must be >= 1")
	}
	if c.PollInterval <= 0 {
		return nil, fmt.Errorf("POLL_INTERVAL must be > 0")
	}
	if c.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("HEARBEAT_INTERVAL must be > 0")
	}
	if c.HandlerTimeout <= 0 {
		return nil, fmt.Errorf("HANDLER_TIMEOUT must be > 0")
	}
	if c.StuckThreshold <= c.HeartbeatInterval {
		return nil, fmt.Errorf("STUCK_THRESHOLD must be more than HEARTBEAT_INTERVAL")
	}
	if c.ReaperInterval <= 0 {
		return nil, fmt.Errorf("REAPER_INTERVAL must be > 0")
	}
	if c.ReaperInterval > c.StuckThreshold {
		return nil, fmt.Errorf("REAPER_INTERVAL (%s) should be <= STUCK_THRESHOLD (%s)", c.ReaperInterval, c.StuckThreshold)
	}
	if c.BaseBackoff <= 0 || c.MaxBackoff <= 0 {
		return nil, fmt.Errorf("BASE_BACKOFF and MAX_BACKOFF must be > 0")
	}
	if c.BaseBackoff > c.MaxBackoff {
		return nil, fmt.Errorf("BASE_BACKOFF (%s) must be <= MAX_BACKOFF (%s)", c.BaseBackoff, c.MaxBackoff)
	}
	if c.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0")
	}
	if c.Port <= 0 {
		return nil, fmt.Errorf("PORT must be > 0")
	}
	if c.AutoMigrate && c.MigrationsDir == "" {
		return nil, fmt.Errorf("MIGRATIONS_DIR must be set when AUTO_MIGRATE is true")
	}

	switch strings.ToLower(c.LogFormat) {
	case "text", "json":
		c.LogFormat = strings.ToLower(c.LogFormat)
	default:
		return nil, fmt.Errorf("invalid LOG_FORMAT: %s", c.LogFormat)
	}

	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
		c.LogLevel = strings.ToLower(c.LogLevel)
	case "warning":
		c.LogLevel = "warn"
	default:
		return nil, fmt.Errorf("invalid LOG_LEVEL: %s", c.LogLevel)
	}

	return c, nil
}

func getEnvString(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultValue
}

// getEnvDuration accepts either Go duration string (e.g. "30s") or a plain integer interpreted as seconds.
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return time.Duration(i) * time.Second
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultValue
}
