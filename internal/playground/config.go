package playground

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config controls the public playground. Defaults are intentionally tuned for
// a responsive demonstration: short poll/heartbeat windows, bounded batches,
// and a dedicated database schema.
type Config struct {
	DatabaseURL string
	MaxConns    int32
	MinConns    int32

	Port            int
	Schema          string
	CrashSchema     string
	AutoMigrate     bool
	ShutdownTimeout time.Duration
	TrustProxy      bool

	WorkerPrefix      string
	WorkerCount       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	StuckThreshold    time.Duration
	ReaperInterval    time.Duration
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration

	MaxBatch        int
	MaxBodyBytes    int64
	MaxPayloadBytes int
	MaxPending      int

	RateLimitPerMinute     int
	RateLimitBurst         int
	SessionRatePerMinute   int
	SessionRateBurst       int
	ReadRateLimitPerMin    int
	ReadRateLimitBurst     int
	CleanupInterval        time.Duration
	Retention              time.Duration
	EnableCrashDemo        bool
	CrashDemoTimeout       time.Duration
	CrashTaskDuration      time.Duration
	CrashHeartbeatInterval time.Duration
	CrashStuckThreshold    time.Duration
	CrashReaperInterval    time.Duration
	CrashHandlerTimeout    time.Duration
	CrashWorkerPrefix      string
	CrashPollInterval      time.Duration

	LogLevel  string
	LogFormat string
}

var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// LoadConfig reads playground configuration from the environment. It reuses
// the same DHARA_* and worker variable names as the production binaries, but
// applies playground-specific defaults where a snappy demo needs them.
func LoadConfig() (*Config, error) {
	cfg := loadConfigRaw()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadConfigRaw reads the environment without applying cross-field validation.
// The crash worker reuses it because it runs with a single schema environment
// (its schema equals its crash schema by construction).
func loadConfigRaw() *Config {
	return &Config{
		DatabaseURL: getEnvString("DHARA_DATABASE_URL", ""),
		MaxConns:    int32(getEnvInt("DHARA_MAX_CONNS", 10)),
		MinConns:    int32(getEnvInt("DHARA_MIN_CONNS", 2)),

		Port:            getEnvInt("PORT", 8080),
		Schema:          getEnvString("PLAYGROUND_DB_SCHEMA", "dhara_playground"),
		CrashSchema:     getEnvString("PLAYGROUND_CRASH_SCHEMA", "dhara_playground_crash"),
		AutoMigrate:     getEnvBool("AUTO_MIGRATE", true),
		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		TrustProxy:      getEnvBool("PLAYGROUND_TRUST_PROXY", false),

		WorkerPrefix:      getEnvString("WORKER_PREFIX", "playground-worker"),
		WorkerCount:       getEnvInt("WORKER_COUNT", 4),
		PollInterval:      getEnvDuration("POLL_INTERVAL", 250*time.Millisecond),
		HeartbeatInterval: getEnvDuration("HEARTBEAT_INTERVAL", 2*time.Second),
		HandlerTimeout:    getEnvDuration("HANDLER_TIMEOUT", 12*time.Second),
		StuckThreshold:    getEnvDuration("STUCK_THRESHOLD", 10*time.Second),
		ReaperInterval:    getEnvDuration("REAPER_INTERVAL", 2*time.Second),
		BaseBackoff:       getEnvDuration("BASE_BACKOFF", time.Second),
		MaxBackoff:        getEnvDuration("MAX_BACKOFF", 30*time.Second),

		MaxBatch:        getEnvInt("PLAYGROUND_MAX_BATCH", 50),
		MaxBodyBytes:    int64(getEnvInt("PLAYGROUND_MAX_BODY_BYTES", 16*1024)),
		MaxPayloadBytes: getEnvInt("PLAYGROUND_MAX_PAYLOAD_BYTES", 4*1024),
		MaxPending:      getEnvInt("PLAYGROUND_MAX_PENDING", 500),

		RateLimitPerMinute:   getEnvInt("PLAYGROUND_RATE_LIMIT_PER_MIN", 30),
		RateLimitBurst:       getEnvInt("PLAYGROUND_RATE_LIMIT_BURST", 10),
		SessionRatePerMinute: getEnvInt("PLAYGROUND_SESSION_RATE_LIMIT_PER_MIN", 20),
		SessionRateBurst:     getEnvInt("PLAYGROUND_SESSION_RATE_LIMIT_BURST", 8),
		ReadRateLimitPerMin:  getEnvInt("PLAYGROUND_READ_RATE_LIMIT_PER_MIN", 180),
		ReadRateLimitBurst:   getEnvInt("PLAYGROUND_READ_RATE_LIMIT_BURST", 40),

		CleanupInterval:        getEnvDuration("PLAYGROUND_CLEANUP_INTERVAL", 5*time.Minute),
		Retention:              getEnvDuration("PLAYGROUND_TASK_RETENTION", time.Hour),
		EnableCrashDemo:        getEnvBool("PLAYGROUND_ENABLE_CRASH_DEMO", true),
		CrashDemoTimeout:       getEnvDuration("PLAYGROUND_CRASH_DEMO_TIMEOUT", 35*time.Second),
		CrashTaskDuration:      getEnvDuration("PLAYGROUND_CRASH_TASK_DURATION", 6*time.Second),
		CrashHeartbeatInterval: getEnvDuration("PLAYGROUND_CRASH_HEARTBEAT_INTERVAL", time.Second),
		CrashStuckThreshold:    getEnvDuration("PLAYGROUND_CRASH_STUCK_THRESHOLD", 4*time.Second),
		CrashReaperInterval:    getEnvDuration("PLAYGROUND_CRASH_REAPER_INTERVAL", time.Second),
		CrashHandlerTimeout:    getEnvDuration("PLAYGROUND_CRASH_HANDLER_TIMEOUT", 30*time.Second),
		CrashWorkerPrefix:      getEnvString("PLAYGROUND_CRASH_WORKER_PREFIX", "playground-crash-worker"),
		CrashPollInterval:      getEnvDuration("PLAYGROUND_CRASH_POLL_INTERVAL", 150*time.Millisecond),

		LogLevel:  getEnvString("LOG_LEVEL", "info"),
		LogFormat: getEnvString("LOG_FORMAT", "text"),
	}
}

func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return errors.New("DHARA_DATABASE_URL must be set")
	}
	if c.MaxConns < 1 || c.MinConns < 1 || c.MinConns > c.MaxConns {
		return errors.New("DHARA_MAX_CONNS and DHARA_MIN_CONNS must be >= 1 and min <= max")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("PORT must be between 1 and 65535")
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT must be positive")
	}
	if err := validateSchema(c.Schema); err != nil {
		return fmt.Errorf("PLAYGROUND_DB_SCHEMA: %w", err)
	}
	if err := validateSchema(c.CrashSchema); err != nil {
		return fmt.Errorf("PLAYGROUND_CRASH_SCHEMA: %w", err)
	}
	if c.Schema == c.CrashSchema {
		return errors.New("PLAYGROUND_DB_SCHEMA and PLAYGROUND_CRASH_SCHEMA must differ")
	}
	if c.WorkerPrefix == "" {
		return errors.New("WORKER_PREFIX must not be empty")
	}
	if c.WorkerCount < 1 || c.WorkerCount > 32 {
		return errors.New("WORKER_COUNT must be between 1 and 32")
	}
	if c.PollInterval <= 0 || c.HeartbeatInterval <= 0 || c.HandlerTimeout <= 0 {
		return errors.New("POLL_INTERVAL, HEARTBEAT_INTERVAL and HANDLER_TIMEOUT must be > 0")
	}
	if c.StuckThreshold <= c.HeartbeatInterval {
		return errors.New("STUCK_THRESHOLD must be greater than HEARTBEAT_INTERVAL")
	}
	if c.ReaperInterval <= 0 || c.ReaperInterval > c.StuckThreshold {
		return errors.New("REAPER_INTERVAL must be > 0 and <= STUCK_THRESHOLD")
	}
	if c.BaseBackoff <= 0 || c.MaxBackoff <= 0 || c.BaseBackoff > c.MaxBackoff {
		return errors.New("BASE_BACKOFF and MAX_BACKOFF must be > 0 and base <= max")
	}
	if c.MaxBatch < 1 || c.MaxBatch > 200 {
		return errors.New("PLAYGROUND_MAX_BATCH must be between 1 and 200")
	}
	if c.MaxPayloadBytes < 2 || c.MaxPayloadBytes > 64*1024 {
		return errors.New("PLAYGROUND_MAX_PAYLOAD_BYTES must be between 2 and 65536")
	}
	if c.MaxBodyBytes < int64(c.MaxPayloadBytes) || c.MaxBodyBytes > 1<<20 {
		return errors.New("PLAYGROUND_MAX_BODY_BYTES must be >= max payload and <= 1 MiB")
	}
	if c.MaxPending < 1 || c.MaxPending > 10000 {
		return errors.New("PLAYGROUND_MAX_PENDING must be between 1 and 10000")
	}
	if c.RateLimitPerMinute < 1 || c.RateLimitBurst < 1 {
		return errors.New("PLAYGROUND_RATE_LIMIT_PER_MIN and BURST must be >= 1")
	}
	if c.SessionRatePerMinute < 1 || c.SessionRateBurst < 1 {
		return errors.New("PLAYGROUND_SESSION_RATE_LIMIT_PER_MIN and BURST must be >= 1")
	}
	if c.ReadRateLimitPerMin < 1 || c.ReadRateLimitBurst < 1 {
		return errors.New("PLAYGROUND_READ_RATE_LIMIT_PER_MIN and BURST must be >= 1")
	}
	if c.CleanupInterval <= 0 || c.Retention <= 0 {
		return errors.New("PLAYGROUND_CLEANUP_INTERVAL and PLAYGROUND_TASK_RETENTION must be > 0")
	}
	if c.EnableCrashDemo {
		if c.CrashSchema == "public" || c.CrashSchema == "pg_catalog" || c.CrashSchema == "information_schema" {
			return errors.New("PLAYGROUND_CRASH_SCHEMA must be a dedicated schema")
		}
		if c.CrashWorkerPrefix == "" || c.CrashDemoTimeout <= 0 || c.CrashTaskDuration <= 0 {
			return errors.New("crash demo settings must be positive")
		}
		if c.CrashDemoTimeout <= c.CrashStuckThreshold {
			return errors.New("PLAYGROUND_CRASH_DEMO_TIMEOUT must be greater than PLAYGROUND_CRASH_STUCK_THRESHOLD")
		}
		if c.CrashTaskDuration > 60*time.Second {
			return errors.New("PLAYGROUND_CRASH_TASK_DURATION must be 60s or less")
		}
		if c.CrashDemoTimeout <= c.CrashTaskDuration+c.CrashStuckThreshold {
			return errors.New("PLAYGROUND_CRASH_DEMO_TIMEOUT must exceed the crash task duration plus the stale threshold")
		}
		if c.CrashHandlerTimeout <= c.CrashTaskDuration {
			return errors.New("PLAYGROUND_CRASH_HANDLER_TIMEOUT must be greater than PLAYGROUND_CRASH_TASK_DURATION")
		}
		if c.CrashHeartbeatInterval <= 0 || c.CrashStuckThreshold <= c.CrashHeartbeatInterval {
			return errors.New("PLAYGROUND_CRASH_STUCK_THRESHOLD must be greater than PLAYGROUND_CRASH_HEARTBEAT_INTERVAL")
		}
		if c.CrashReaperInterval <= 0 || c.CrashHandlerTimeout <= 0 || c.CrashPollInterval <= 0 {
			return errors.New("crash demo intervals must be > 0")
		}
	}
	return nil
}

func validateSchema(schema string) error {
	if !schemaPattern.MatchString(schema) {
		return errors.New("must match [a-z_][a-z0-9_]{0,62}")
	}
	if strings.HasPrefix(schema, "pg_") {
		return errors.New("must not use PostgreSQL's reserved pg_ prefix")
	}
	switch schema {
	case "public", "pg_catalog", "information_schema":
		return fmt.Errorf("%q is not a dedicated playground schema", schema)
	}
	return nil
}

func getEnvString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			return d
		}
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return fallback
}
