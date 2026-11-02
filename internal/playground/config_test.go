package playground

import (
	"testing"
	"time"
)

func testConfig() *Config {
	return &Config{
		DatabaseURL:            "postgres://dhara:dhara@localhost:5432/dhara?sslmode=disable",
		MaxConns:               10,
		MinConns:               2,
		Port:                   8080,
		Schema:                 "dhara_playground_test",
		CrashSchema:            "dhara_playground_crash_test",
		AutoMigrate:            true,
		ShutdownTimeout:        15 * time.Second,
		WorkerPrefix:           "test-worker",
		WorkerCount:            4,
		PollInterval:           250 * time.Millisecond,
		HeartbeatInterval:      2 * time.Second,
		HandlerTimeout:         12 * time.Second,
		StuckThreshold:         10 * time.Second,
		ReaperInterval:         2 * time.Second,
		BaseBackoff:            time.Second,
		MaxBackoff:             30 * time.Second,
		MaxBatch:               50,
		MaxBodyBytes:           16 * 1024,
		MaxPayloadBytes:        4 * 1024,
		MaxPending:             500,
		RateLimitPerMinute:     30,
		RateLimitBurst:         10,
		SessionRatePerMinute:   20,
		SessionRateBurst:       8,
		ReadRateLimitPerMin:    180,
		ReadRateLimitBurst:     40,
		CleanupInterval:        5 * time.Minute,
		Retention:              time.Hour,
		EnableCrashDemo:        true,
		CrashDemoTimeout:       35 * time.Second,
		CrashTaskDuration:      6 * time.Second,
		CrashHeartbeatInterval: time.Second,
		CrashStuckThreshold:    4 * time.Second,
		CrashReaperInterval:    time.Second,
		CrashHandlerTimeout:    30 * time.Second,
		CrashWorkerPrefix:      "test-crash-worker",
		CrashPollInterval:      150 * time.Millisecond,
	}
}

func TestConfigValidate(t *testing.T) {
	if err := testConfig().Validate(); err != nil {
		t.Fatalf("baseline config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing database url", func(c *Config) { c.DatabaseURL = "" }},
		{"invalid schema", func(c *Config) { c.Schema = "Bad-Schema" }},
		{"public schema", func(c *Config) { c.Schema = "public" }},
		{"reserved pg prefix", func(c *Config) { c.Schema = "pg_demo" }},
		{"same schema", func(c *Config) { c.CrashSchema = c.Schema }},
		{"zero workers", func(c *Config) { c.WorkerCount = 0 }},
		{"stuck threshold below heartbeat", func(c *Config) { c.StuckThreshold = c.HeartbeatInterval }},
		{"reaper slower than stuck threshold", func(c *Config) { c.ReaperInterval = c.StuckThreshold + time.Second }},
		{"payload exceeds body", func(c *Config) { c.MaxBodyBytes = 10 }},
		{"zero batch", func(c *Config) { c.MaxBatch = 0 }},
		{"zero retention", func(c *Config) { c.Retention = 0 }},
		{"zero shutdown timeout", func(c *Config) { c.ShutdownTimeout = 0 }},
		{"crash heartbeat not below threshold", func(c *Config) { c.CrashStuckThreshold = c.CrashHeartbeatInterval }},
		{"crash demo timeout too small", func(c *Config) { c.CrashDemoTimeout = c.CrashStuckThreshold }},
		{"crash task too long", func(c *Config) { c.CrashTaskDuration = 61 * time.Second }},
		{"crash demo shorter than task plus reaper", func(c *Config) { c.CrashDemoTimeout = 9 * time.Second }},
		{"crash handler times out first", func(c *Config) { c.CrashHandlerTimeout = c.CrashTaskDuration }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("Validate() = nil, want error")
			}
		})
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("DHARA_DATABASE_URL", "postgres://example")
	t.Setenv("PLAYGROUND_DB_SCHEMA", "env_schema")
	t.Setenv("PLAYGROUND_CRASH_SCHEMA", "env_crash_schema")
	t.Setenv("PLAYGROUND_MAX_BATCH", "12")
	t.Setenv("PLAYGROUND_TASK_RETENTION", "45m")
	t.Setenv("PLAYGROUND_ENABLE_CRASH_DEMO", "false")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	if cfg.Schema != "env_schema" || cfg.CrashSchema != "env_crash_schema" {
		t.Errorf("schemas = %q/%q", cfg.Schema, cfg.CrashSchema)
	}
	if cfg.MaxBatch != 12 {
		t.Errorf("MaxBatch = %d, want 12", cfg.MaxBatch)
	}
	if cfg.Retention != 45*time.Minute {
		t.Errorf("Retention = %v, want 45m", cfg.Retention)
	}
	if cfg.EnableCrashDemo {
		t.Error("EnableCrashDemo = true, want false")
	}
}
