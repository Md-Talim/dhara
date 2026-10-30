package config

import (
	"testing"
	"time"
)

func TestDatabaseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *DatabaseConfig)
		wantErr bool
	}{
		{"valid", func(c *DatabaseConfig) {}, false},
		{"missing URL", func(c *DatabaseConfig) { c.URL = "" }, true},
		{"max conns zero", func(c *DatabaseConfig) { c.MaxConns = 0 }, true},
		{"min conns zero", func(c *DatabaseConfig) { c.MinConns = 0 }, true},
		{"min > max", func(c *DatabaseConfig) { c.MaxConns, c.MinConns = 5, 10 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &DatabaseConfig{URL: "postgres://x", MaxConns: 10, MinConns: 2}
			tt.mutate(c)
			if gotErr := c.Validate() != nil; gotErr != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", c.Validate(), tt.wantErr)
			}
		})
	}
}

func TestWorkerConfig_Validate(t *testing.T) {
	valid := &WorkerConfig{
		WorkerPrefix:      "w",
		WorkerCount:       3,
		PollInterval:      time.Second,
		HeartbeatInterval: 30 * time.Second,
		HandlerTimeout:    5 * time.Minute,
		StuckThreshold:    5 * time.Minute,
		ReaperInterval:    30 * time.Second,
		BaseBackoff:       time.Second,
		MaxBackoff:        5 * time.Minute,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("baseline WorkerConfig failed Validate: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(c *WorkerConfig)
	}{
		{"empty prefix", func(c *WorkerConfig) { c.WorkerPrefix = "" }},
		{"zero workers", func(c *WorkerConfig) { c.WorkerCount = 0 }},
		{"zero poll interval", func(c *WorkerConfig) { c.PollInterval = 0 }},
		{"zero heartbeat interval", func(c *WorkerConfig) { c.HeartbeatInterval = 0 }},
		{"zero handler timeout", func(c *WorkerConfig) { c.HandlerTimeout = 0 }},
		{"stuck threshold not above heartbeat", func(c *WorkerConfig) {
			c.HeartbeatInterval = 30 * time.Second
			c.StuckThreshold = 30 * time.Second // must be strictly greater
		}},
		{"stuck threshold below heartbeat", func(c *WorkerConfig) {
			c.HeartbeatInterval = 30 * time.Second
			c.StuckThreshold = 10 * time.Second
		}},
		{"zero reaper interval", func(c *WorkerConfig) { c.ReaperInterval = 0 }},
		{"reaper slower than stuck threshold", func(c *WorkerConfig) {
			c.StuckThreshold = time.Minute
			c.ReaperInterval = 2 * time.Minute
		}},
		{"base backoff zero", func(c *WorkerConfig) { c.BaseBackoff = 0 }},
		{"base backoff above max", func(c *WorkerConfig) { c.BaseBackoff, c.MaxBackoff = 10*time.Minute, time.Minute }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := *valid
			tt.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Error("Validate() = nil, want error")
			}
		})
	}
}

func TestLoggingConfig_Validate(t *testing.T) {
	tests := []struct {
		name, level, format string
		wantErr             bool
	}{
		{"valid defaults", "info", "text", false},
		{"json format", "debug", "json", false},
		{"warning alias normalized", "warning", "text", false},
		{"uppercase accepted", "INFO", "JSON", false},
		{"invalid level", "verbose", "text", true},
		{"invalid format", "info", "logfmt", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &LoggingConfig{Level: tt.level, Format: tt.format}
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && tt.level == "warning" && c.Level != "warn" {
				t.Errorf("level = %q, want normalized %q", c.Level, "warn")
			}
		})
	}
}

func TestServerConfig_Validate(t *testing.T) {
	if err := (&ServerConfig{Port: 8080}).Validate(); err != nil {
		t.Errorf("valid port rejected: %v", err)
	}
	if err := (&ServerConfig{Port: 0}).Validate(); err == nil {
		t.Error("port 0 accepted")
	}
}

func TestShutdownConfig_Validate(t *testing.T) {
	if err := (&ShutdownConfig{ShutdownTimeout: time.Second}).Validate(); err != nil {
		t.Errorf("valid timeout rejected: %v", err)
	}
	if err := (&ShutdownConfig{ShutdownTimeout: 0}).Validate(); err == nil {
		t.Error("zero timeout accepted")
	}
}

func TestLoad_SkipsNilSections(t *testing.T) {
	if err := Load(nil, &ServerConfig{Port: 8080}, nil); err != nil {
		t.Errorf("Load() with nil sections = %v, want nil", err)
	}
}

func TestGetEnvDuration_PlainIntegerMeansSeconds(t *testing.T) {
	t.Setenv("DHARA_TEST_DURATION", "45")
	if got := getEnvDuration("DHARA_TEST_DURATION", time.Second); got != 45*time.Second {
		t.Errorf("getEnvDuration(plain int) = %v, want 45s", got)
	}

	t.Setenv("DHARA_TEST_DURATION", "45s")
	if got := getEnvDuration("DHARA_TEST_DURATION", time.Second); got != 45*time.Second {
		t.Errorf("getEnvDuration(duration string) = %v, want 45s", got)
	}

	// Invalid values fall back to the default.
	t.Setenv("DHARA_TEST_DURATION", "banana")
	if got := getEnvDuration("DHARA_TEST_DURATION", 7*time.Second); got != 7*time.Second {
		t.Errorf("getEnvDuration(invalid) = %v, want default 7s", got)
	}
}

func TestWorkerConfig_ValidateMetricsPort(t *testing.T) {
	cfg := func(port int) *WorkerConfig {
		return &WorkerConfig{
			WorkerPrefix:      "w",
			WorkerCount:       1,
			PollInterval:      time.Second,
			HeartbeatInterval: 30 * time.Second,
			HandlerTimeout:    time.Minute,
			StuckThreshold:    5 * time.Minute,
			ReaperInterval:    30 * time.Second,
			BaseBackoff:       time.Second,
			MaxBackoff:        time.Minute,
			MetricsPort:       port,
		}
	}

	if err := cfg(9090).Validate(); err != nil {
		t.Errorf("valid port rejected: %v", err)
	}
	if err := cfg(0).Validate(); err != nil {
		t.Errorf("0 (disabled) rejected: %v", err)
	}
	if err := cfg(-1).Validate(); err == nil {
		t.Error("-1 accepted")
	}
	if err := cfg(70000).Validate(); err == nil {
		t.Error("70000 accepted")
	}
}
