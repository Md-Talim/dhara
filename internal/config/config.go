package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Database   *DatabaseConfig
	Migrations *MigrationsConfig
	Server     *ServerConfig
	Shutdown   *ShutdownConfig
	Worker     *WorkerConfig
	Logging    *LoggingConfig
}

func NewFromEnv() (*Config, error) {
	c := &Config{
		Database:   NewDatabaseConfig(),
		Migrations: NewMigrationsConfig(),
		Server:     NewServerConfig(),
		Shutdown:   NewShutdownConfig(),
		Worker:     NewWorkerConfig(),
		Logging:    NewLoggingConfig(),
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Config) Validate() error {
	return Load(c.Database, c.Migrations, c.Server, c.Shutdown, c.Worker, c.Logging)
}

// Load validates the given config sections, returning the first error found.
// Nil sections are skipped, so callers can validate partial sets.
func Load(sections ...interface{ Validate() error }) error {
	for _, s := range sections {
		if s == nil {
			continue
		}
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// --- env helpers shared by all sections ---

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

// getEnvDuration accepts either Go duration string (e.g. "30s") or a
// plain integer interpreted as seconds.
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
