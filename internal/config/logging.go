package config

import (
	"fmt"
	"strings"
)

// LoggingConfig holds logging output settings.
type LoggingConfig struct {
	Level  string
	Format string
}

func NewLoggingConfig() *LoggingConfig {
	return &LoggingConfig{
		Level:  getEnvString("LOG_LEVEL", "info"),
		Format: getEnvString("LOG_FORMAT", "text"),
	}
}

func (c *LoggingConfig) Validate() error {
	switch strings.ToLower(c.Format) {
	case "text", "json":
		c.Format = strings.ToLower(c.Format)
	default:
		return fmt.Errorf("invalid LOG_FORMAT: %s", c.Format)
	}

	switch strings.ToLower(c.Level) {
	case "debug", "info", "warn", "error":
		c.Level = strings.ToLower(c.Level)
	case "warning":
		c.Level = "warn"
	default:
		return fmt.Errorf("invalid LOG_LEVEL: %s", c.Level)
	}

	return nil
}
