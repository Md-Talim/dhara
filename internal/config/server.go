package config

import (
	"errors"
	"time"
)

// ServerConfig holds the HTTP API server settings.
type ServerConfig struct {
	Port int
}

func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Port: getEnvInt("PORT", 8080),
	}
}

func (c *ServerConfig) Validate() error {
	if c.Port <= 0 {
		return errors.New("PORT must be > 0")
	}
	return nil
}

// ShutdownConfig holds the graceful-shutdown drain timeout shared by the API
// server and the worker process.
type ShutdownConfig struct {
	ShutdownTimeout time.Duration
}

func NewShutdownConfig() *ShutdownConfig {
	return &ShutdownConfig{

		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func (c *ShutdownConfig) Validate() error {
	if c.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT must be > 0")
	}
	return nil
}
