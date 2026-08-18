package config

import "errors"

type DatabaseConfig struct {
	URL      string
	MaxConns int
	MinConns int
}

func NewDatabaseConfig() *DatabaseConfig {
	return &DatabaseConfig{
		URL:      getEnvString("DHARA_DATABASE_URL", ""),
		MaxConns: getEnvInt("DHARA_MAX_CONNS", 10),
		MinConns: getEnvInt("DHARA_MIN_CONNS", 2),
	}
}

func (c *DatabaseConfig) Validate() error {
	if c.URL == "" {
		return errors.New("DHARA_DATABASE_URL must be set")
	}
	if c.MaxConns < 1 {
		return errors.New("DHARA_MAX_CONNS must be >= 1")
	}
	if c.MinConns < 1 {
		return errors.New("DHARA_MIN_CONNS must be >= 1")
	}
	if c.MinConns > c.MaxConns {
		return errors.New("DHARA_MIN_CONNS must be <= DHARA_MAX_CONNS")
	}
	return nil
}
