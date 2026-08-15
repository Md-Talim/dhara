package config

import "errors"

type DatabaseConfig struct {
	URL string
}

func NewDatabaseConfig() *DatabaseConfig {
	return &DatabaseConfig{
		URL: getEnvString("DARA_DATABASE_URL", ""),
	}
}

func (c *DatabaseConfig) Validate() error {
	if c.URL == "" {
		return errors.New("DHARA_DATABASE_URL must be set")
	}
	return nil
}
