package config

import "errors"

type MigrationsConfig struct {
	AutoMigrate   bool
	MigrationsDir string
}

func NewMigrationsConfig() *MigrationsConfig {
	return &MigrationsConfig{
		AutoMigrate:   getEnvBool("AUTO_MIGRATE", true),
		MigrationsDir: getEnvString("MIGRATIONS_DIR", "migrations"),
	}
}

func (c *MigrationsConfig) Validate() error {
	if c.AutoMigrate && c.MigrationsDir == "" {
		return errors.New("MIGRATIONS_DIR must be set when AUTO_MIGRATE is true")
	}
	return nil
}
