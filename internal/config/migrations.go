package config

// MigrationsConfig controls automatic migrations at binary startup.
type MigrationsConfig struct {
	AutoMigrate bool
}

func NewMigrationsConfig() *MigrationsConfig {
	return &MigrationsConfig{
		AutoMigrate: getEnvBool("AUTO_MIGRATE", true),
	}
}

func (c *MigrationsConfig) Validate() error {
	return nil
}
