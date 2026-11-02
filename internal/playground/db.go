package playground

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara"
)

// ensureSchema creates the dedicated playground schema. The schema name has
// already been validated by Config.Validate, and pgx.Identifier quotes it for
// the CREATE statement.
func ensureSchema(ctx context.Context, databaseURL, schema string) error {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 2
	cfg.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open admin pool: %w", err)
	}
	defer pool.Close()

	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+ident); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}
	return nil
}

// openSchemaPool opens a pool whose connections resolve unqualified table
// names through the dedicated playground schema. This is the isolation
// boundary that lets the playground reuse Dhara's existing queries unchanged.
func openSchemaPool(ctx context.Context, databaseURL, schema string, maxConns, minConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns < 1 {
		maxConns = 10
	}
	if minConns < 1 {
		minConns = 2
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = minConns
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open %s pool: %w", schema, err)
	}
	return pool, nil
}

// PrepareSchema creates and migrates a dedicated schema using Dhara's embedded
// migrations. It is safe to run repeatedly.
func PrepareSchema(ctx context.Context, cfg *Config, schema string) (*pgxpool.Pool, error) {
	if err := ensureSchema(ctx, cfg.DatabaseURL, schema); err != nil {
		return nil, err
	}
	pool, err := openSchemaPool(ctx, cfg.DatabaseURL, schema, cfg.MaxConns, cfg.MinConns)
	if err != nil {
		return nil, err
	}
	if cfg.AutoMigrate {
		mctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		if _, err := dhara.Migrate(mctx, pool); err != nil {
			pool.Close()
			return nil, fmt.Errorf("migrate schema %s: %w", schema, err)
		}
	}
	return pool, nil
}
