package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open creates a pgx connection pool for the given PostgreSQL URL.
// maxConns sets the maximum number of connections in the pool.
func Open(ctx context.Context, databaseURL string, maxConns int32, minConns int32) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, errors.New("missing database URL")
	}
	if maxConns < 1 {
		maxConns = 24
	}
	if minConns < 1 {
		minConns = 2
	}

	config, _ := pgxpool.ParseConfig(databaseURL)
	config.MaxConns = maxConns
	config.MinConns = minConns
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
