package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/db/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	pool, err := db.Open(ctx)
	if err != nil {
		logger.Error("failed to create db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "internal/db/migrations"
	}

	mctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if err := migrations.RunMigrations(mctx, pool, logger, migrationsDir); err != nil {
		logger.Error("migration run failed", "err", err)
		os.Exit(1)
	}

	logger.Info("migration run complete")
}
