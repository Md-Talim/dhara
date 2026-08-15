package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/vow"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	dbCfg := config.NewDatabaseConfig()
	migCfg := config.NewMigrationsConfig()
	if err := config.Load(dbCfg, migCfg); err != nil {
		logger.Error("invalid config", "err", err)
		os.Exit(1)
	}

	pool, err := db.Open(ctx, dbCfg.URL)
	if err != nil {
		logger.Error("failed to create db pool", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	migrator, err := vow.New(pool, os.DirFS("migrations"),
		vow.WithTableName("dhara_vow_migrations"),
		vow.WithLockName("dhara_vow_lock"),
		vow.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create migrator", "err", err)
		os.Exit(1)
	}

	mctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	result, err := migrator.Up(mctx)
	if err != nil {
		logger.Error("migration run failed", "err", err, "applied", result.Versions)
		os.Exit(1)
	}

	logger.Info("migration run complete",
		"applied", result.Versions,
		"skipped", result.Skipped,
		"duration", result.Duration,
	)
}
