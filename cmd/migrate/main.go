package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/md-talim/dhara"
	"github.com/md-talim/dhara/internal/config"
	"github.com/md-talim/dhara/internal/db"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("migration failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	args := os.Args[1:]

	direction := "up"
	steps := 1
	if len(args) > 0 {
		switch args[0] {
		case "up":
		case "down":
			direction = "down"
			if len(args) > 1 {
				n, err := strconv.Atoi(args[1])
				if err != nil || n < 1 {
					return fmt.Errorf("invalid step count %q: must be a positive integer", args[1])
				}
				steps = n
			}
		default:
			return fmt.Errorf("unknown command %q\n\nusage:\n  dhara-migrate [up]      apply all pending migrations (default)\n  dhara-migrate down [N]  roll back N migrations (default 1)", args[0])
		}
	}

	dbCfg := config.NewDatabaseConfig()
	if err := config.Load(dbCfg); err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := db.Open(ctx, dbCfg.URL, int32(dbCfg.MaxConns), int32(dbCfg.MinConns))
	if err != nil {
		return fmt.Errorf("failed to create db pool: %w", err)
	}
	defer pool.Close()

	mctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if direction == "up" {
		res, err := dhara.Migrate(mctx, pool)
		if err != nil {
			return err
		}
		logger.Info("migration run complete",
			"applied", res.Versions,
			"skipped", res.Skipped,
		)
		return nil
	}

	res, err := dhara.MigrateDown(mctx, pool, steps)
	if err != nil {
		return err
	}
	logger.Info("rollback run complete",
		"rolled_back", res.Versions,
	)
	return nil
}
