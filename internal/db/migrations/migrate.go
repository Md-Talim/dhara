package migrations

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var migrationFilenamePattern = regexp.MustCompile(`^\d{6}_[a-z0-9_]+\.sql$`)

func RunMigrations(ctx context.Context, db *pgxpool.Pool, logger *slog.Logger, migrationsDir string) error {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "migrations", "dir", migrationsDir)

	start := time.Now()
	logger.Info("starting migration run")

	conn, err := db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Session-level advisory lock
	lockName := "dhara:migrations"
	logger.Debug("acquiring advisory lock", "lock_name", lockName)

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockName); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	logger.Debug("advisory lock acquired", "lock_name", lockName)

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := conn.Exec(cleanupCtx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockName); err != nil {
			logger.Warn("failed to release advisory lock", "lock_name", lockName, "err", err)
			return
		}
		logger.Debug("advisory lock released", "lock_name", lockName)
	}()

	// Ensure migrations tracking table exists
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	migrationFiles, err := listSQLMigrations(migrationsDir)
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	logger.Debug("migration files discovered", "count", len(migrationFiles))

	// Read already-applied versions from DB
	applied, err := loadAppliedMigrations(ctx, conn)
	if err != nil {
		return err
	}
	logger.Debug("loaded applied migrations", "count", len(applied))

	// Strict validation: DB versions must all exists on disk
	if err = validateAppliedVersionsExistsOnDisk(applied, migrationFiles); err != nil {
		return err
	}

	appliedNow := 0
	skipped := 0

	// Execute only pending migrations, each in its own transaction
	for _, filename := range migrationFiles {
		if _, ok := applied[filename]; ok {
			skipped++
			logger.Debug("skipping already-applied migration", "version", filename)
			continue
		}

		fullPath := filepath.Join(migrationsDir, filename)
		sqlBytes, err := os.ReadFile(fullPath)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", filename, err)
		}

		logger.Info("applying migration", "version", filename)
		migrationStart := time.Now()

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", filename, err)
		}

		committed := false
		func() {
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if !committed {
					if rbErr := tx.Rollback(cleanupCtx); rbErr != nil && rbErr != pgx.ErrTxClosed {
						logger.Warn("rollback after failed migration step returned error", "version", filename, "err", rbErr)
					}
				}
			}()

			if _, err = tx.Exec(ctx, string(sqlBytes)); err != nil {
				return
			}

			if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, filename); err != nil {
				return
			}

			if err = tx.Commit(ctx); err != nil {
				return
			}
			committed = true
		}()

		if err != nil {
			return fmt.Errorf("apply migration %s: %w", filename, err)
		}

		appliedNow++
		logger.Info("migration applied", "version", filename, "duration_ms", time.Since(migrationStart).Milliseconds())
	}

	logger.Info(
		"migration run complete",
		"applied", appliedNow,
		"skipped", skipped,
		"total_files", len(migrationFiles),
		"duration_ms", time.Since(start).Milliseconds(),
	)

	return nil
}

func listSQLMigrations(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]string) // normalized -> original
	var out []string

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".sql") {
			continue
		}

		if !migrationFilenamePattern.MatchString(name) {
			return nil, fmt.Errorf(
				"invalid migration filename %q in %s: expected format %q (example: 001_create_tasks.sql)",
				name, dir, `NNN_snake_case.sql`,
			)
		}

		normalized := strings.ToLower(name)
		if prev, ok := seen[normalized]; ok {
			return nil, fmt.Errorf("duplicate migration filename detected (case-insensitive): %q and %q in %s", prev, name, dir)
		}

		seen[normalized] = name
		out = append(out, name)
	}

	sort.Strings(out)
	return out, nil
}

func loadAppliedMigrations(ctx context.Context, conn *pgxpool.Conn) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("read applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]struct{})

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan applied version: %w", err)
		}
		applied[version] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied versions: %w", err)
	}
	return applied, nil
}

func validateAppliedVersionsExistsOnDisk(appliedMigrations map[string]struct{}, migrationFiles []string) error {
	onDisk := make(map[string]struct{}, len(migrationFiles))
	for _, f := range migrationFiles {
		onDisk[f] = struct{}{}
	}

	var missing []string
	for migration := range appliedMigrations {
		if _, ok := onDisk[migration]; !ok {
			missing = append(missing, migration)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			`migration history mismatch: applied version(s) missing on disk: %s.
			Migration filenames are immutable version IDs; do not rename or delete applied migration files.`,
			strings.Join(missing, ", "),
		)
	}

	return nil
}
