package dhara

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/vow"
)

//go:embed migrations/*.sql
var migrationsRoot embed.FS

// MigrationFS exposes the migrations embedded in the library, rooted at the
// migrations directory (flat NNNNNN_name.up.sql / .down.sql files).
var MigrationFS fs.FS = mustSub(migrationsRoot, "migrations")

// MigrationResult reports what a migration run changed
type MigrationResult struct {
	Versions []string
	Skipped  int
}

// Migrate applies all pending migrations using embedded migration files
func Migrate(ctx context.Context, pool *pgxpool.Pool) (MigrationResult, error) {
	return runMigrations(ctx, pool, MigrationFS, true, 0)
}

// MigrateDown rolls back the given number of migrations using the embedded
// files. Each rollback runs the matching .down.sql
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, steps int) (MigrationResult, error) {
	return runMigrations(ctx, pool, MigrationFS, false, steps)
}

// MigrateWith applies pending migrations from a custom FS (e.g. your own
// os.DirFS when you maintain your own migration files).
func MigrateWith(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) (MigrationResult, error) {
	return runMigrations(ctx, pool, fsys, true, 0)
}

// MigrateDownWith rolls back migrations from a custom FS.
func MigrateDownWith(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, steps int) (MigrationResult, error) {
	return runMigrations(ctx, pool, fsys, false, steps)
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, up bool, steps int) (MigrationResult, error) {
	migrator, err := vow.New(pool, fsys,
		vow.WithTableName("dhara_vow_migrations"),
		vow.WithLockName("dhara_vow_lock"),
	)
	if err != nil {
		return MigrationResult{}, fmt.Errorf("dhara: create migrator: %w", err)
	}

	var res vow.Result
	if up {
		res, err = migrator.Up(ctx)
	} else {
		res, err = migrator.Down(ctx, steps)
	}
	if err != nil {
		return MigrationResult{}, err
	}
	return MigrationResult{Versions: res.Versions, Skipped: res.Skipped}, nil
}

func mustSub(root embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(root, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
