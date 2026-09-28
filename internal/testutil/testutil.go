// Package testutil provides shared helpers for dhara's test suites.
//
// It is a normal internal package (not an import-restricted _test package) so
// that test binaries in any package can reuse the same setup logic. Nothing
// in here ships to library users.
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Type aliases so call sites read testutil.T instead of testing.T while
// keeping signatures compatible with the stdlib.
type (
	TB = testing.TB
	T  = testing.T
	B  = testing.B
	M  = testing.M
)

// TestDBURL returns the database URL for DB-backed tests. It first checks
// DHARA_TEST_DATABASE_URL, then DATABASE_URL (as set by run-tests.sh), and
// finally falls back to the conventional local default used by the project.
func TestDBURL() string {
	if v := os.Getenv("DHARA_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable"
}

// SkipIfNoTestDB skips the current test when DB-backed tests cannot run:
// either the caller passed -short (fast local loop) or no test database URL
// is configured. DB tests are opt-in per invocation, never accidental.
func SkipIfNoTestDB(t TB) {
	if testing.Short() {
		t.Skip("skipping DB-backed test in -short mode")
	}
	if os.Getenv("DHARA_TEST_DATABASE_URL") == "" {
		t.Skip("no DHARA_TEST_DATABASE_URL set; skipping DB-backed test")
	}
}

// Eventually polls cond until it returns true or the timeout elapses,
// retrying every interval. On timeout it fails the test and stops it, so
// call sites can assert inline without extra nil checks:
//
//	testutil.Eventually(t, 5*time.Second, 10*time.Millisecond, func() bool {
//		task, _ := store.GetById(ctx, id)
//		return task != nil && task.Status == "RUNNING"
//	})
//
// All wait loops in this project use this helper instead of time.Sleep so
// tests are as fast as possible yet immune to slow CI machines.
func Eventually(t TB, timeout, interval time.Duration, cond func() bool) {
	t.Helper()
	t.Log("waiting for condition...")
	if !poll(timeout, interval, cond) {
		t.Fatalf("condition not met within %v", timeout)
	}
	t.Log("condition met")
}

// EventuallyTiming measures how long cond took to become true and fails the
// test if it exceeded budget. Use it to assert deadlines ("reaper recovered
// the task within X") rather than just eventual success.
func EventuallyTiming(t TB, timeout, interval time.Duration, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	if !poll(timeout, interval, cond) {
		t.Fatalf("condition not met within %v", timeout)
	}
	elapsed := time.Since(start)
	if elapsed > timeout {
		t.Fatalf("condition met after %v, exceeded budget of %v", elapsed, timeout)
	}
	return elapsed
}

// Never ensures cond stays false for the full window. Use for negative
// controls, e.g. "a healthy heartbeating task must NOT be reaped".
func Never(t TB, window, interval time.Duration, cond func() bool) {
	t.Helper()
	if poll(window, interval, cond) {
		t.Fatalf("condition became true within %v, but it must never hold", window)
	}
}

func poll(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// UniqueKey returns an idempotency key unique to this test run, so parallel
// or repeated runs never collide on the (type, idempotency_key) constraint:
//
//	key := testutil.UniqueKey(t, "order") // e.g. "order-TestX-1712345678901234567"
func UniqueKey(t TB, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%s-%d", prefix, t.Name(), time.Now().UnixNano())
}

// MustTruncate wipes all queue tables between tests. It fails the test on
// error (unlike a bare Exec, which would silently leave rows behind and
// produce confusing cross-test failures).
func MustTruncate(ctx context.Context, t TB, db DB) {
	t.Helper()
	_, err := db.Exec(ctx, `TRUNCATE TABLE dead_letters, task_logs, tasks RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

// CleanupTruncate registers a t.Cleanup that truncates after the test.
func CleanupTruncate(ctx context.Context, t TB, db DB) {
	t.Cleanup(func() {
		t.Helper()
		MustTruncate(ctx, t, db)
	})
}

// DB is the minimal surface testutil needs from a pool: Exec. It matches
// *pgxpool.Pool exactly, but keeping it an interface avoids importing pgxpool
// (and keeps the helper honest about how little it actually needs).
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}
