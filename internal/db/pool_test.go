package db_test

import (
	"context"
	"sync"
	"testing"

	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/testutil"
)

func TestOpen_Success(t *testing.T) {
	testutil.SkipIfNoTestDB(t)
	ctx := context.Background()
	pool, err := db.Open(ctx, testutil.TestDBURL(), 2, 1)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	pool.Close()
}

func TestOpen_MissingURL(t *testing.T) {
	ctx := context.Background()
	pool, err := db.Open(ctx, "", 2, 1)
	if err == nil {
		pool.Close()
		t.Fatal("expected error for empty database URL, got nil")
	}
}

func TestOpen_MalformedURL(t *testing.T) {
	// Regression test: Open used to swallow the ParseConfig error, which
	// panicked on a nil config instead of returning a clean error.
	ctx := context.Background()
	pool, err := db.Open(ctx, "://not a url", 2, 1)
	if err == nil {
		pool.Close()
		t.Fatal("expected error for malformed database URL, got nil")
	}
	if pool != nil {
		t.Fatal("Open() returned a non-nil pool together with an error")
	}
}

func TestOpen_InvalidConnCountsFallBackToDefaults(t *testing.T) {
	testutil.SkipIfNoTestDB(t)
	// maxConns/minConns < 1 fall back to sane defaults instead of failing.
	ctx := context.Background()
	pool, err := db.Open(ctx, testutil.TestDBURL(), 0, 0)
	if err != nil {
		t.Fatalf("expected success with fallback defaults, got error: %v", err)
	}
	defer pool.Close()

	stat := pool.Stat()
	if stat.MaxConns() != 10 {
		t.Errorf("MaxConns = %d, want fallback 10", stat.MaxConns())
	}
}

// Fails under -race if Open races on shared config defaults.
func TestOpen_Concurrent(t *testing.T) {
	testutil.SkipIfNoTestDB(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool, err := db.Open(ctx, testutil.TestDBURL(), 2, 1)
			if err == nil {
				pool.Close()
			}
		}()
	}
	wg.Wait()
}
