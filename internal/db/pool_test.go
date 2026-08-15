package db_test

import (
	"context"
	"testing"

	"github.com/md-talim/dhara/internal/db"
)

func TestOpen_Success(t *testing.T) {
	ctx := context.Background()
	pool, err := db.Open(ctx, "postgres://dhara:dhara@localhost:5433/dhara_test?sslmode=disable")
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	pool.Close()
}

func TestOpen_MissingEnv(t *testing.T) {
	ctx := context.Background()
	pool, err := db.Open(ctx, "")
	if err == nil {
		pool.Close()
		t.Fatalf("expected success, got error: %v", err)
	}
}
