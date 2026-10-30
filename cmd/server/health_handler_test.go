package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/md-talim/dhara/internal/db"
	"github.com/md-talim/dhara/internal/testutil"
)

func TestLiveness_AlwaysReturns200(t *testing.T) {
	h := newHealthHandler(time.Now(), nil) // liveness must not touch the DB

	rr := httptest.NewRecorder()
	h.checkLiveness(rr, httptest.NewRequest(http.MethodGet, "/api/v1/livez", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp healthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want ok", resp.Status)
	}
}

func TestReadiness_DBPoolClosed_Returns503(t *testing.T) {
	// Lazily created, then closed: Ping can never succeed. No DB required.
	pool, err := pgxpool.New(context.Background(),
		"postgres://dhara:dhara@127.0.0.1:1/dhara_test?sslmode=disable")
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	pool.Close()

	h := newHealthHandler(time.Now(), pool)
	rr := httptest.NewRecorder()
	h.checkReadiness(rr, httptest.NewRequest(http.MethodGet, "/api/v1/readyz", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body: %s)", rr.Code, rr.Body.String())
	}
	var resp healthResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "fail" {
		t.Errorf("status = %q, want fail", resp.Status)
	}
	dbCheck, ok := resp.Checks["db"].(map[string]any)
	if !ok {
		t.Fatalf("checks.db = %#v, want an object", resp.Checks["db"])
	}
	if dbCheck["status"] != "fail" {
		t.Errorf("checks.db.status = %v, want fail", dbCheck["status"])
	}
}

func TestReadiness_HealthyDB_Returns200(t *testing.T) {
	testutil.SkipIfNoTestDB(t)

	ctx := context.Background()
	pool, err := db.Open(ctx, testutil.TestDBURL(), 5, 1)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(pool.Close)

	h := newHealthHandler(time.Now(), pool)
	rr := httptest.NewRecorder()
	h.checkReadiness(rr, httptest.NewRequest(http.MethodGet, "/api/v1/readyz", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
}
