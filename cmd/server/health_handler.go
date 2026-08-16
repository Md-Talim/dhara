package main

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type healthHandler struct {
	start time.Time
	db    *pgxpool.Pool
}

func newHealthHandler(start time.Time, db *pgxpool.Pool) *healthHandler {
	return &healthHandler{start: start, db: db}
}

type checkResult struct {
	Status    string `json:"status"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
}

type healthResponse struct {
	Status  string         `json:"status"`
	UptimeS int64          `json:"uptime_s"`
	Checks  map[string]any `json:"checks,omitempty"`
}

func (h *healthHandler) checkLiveness(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		UptimeS: int64(time.Since(h.start).Seconds()),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *healthHandler) checkReadiness(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		UptimeS: int64(time.Since(h.start).Seconds()),
		Checks:  make(map[string]any),
	}

	overallFail := false

	dbStart := time.Now()
	dbCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	dbErr := h.db.Ping(dbCtx)
	cancel()

	dbCheck := checkResult{
		Status:    "ok",
		LatencyMS: time.Since(dbStart).Milliseconds(),
	}
	if dbErr != nil {
		dbCheck.Status = "fail"
		overallFail = true
	}
	resp.Checks["db"] = dbCheck

	if overallFail {
		resp.Status = "fail"
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
