package api

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HealthHandler struct {
	start             time.Time
	DB                *pgxpool.Pool
	IsMigrationsReady func() bool
	IsWorkerReady     func() bool
}

func NewHealthHandler(start time.Time, db *pgxpool.Pool) *HealthHandler {
	return &HealthHandler{start: start, DB: db, IsMigrationsReady: nil, IsWorkerReady: nil}
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

func (h *HealthHandler) CheckLiveness(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		UptimeS: int64(time.Since(h.start).Seconds()),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *HealthHandler) CheckReadiness(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{
		Status:  "ok",
		UptimeS: int64(time.Since(h.start).Seconds()),
		Checks:  make(map[string]any),
	}

	overallFail := false

	dbStart := time.Now()
	dbCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	dbErr := h.DB.Ping(dbCtx)
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

	if h.IsMigrationsReady != nil {
		s := "ok"
		if !h.IsMigrationsReady() {
			s = "fail"
			overallFail = true
		}
		resp.Checks["migrations"] = checkResult{Status: s}
	}

	if h.IsWorkerReady != nil {
		s := "ok"
		if !h.IsWorkerReady() {
			s = "fail"
			overallFail = true
		}
		resp.Checks["worker"] = checkResult{Status: s}
	}

	if overallFail {
		resp.Status = "fail"
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
