package main

import (
	"encoding/json"
	"net/http"
)

// envelope is a helper type for wrapping JSON responses, e.g., {"error": "..."}
type envelope map[string]any

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, envelope{"error": message})
}

func writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
