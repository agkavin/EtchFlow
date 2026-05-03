package handler

import (
	"context"
	"net/http"
	"time"
)

// Ready handles GET /ready.
// Returns 200 if the service is ready (Postgres is reachable).
// This is different from /health which just checks the service is running.
func (h *Handlers) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.store.Ping(ctx); err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":  "unavailable",
			"error":   "database not reachable",
			"detail":   err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
	})
}