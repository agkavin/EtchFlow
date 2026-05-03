package handler

import (
	"net/http"
)

// Health handles GET /health
// Returns a simple liveness response. Does NOT check the database.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": "0.1.0-mvp",
	})
}


