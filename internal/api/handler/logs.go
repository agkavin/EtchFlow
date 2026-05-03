package handler

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// GetLogs handles GET /runs/{id}/logs.
// Returns the audit trail for a run.
func (h *Handlers) GetLogs(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	if runID == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "run ID is required")
		return
	}

	// First verify the run exists
	_, err := h.store.Runs.GetRun(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) {
		respondError(w, http.StatusNotFound, "Not Found", "Run not found")
		return
	}
	if err != nil {
		h.logger.Error("failed to get run", zap.Error(err), zap.String("run_id", runID))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to fetch run")
		return
	}

	logs, err := h.store.Logs.GetLogsForRun(r.Context(), runID)
	if err != nil {
		h.logger.Error("failed to get logs", zap.Error(err), zap.String("run_id", runID))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to fetch logs")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"run_id": runID,
		"logs":   logs,
		"total":  len(logs),
	})
}