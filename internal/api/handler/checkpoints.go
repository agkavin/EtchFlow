package handler

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// GetCheckpoints handles GET /runs/{id}/checkpoints.
// Returns the full checkpoint history for a run, used by LangGraph's list() method.
func (h *Handlers) GetCheckpoints(w http.ResponseWriter, r *http.Request) {
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

	checkpoints, err := h.store.Checkpoints.GetCheckpointsForRun(r.Context(), runID)
	if err != nil {
		h.logger.Error("failed to get checkpoints", zap.Error(err), zap.String("run_id", runID))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to fetch checkpoints")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"run_id":      runID,
		"checkpoints": checkpoints,
		"total":       len(checkpoints),
	})
}