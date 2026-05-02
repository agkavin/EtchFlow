package handler

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// GetState handles GET /runs/{id}/state.
//
// Called by EtchFlowCheckpointSaver.get_tuple() on graph.invoke() start.
// This is the crash recovery mechanism: Python loads the last committed
// checkpoint so LangGraph can skip already-completed nodes.
//
// Response 200: { "run_id": "...", "last_node_completed": "...", "state": {...}, "checkpointed_at": "..." }
// Response 404: no checkpoint exists yet (fresh start — LangGraph starts from entry_point)
func (h *Handlers) GetState(w http.ResponseWriter, r *http.Request) {
	runIDStr := chi.URLParam(r, "id")
	runID := runIDStr
	if runID == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "run ID is required")
		return
	}

	run, err := h.store.Runs.GetRun(r.Context(), runID)
	if errors.Is(err, store.ErrNotFound) {
		respondError(w, http.StatusNotFound, "Not Found",
			"Run "+runIDStr+" not found")
		return
	}
	if err != nil {
		h.logger.Error("failed to get run", zap.Error(err), zap.String("run_id", runIDStr))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to fetch run")
		return
	}

	// No checkpoint yet → 404 so Python knows to start fresh
	if run.CurrentState == nil {
		respondError(w, http.StatusNotFound, "No Checkpoint Found",
			"Run "+runIDStr+" has no committed checkpoints. Start from the beginning.")
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"run_id":              run.ID,
		"last_node_completed": run.LastNodeCompleted,
		"state":               run.CurrentState,
		"checkpointed_at":     run.UpdatedAt,
	})
}
