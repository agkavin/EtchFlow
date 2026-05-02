package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/marcusferl/etchflow/internal/models"
	"github.com/marcusferl/etchflow/internal/store"
	"go.uber.org/zap"
)

// saveCheckpointRequest is the expected body for PUT /runs/{id}/checkpoint.
type saveCheckpointRequest struct {
	NodeName string         `json:"node_name"`
	State    map[string]any `json:"state"`
}

// SaveCheckpoint handles PUT /runs/{id}/checkpoint.
//
// This is the most important endpoint. Called by EtchFlowCheckpointSaver.put()
// after every node completes. Atomically:
//   - Inserts checkpoint (ON CONFLICT DO NOTHING — idempotent)
//   - Updates runs.current_state and last_node_completed
//   - Auto-transitions PENDING → RUNNING on first checkpoint
//   - Transitions RUNNING → SUCCESS on finish_point node
//
// Response 200 (continue): { "continue": true, "halt_reason": null }
// Response 200 (done):     { "continue": false, "halt_reason": null }
func (h *Handlers) SaveCheckpoint(w http.ResponseWriter, r *http.Request) {
	runIDStr := chi.URLParam(r, "id")
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "Invalid run ID: must be a UUID")
		return
	}

	var req saveCheckpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "Invalid JSON body: "+err.Error())
		return
	}

	if req.NodeName == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "node_name is required")
		return
	}
	if req.State == nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "state must not be null")
		return
	}

	// Fetch current run to know its status and finish_point
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

	// Guard: only accept checkpoints for runs that are in a live state
	if run.Status == models.StatusSuccess || run.Status == models.StatusFailed {
		respondError(w, http.StatusConflict, "Conflict",
			"Run "+runIDStr+" is in terminal state: "+run.Status+". No further checkpoints accepted.")
		return
	}

	// The core operation: atomic checkpoint + state update + status transition
	result, err := h.store.AtomicCheckpoint(r.Context(), run, req.NodeName, req.State)
	if err != nil {
		h.logger.Error("atomic checkpoint failed",
			zap.Error(err),
			zap.String("run_id", runIDStr),
			zap.String("node_name", req.NodeName))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to save checkpoint")
		return
	}

	// Append audit log events (non-blocking — log failure should not fail the checkpoint)
	if result.WasNew {
		if logErr := h.store.Logs.Append(r.Context(), runID, req.NodeName, models.EventNodeCompleted,
			"node: "+req.NodeName, nil); logErr != nil {
			h.logger.Warn("failed to append NODE_COMPLETED log", zap.Error(logErr))
		}

		if !result.Continue {
			if logErr := h.store.Logs.Append(r.Context(), runID, "", models.EventSuccess,
				"run completed successfully", nil); logErr != nil {
				h.logger.Warn("failed to append SUCCESS log", zap.Error(logErr))
			}
		}
	}

	h.logger.Info("checkpoint saved",
		zap.String("run_id", runIDStr),
		zap.String("node_name", req.NodeName),
		zap.Bool("was_new", result.WasNew),
		zap.Bool("continue", result.Continue))

	respondJSON(w, http.StatusOK, map[string]any{
		"continue":    result.Continue,
		"halt_reason": nil,
	})
}
