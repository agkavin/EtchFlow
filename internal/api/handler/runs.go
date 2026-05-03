package handler

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/marcusferl/etchflow/internal/models"
	"go.uber.org/zap"
)

// createRunRequest is the expected body for POST /runs.
type createRunRequest struct {
	ID              string                 `json:"id,omitempty"`
	GraphDefinition models.GraphDefinition `json:"graph_definition"`
	InputData       map[string]any         `json:"input_data"`
}

// CreateRun handles POST /runs.
//
// Registers a new LangGraph DAG run. Python calls this once, then immediately
// starts graph.invoke(). EtchFlow does NOT trigger Python — Python triggers itself.
//
// Response 201: { "run_id": "...", "status": "PENDING", "created_at": "..." }
func (h *Handlers) CreateRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "Invalid JSON body: "+err.Error())
		return
	}

	// Validate graph_definition
	if len(req.GraphDefinition.Nodes) == 0 {
		respondError(w, http.StatusBadRequest, "Bad Request", "graph_definition.nodes must not be empty")
		return
	}
	if req.GraphDefinition.EntryPoint == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "graph_definition.entry_point is required")
		return
	}


	// Validate input_data
	if len(req.InputData) == 0 {
		respondError(w, http.StatusBadRequest, "Bad Request", "input_data must not be empty")
		return
	}

	run, err := h.store.Runs.CreateRun(r.Context(), req.ID, req.GraphDefinition, req.InputData)
	if err != nil {
		h.logger.Error("failed to create run", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to create run")
		return
	}

	// Append SUBMITTED event to audit log. Log failure does not block the response.
	if logErr := h.store.Logs.Append(r.Context(), run.ID, "", models.EventSubmitted, "run created by python client", nil); logErr != nil {
		h.logger.Warn("failed to append SUBMITTED log", zap.Error(logErr), zap.String("run_id", run.ID))
	}

	h.logger.Info("run created", zap.String("run_id", run.ID), zap.String("status", run.Status))

	respondJSON(w, http.StatusCreated, map[string]any{
		"run_id":     run.ID,
		"status":     run.Status,
		"created_at": run.CreatedAt,
	})
}

// ClaimNextRun handles POST /runs/claim.
func (h *Handlers) ClaimNextRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "Invalid JSON body: "+err.Error())
		return
	}
	if req.WorkerID == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "worker_id is required")
		return
	}

	run, err := h.store.Runs.ClaimNextRun(r.Context(), req.WorkerID)
	if err != nil {
		if err.Error() == "run not found" {
			respondError(w, http.StatusNotFound, "Not Found", "No pending runs available")
			return
		}
		h.logger.Error("failed to claim next run", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to claim run")
		return
	}

	// Log CLAIMED
	_ = h.store.Logs.Append(r.Context(), run.ID, "", models.EventClaimed, "run claimed by worker", map[string]any{"worker_id": req.WorkerID})

	respondJSON(w, http.StatusOK, run)
}

// GetRun handles GET /runs/{id}.
func (h *Handlers) GetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	run, err := h.store.Runs.GetRun(r.Context(), runID)
	if err != nil {
		if err.Error() == "run not found" {
			respondError(w, http.StatusNotFound, "Not Found", "Run not found")
			return
		}
		h.logger.Error("failed to get run", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to get run")
		return
	}
	respondJSON(w, http.StatusOK, run)
}

// UpdateHeartbeat handles PUT /runs/{id}/heartbeat.
func (h *Handlers) UpdateHeartbeat(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	err := h.store.Runs.UpdateHeartbeat(r.Context(), runID)
	if err != nil {
		if err.Error() == "run not found" {
			respondError(w, http.StatusNotFound, "Not Found", "Run not found or not in RUNNING state")
			return
		}
		h.logger.Error("failed to update heartbeat", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to update heartbeat")
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// FailRun handles POST /runs/{id}/fail.
func (h *Handlers) FailRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	var req struct {
		Error string `json:"error"`
		Fatal bool   `json:"fatal"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Bad Request", "Invalid JSON body: "+err.Error())
		return
	}

	err := h.store.Runs.FailRun(r.Context(), runID, req.Error, req.Fatal)
	if err != nil {
		if err.Error() == "run not found" {
			respondError(w, http.StatusNotFound, "Not Found", "Run not found")
			return
		}
		h.logger.Error("failed to mark run failed", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to fail run")
		return
	}

	// Log FAILED
	_ = h.store.Logs.Append(r.Context(), runID, "", models.EventFailed, req.Error, map[string]any{"fatal": req.Fatal})

	respondJSON(w, http.StatusOK, map[string]string{"status": "failed"})
}

// CancelRun handles POST /runs/{id}/cancel.
func (h *Handlers) CancelRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	
	// Fast path update for cancel
	found, err := h.store.Runs.CancelRun(r.Context(), runID)
	if err != nil {
		h.logger.Error("failed to cancel run", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to cancel run")
		return
	}
	if !found {
		respondError(w, http.StatusNotFound, "Not Found", "Run not found")
		return
	}

	// Log CANCELLED
	_ = h.store.Logs.Append(r.Context(), runID, "", "CANCELLED", "run cancelled via api", nil)

	respondJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// CompleteRun handles POST /runs/{id}/complete.
func (h *Handlers) CompleteRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")

	err := h.store.Runs.CompleteRun(r.Context(), runID)
	if err != nil {
		if err.Error() == "run not found" {
			respondError(w, http.StatusNotFound, "Not Found", "Run not found or already in terminal state")
			return
		}
		h.logger.Error("failed to complete run", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Internal Server Error", "Failed to complete run")
		return
	}

	// Log SUCCESS
	_ = h.store.Logs.Append(r.Context(), runID, "", models.EventSuccess, "run completed successfully via explicit signal", nil)

	respondJSON(w, http.StatusOK, map[string]string{"status": "success"})
}

