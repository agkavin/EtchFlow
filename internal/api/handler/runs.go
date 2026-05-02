package handler

import (
	"encoding/json"
	"net/http"

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
	if req.GraphDefinition.FinishPoint == "" {
		respondError(w, http.StatusBadRequest, "Bad Request", "graph_definition.finish_point is required")
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
