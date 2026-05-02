package models

import (
	"time"
)

// Run status constants — MVP subset only.
// Phase 1.5 will add: RETRYING, DEAD, CANCELLED, TIMEOUT
const (
	StatusPending = "PENDING"
	StatusRunning = "RUNNING"
	StatusSuccess = "SUCCESS"
	StatusFailed  = "FAILED"
)

// GraphDefinition holds the DAG topology submitted by Python.
// EtchFlow stores this as metadata — it does NOT use it to drive execution.
// Python drives its own execution. EtchFlow records what happened.
type GraphDefinition struct {
	Nodes       []string         `json:"nodes"`
	Edges       []GraphEdge      `json:"edges"`
	EntryPoint  string           `json:"entry_point"`
	FinishPoint string           `json:"finish_point"`
}

// GraphEdge represents a directed edge in the DAG.
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Run represents a single workflow execution lifecycle.
// One row per workflow execution in the `runs` table.
type Run struct {
	ID                 string          `json:"run_id" db:"id"`
	GraphDefinition    GraphDefinition `json:"graph_definition" db:"graph_definition"`
	InputData          map[string]any  `json:"input_data" db:"input_data"`
	CurrentState       map[string]any  `json:"current_state,omitempty" db:"current_state"`
	Status             string          `json:"status" db:"status"`
	LastNodeCompleted  string          `json:"last_node_completed,omitempty" db:"last_node_completed"`
	CreatedAt          time.Time       `json:"created_at" db:"created_at"`
	StartedAt          *time.Time      `json:"started_at,omitempty" db:"started_at"`
	CompletedAt        *time.Time      `json:"completed_at,omitempty" db:"completed_at"`
	UpdatedAt          time.Time       `json:"updated_at" db:"updated_at"`
}
