package models

import (
	"time"
)

// Run status constants
const (
	StatusPending   = "PENDING"
	StatusRunning   = "RUNNING"
	StatusSuccess   = "SUCCESS"
	StatusFailed    = "FAILED"
	StatusRetrying  = "RETRYING"
	StatusDead      = "DEAD"
	StatusCancelled = "CANCELLED"
	StatusTimeout   = "TIMEOUT"
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

	// Orchestration fields (Phase 1.5)
	WorkerID        *string    `json:"worker_id,omitempty" db:"worker_id"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty" db:"last_heartbeat_at"`
	AttemptCount    int        `json:"attempt_count" db:"attempt_count"`
	MaxRetries      int        `json:"max_retries" db:"max_retries"`
	BaseDelayMs     int        `json:"base_delay_ms" db:"base_delay_ms"`
	MaxDelayMs      int        `json:"max_delay_ms" db:"max_delay_ms"`
	NextRetryAt     *time.Time `json:"next_retry_at,omitempty" db:"next_retry_at"`
	LastError       *string    `json:"last_error,omitempty" db:"last_error"`
}
