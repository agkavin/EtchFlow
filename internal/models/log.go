package models

import (
	"time"

	"github.com/google/uuid"
)

// Event type constants for agent_logs.
// MVP subset — Phase 1.5 will add CLAIMED, REAPED, RETRYING, DEAD, CANCELLED, TIMEOUT.
const (
	EventSubmitted     = "SUBMITTED"
	EventNodeCompleted = "NODE_COMPLETED"
	EventSuccess       = "SUCCESS"
	EventFailed        = "FAILED"
)

// AgentLog is an append-only audit record.
// Written on every state transition and checkpoint. Never updated, never deleted.
type AgentLog struct {
	ID        uuid.UUID      `json:"id" db:"id"`
	RunID     string         `json:"run_id" db:"run_id"`
	NodeName  string         `json:"node_name,omitempty" db:"node_name"`
	EventType string         `json:"event_type" db:"event_type"`
	Message   string         `json:"message,omitempty" db:"message"`
	Metadata  map[string]any `json:"metadata,omitempty" db:"metadata"`
	CreatedAt time.Time      `json:"created_at" db:"created_at"`
}
