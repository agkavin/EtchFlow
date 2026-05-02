package models

import (
	"time"

	"github.com/google/uuid"
)

// Checkpoint represents a single node's committed state within a run.
// One row per completed node per run in the `checkpoints` table.
// The UNIQUE(run_id, node_name) constraint ensures idempotency.
type Checkpoint struct {
	ID        uuid.UUID      `json:"id" db:"id"`
	RunID     string         `json:"run_id" db:"run_id"`
	NodeName  string         `json:"node_name" db:"node_name"`
	StateJSON map[string]any `json:"state" db:"state_json"`
	CreatedAt time.Time      `json:"created_at" db:"created_at"`
}
