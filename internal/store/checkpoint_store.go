package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore handles all database operations for the checkpoints table.
type CheckpointStore struct {
	pool *pgxpool.Pool
}

// NewCheckpointStore creates a new CheckpointStore.
func NewCheckpointStore(pool *pgxpool.Pool) *CheckpointStore {
	return &CheckpointStore{pool: pool}
}

// saveCheckpointTx inserts a checkpoint inside an existing transaction.
// Returns created=true if a new row was inserted, created=false if the checkpoint
// already existed (ON CONFLICT DO NOTHING — idempotency guarantee).
//
// This must be called inside a transaction that also updates runs.current_state.
// Both operations commit together or both roll back — that's the durability guarantee.
func (s *CheckpointStore) saveCheckpointTx(ctx context.Context, tx pgx.Tx, runID uuid.UUID, nodeName string, stateJSON []byte) (bool, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `
		INSERT INTO checkpoints (run_id, node_name, state_json)
		VALUES ($1, $2, $3)
		ON CONFLICT ON CONSTRAINT uq_checkpoint_run_node DO NOTHING
		RETURNING id
	`, runID, nodeName, stateJSON).Scan(&id)

	if err == pgx.ErrNoRows {
		// Conflict: checkpoint already exists. This is idempotent — not an error.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert checkpoint: %w", err)
	}

	return true, nil
}

// GetLastCheckpoint fetches the most recent checkpoint for a run.
// Returns nil, nil if no checkpoints exist yet (fresh run).
func (s *CheckpointStore) GetLastCheckpoint(ctx context.Context, runID uuid.UUID) (map[string]any, string, error) {
	var stateJSON []byte
	var nodeName string

	err := s.pool.QueryRow(ctx, `
		SELECT node_name, state_json
		FROM checkpoints
		WHERE run_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, runID).Scan(&nodeName, &stateJSON)

	if err == pgx.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("get last checkpoint: %w", err)
	}

	var state map[string]any
	if err := json.Unmarshal(stateJSON, &state); err != nil {
		return nil, "", fmt.Errorf("unmarshal checkpoint state: %w", err)
	}

	return state, nodeName, nil
}
