package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LogStore handles append-only writes to the agent_logs table.
// Logs are never updated and never deleted (within a run's lifetime).
type LogStore struct {
	pool *pgxpool.Pool
}

// NewLogStore creates a new LogStore.
func NewLogStore(pool *pgxpool.Pool) *LogStore {
	return &LogStore{pool: pool}
}

// Append writes an audit event to agent_logs.
// nodeName is empty ("") for run-level events (SUBMITTED, SUCCESS, etc.).
// metadata is optional — pass nil if not needed.
//
// Log failures do NOT propagate as errors by design: a logging failure should
// never block a checkpoint write. Callers are responsible for handling this.
func (s *LogStore) Append(ctx context.Context, runID uuid.UUID, nodeName, eventType, message string, metadata map[string]any) error {
	var metadataJSON []byte
	var err error

	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal log metadata: %w", err)
		}
	}

	var nodeNamePtr *string
	if nodeName != "" {
		nodeNamePtr = &nodeName
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO agent_logs (run_id, node_name, event_type, message, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`, runID, nodeNamePtr, eventType, message, metadataJSON)
	if err != nil {
		return fmt.Errorf("append log: %w", err)
	}

	return nil
}
