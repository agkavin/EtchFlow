package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
func (s *LogStore) Append(ctx context.Context, runID string, nodeName, eventType, message string, metadata map[string]any) error {
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

// GetLogsForRun fetches all logs for a run, ordered by creation time.
func (s *LogStore) GetLogsForRun(ctx context.Context, runID string) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, node_name, event_type, message, metadata, created_at
		FROM agent_logs
		WHERE run_id = $1
		ORDER BY created_at ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("get logs: %w", err)
	}
	defer rows.Close()

	var logs []map[string]any
	for rows.Next() {
		var id string
		var nodeName, eventType, message *string
		var metadata []byte
		var createdAt time.Time

		if err := rows.Scan(&id, &nodeName, &eventType, &message, &metadata, &createdAt); err != nil {
			return nil, fmt.Errorf("scan log: %w", err)
		}

		var metadataMap map[string]any
		if metadata != nil {
			if err := json.Unmarshal(metadata, &metadataMap); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}

		log := map[string]any{
			"id":          id,
			"node_name":   nodeName,
			"event_type":  eventType,
			"message":     message,
			"metadata":    metadataMap,
			"created_at":  createdAt,
		}
		logs = append(logs, log)
	}

	return logs, nil
}
