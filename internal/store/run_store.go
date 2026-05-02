package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcusferl/etchflow/internal/models"
)

// ErrNotFound is returned when a run does not exist.
var ErrNotFound = errors.New("run not found")

// RunStore handles all database operations for the runs table.
type RunStore struct {
	pool *pgxpool.Pool
}

// NewRunStore creates a new RunStore.
func NewRunStore(pool *pgxpool.Pool) *RunStore {
	return &RunStore{pool: pool}
}

// CreateRun inserts a new run into the database with PENDING status.
// Returns the created Run with its generated UUID (or provided ID).
func (s *RunStore) CreateRun(ctx context.Context, id string, graphDef models.GraphDefinition, inputData map[string]any) (*models.Run, error) {
	graphDefJSON, err := json.Marshal(graphDef)
	if err != nil {
		return nil, fmt.Errorf("marshal graph_definition: %w", err)
	}
	inputDataJSON, err := json.Marshal(inputData)
	if err != nil {
		return nil, fmt.Errorf("marshal input_data: %w", err)
	}

	var run models.Run
	var graphDefRaw []byte
	var inputDataRaw []byte

	err = s.pool.QueryRow(ctx, `
		INSERT INTO runs (id, graph_definition, input_data)
		VALUES (COALESCE(NULLIF($1, ''), gen_random_uuid()::text), $2, $3)
		RETURNING id, graph_definition, input_data, current_state, status,
		          COALESCE(last_node_completed, ''), created_at, started_at, completed_at, updated_at
	`, id, graphDefJSON, inputDataJSON).Scan(
		&run.ID,
		&graphDefRaw,
		&inputDataRaw,
		nil, // current_state is NULL on creation
		&run.Status,
		&run.LastNodeCompleted,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert run: %w", err)
	}

	if err := json.Unmarshal(graphDefRaw, &run.GraphDefinition); err != nil {
		return nil, fmt.Errorf("unmarshal graph_definition: %w", err)
	}
	if err := json.Unmarshal(inputDataRaw, &run.InputData); err != nil {
		return nil, fmt.Errorf("unmarshal input_data: %w", err)
	}

	return &run, nil
}

// GetRun fetches a run by ID. Returns ErrNotFound if it doesn't exist.
func (s *RunStore) GetRun(ctx context.Context, id string) (*models.Run, error) {
	var run models.Run
	var graphDefRaw []byte
	var inputDataRaw []byte
	var currentStateRaw []byte

	err := s.pool.QueryRow(ctx, `
		SELECT id, graph_definition, input_data, current_state, status,
		       COALESCE(last_node_completed, ''), created_at, started_at, completed_at, updated_at
		FROM runs
		WHERE id = $1
	`, id).Scan(
		&run.ID,
		&graphDefRaw,
		&inputDataRaw,
		&currentStateRaw,
		&run.Status,
		&run.LastNodeCompleted,
		&run.CreatedAt,
		&run.StartedAt,
		&run.CompletedAt,
		&run.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}

	if err := json.Unmarshal(graphDefRaw, &run.GraphDefinition); err != nil {
		return nil, fmt.Errorf("unmarshal graph_definition: %w", err)
	}
	if err := json.Unmarshal(inputDataRaw, &run.InputData); err != nil {
		return nil, fmt.Errorf("unmarshal input_data: %w", err)
	}
	if currentStateRaw != nil {
		if err := json.Unmarshal(currentStateRaw, &run.CurrentState); err != nil {
			return nil, fmt.Errorf("unmarshal current_state: %w", err)
		}
	}

	return &run, nil
}

// updateCurrentStateTx updates the run's current_state and last_node_completed within a transaction.
// Called inside the atomic checkpoint transaction — do not call standalone.
func (s *RunStore) updateCurrentStateTx(ctx context.Context, tx pgx.Tx, id string, stateJSON []byte, lastNode string) error {
	_, err := tx.Exec(ctx, `
		UPDATE runs
		SET current_state = $1, last_node_completed = $2, updated_at = NOW()
		WHERE id = $3
	`, stateJSON, lastNode, id)
	if err != nil {
		return fmt.Errorf("update current_state: %w", err)
	}
	return nil
}

// setRunningTx transitions the run from PENDING → RUNNING within a transaction.
// Uses COALESCE so started_at is only set on first transition.
// Safe to call even if already RUNNING (WHERE status = 'PENDING' ensures idempotency).
func (s *RunStore) setRunningTx(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    started_at = COALESCE(started_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1 AND status = 'PENDING'
	`, id)
	if err != nil {
		return fmt.Errorf("set running: %w", err)
	}
	return nil
}

// setSuccessTx transitions the run from RUNNING → SUCCESS within a transaction.
// Called when the finish_point node is checkpointed.
func (s *RunStore) setSuccessTx(ctx context.Context, tx pgx.Tx, id string) error {
	now := time.Now()
	_, err := tx.Exec(ctx, `
		UPDATE runs
		SET status = 'SUCCESS',
		    completed_at = $1,
		    updated_at = NOW()
		WHERE id = $2 AND status = 'RUNNING'
	`, now, id)
	if err != nil {
		return fmt.Errorf("set success: %w", err)
	}
	return nil
}
