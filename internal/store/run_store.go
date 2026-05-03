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
		          COALESCE(last_node_completed, ''), created_at, started_at, completed_at, updated_at,
		          worker_id, last_heartbeat_at, attempt_count, max_retries, base_delay_ms, max_delay_ms, next_retry_at, last_error
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
		&run.WorkerID,
		&run.LastHeartbeatAt,
		&run.AttemptCount,
		&run.MaxRetries,
		&run.BaseDelayMs,
		&run.MaxDelayMs,
		&run.NextRetryAt,
		&run.LastError,
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
		       COALESCE(last_node_completed, ''), created_at, started_at, completed_at, updated_at,
		       worker_id, last_heartbeat_at, attempt_count, max_retries, base_delay_ms, max_delay_ms, next_retry_at, last_error
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
		&run.WorkerID,
		&run.LastHeartbeatAt,
		&run.AttemptCount,
		&run.MaxRetries,
		&run.BaseDelayMs,
		&run.MaxDelayMs,
		&run.NextRetryAt,
		&run.LastError,
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

// updateStatusTx is a helper to update a run's status inside a transaction.
func (s *RunStore) updateStatusTx(ctx context.Context, tx pgx.Tx, id string, fromStatus, toStatus string, completedAt *time.Time) error {
	var err error
	if completedAt != nil {
		_, err = tx.Exec(ctx, `
			UPDATE runs SET status = $1, completed_at = $2, updated_at = NOW()
			WHERE id = $3 AND status = $4
		`, toStatus, completedAt, id, fromStatus)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE runs SET status = $1, updated_at = NOW()
			WHERE id = $2 AND status = $3
		`, toStatus, id, fromStatus)
	}
	if err != nil {
		return fmt.Errorf("update status %s -> %s: %w", fromStatus, toStatus, err)
	}
	return nil
}

// setRunningTx transitions the run from PENDING → RUNNING within a transaction.
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
func (s *RunStore) setSuccessTx(ctx context.Context, tx pgx.Tx, id string) error {
	now := time.Now()
	return s.updateStatusTx(ctx, tx, id, "RUNNING", "SUCCESS", &now)
}

// CompleteRun transitions a run to SUCCESS. Used for explicit completion report.
func (s *RunStore) CompleteRun(ctx context.Context, id string) error {
	now := time.Now()
	res, err := s.pool.Exec(ctx, `
		UPDATE runs
		SET status = 'SUCCESS',
		    completed_at = $1,
		    updated_at = NOW()
		WHERE id = $2 AND (status = 'RUNNING' OR status = 'PENDING')
	`, now, id)
	if err != nil {
		return fmt.Errorf("complete run: %w", err)
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}



// ClaimNextRun atomically finds the oldest PENDING run, marks it RUNNING,
// and sets the worker_id and last_heartbeat_at.
func (s *RunStore) ClaimNextRun(ctx context.Context, workerID string) (*models.Run, error) {
	var run models.Run
	var graphDefRaw []byte
	var inputDataRaw []byte
	var currentStateRaw []byte

	err := s.pool.QueryRow(ctx, `
		UPDATE runs
		SET status = 'RUNNING',
		    worker_id = $1,
		    started_at = COALESCE(started_at, NOW()),
		    last_heartbeat_at = NOW(),
		    updated_at = NOW()
		WHERE id = (
			SELECT id FROM runs
			WHERE status = 'PENDING'
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, graph_definition, input_data, current_state, status,
		          COALESCE(last_node_completed, ''), created_at, started_at, completed_at, updated_at,
		          worker_id, last_heartbeat_at, attempt_count, max_retries, base_delay_ms, max_delay_ms, next_retry_at, last_error
	`, workerID).Scan(
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
		&run.WorkerID,
		&run.LastHeartbeatAt,
		&run.AttemptCount,
		&run.MaxRetries,
		&run.BaseDelayMs,
		&run.MaxDelayMs,
		&run.NextRetryAt,
		&run.LastError,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("claim next run: %w", err)
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

// UpdateHeartbeat updates the last_heartbeat_at timestamp for a RUNNING run.
func (s *RunStore) UpdateHeartbeat(ctx context.Context, id string) error {
	res, err := s.pool.Exec(ctx, `
		UPDATE runs
		SET last_heartbeat_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND status = 'RUNNING'
	`, id)
	if err != nil {
		return fmt.Errorf("update heartbeat: %w", err)
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// FailRun handles a failure report. It calculates exponential backoff and sets
// the status to RETRYING, or DEAD if fatal is true or max_retries is reached.
func (s *RunStore) FailRun(ctx context.Context, id string, errMsg string, fatal bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var attemptCount, maxRetries, baseDelayMs, maxDelayMs int
	err = tx.QueryRow(ctx, `
		SELECT attempt_count, max_retries, base_delay_ms, max_delay_ms
		FROM runs WHERE id = $1 FOR UPDATE
	`, id).Scan(&attemptCount, &maxRetries, &baseDelayMs, &maxDelayMs)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("get run for fail: %w", err)
	}

	attemptCount++

	if fatal || attemptCount >= maxRetries {
		// Mark DEAD
		_, err = tx.Exec(ctx, `
			UPDATE runs
			SET status = 'DEAD',
			    last_error = $1,
			    attempt_count = $2,
			    worker_id = NULL,
			    updated_at = NOW()
			WHERE id = $3
		`, errMsg, attemptCount, id)
	} else {
		// Calculate backoff: base * (2 ^ (attempt - 1))
		multiplier := 1 << (attemptCount - 1)
		delayMs := baseDelayMs * multiplier
		if delayMs > maxDelayMs {
			delayMs = maxDelayMs
		}
		
		delayStr := fmt.Sprintf("%d milliseconds", delayMs)

		_, err = tx.Exec(ctx, `
			UPDATE runs
			SET status = 'RETRYING',
			    last_error = $1,
			    attempt_count = $2,
			    next_retry_at = NOW() + $3::interval,
			    worker_id = NULL,
			    updated_at = NOW()
			WHERE id = $4
		`, errMsg, attemptCount, delayStr, id)
	}

	if err != nil {
		return fmt.Errorf("update fail state: %w", err)
	}

	return tx.Commit(ctx)
}

// ReapStaleRuns resets RUNNING runs to PENDING if they haven't heartbeated recently.
// Only reaps runs that have worker_id set - meaning a worker claimed them but died
// without releasing the run. Runs without worker_id are orphans to be picked up.
func (s *RunStore) ReapStaleRuns(ctx context.Context, timeout time.Duration) (int64, error) {
	staleThreshold := time.Now().Add(-timeout)
	res, err := s.pool.Exec(ctx, `
		UPDATE runs
		SET status = 'PENDING',
		    worker_id = NULL,
		    updated_at = NOW()
		WHERE status = 'RUNNING' 
		  AND worker_id IS NOT NULL
		  AND last_heartbeat_at < $1
	`, staleThreshold)
	if err != nil {
		return 0, fmt.Errorf("reap stale runs: %w", err)
	}
	return res.RowsAffected(), nil
}

// WakeRetryingRuns flips RETRYING runs back to PENDING if their backoff has expired.
// Returns the number of runs woken.
func (s *RunStore) WakeRetryingRuns(ctx context.Context) (int64, error) {
	res, err := s.pool.Exec(ctx, `
		UPDATE runs
		SET status = 'PENDING',
		    updated_at = NOW()
		WHERE status = 'RETRYING' AND next_retry_at <= NOW()
	`)
	if err != nil {
		return 0, fmt.Errorf("wake retrying runs: %w", err)
	}
	return res.RowsAffected(), nil
}

// CancelRun marks a run as CANCELLED.
// Returns (true, nil) if found and cancelled, (false, nil) if not found.
func (s *RunStore) CancelRun(ctx context.Context, id string) (bool, error) {
	res, err := s.pool.Exec(ctx, "UPDATE runs SET status = 'CANCELLED', updated_at = NOW() WHERE id = $1", id)
	if err != nil {
		return false, fmt.Errorf("cancel run: %w", err)
	}
	return res.RowsAffected() > 0, nil
}

