package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcusferl/etchflow/internal/models"
)

// Store is the single access point for all database operations.
// Composes RunStore, CheckpointStore, and LogStore.
// The critical AtomicCheckpoint method lives here because it coordinates all three.
type Store struct {
	pool       *pgxpool.Pool
	Runs       *RunStore
	Checkpoints *CheckpointStore
	Logs       *LogStore
}

// New creates a fully initialised Store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:        pool,
		Runs:        NewRunStore(pool),
		Checkpoints: NewCheckpointStore(pool),
		Logs:        NewLogStore(pool),
	}
}

// CheckpointResult is the result of an AtomicCheckpoint call.
type CheckpointResult struct {
	// Continue tells Python whether to keep executing nodes.
	// false when the run has reached the __end__ node (SUCCESS).
	Continue   bool
	// HaltReason is set when Continue=false and explains why.
	HaltReason string
	// WasNew is true if the checkpoint was freshly inserted (not a duplicate).
	WasNew     bool
}

// isTerminalNode checks if the node name indicates LangGraph has completed.
// LangGraph sends "__end__" when the graph naturally completes.
func isTerminalNode(nodeName string) bool {
	return nodeName == "__end__" || nodeName == "__root__"
}

// AtomicCheckpoint is the core operation of EtchFlow.
//
// In a single database transaction it:
//  1. Inserts the checkpoint (ON CONFLICT DO NOTHING — idempotent)
//  2. Updates runs.current_state and last_node_completed
//  3. If the run is still PENDING, auto-transitions it to RUNNING
//  4. If the node is "__end__" (LangGraph's terminal node), auto-transition to SUCCESS
//
// Both the checkpoint insert and the run state update commit together
// or both roll back. This is the durability guarantee.
func (s *Store) AtomicCheckpoint(ctx context.Context, run *models.Run, nodeName string, state map[string]any) (*CheckpointResult, error) {
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshal state: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step 1: Insert checkpoint (idempotent)
	created, err := s.Checkpoints.saveCheckpointTx(ctx, tx, run.ID, nodeName, stateJSON)
	if err != nil {
		return nil, fmt.Errorf("save checkpoint: %w", err)
	}

	if created {
		// Step 2: Update current_state and last_node_completed on the run
		if err := s.Runs.updateCurrentStateTx(ctx, tx, run.ID, stateJSON, nodeName); err != nil {
			return nil, err
		}

		// Step 3: Auto-transition PENDING → RUNNING on first checkpoint
		if run.Status == models.StatusPending {
			if err := s.Runs.setRunningTx(ctx, tx, run.ID); err != nil {
				return nil, err
			}
		}

		// Step 4: Auto-transition RUNNING → SUCCESS on __end__ node
		// This is how LangGraph signals natural completion
		if isTerminalNode(nodeName) && run.Status == models.StatusRunning {
			if err := s.Runs.setSuccessTx(ctx, tx, run.ID); err != nil {
				return nil, fmt.Errorf("auto-success transition: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	// Determine if we should continue or halt
	isComplete := isTerminalNode(nodeName)

	return &CheckpointResult{
		Continue:   !isComplete,
		WasNew:     created,
		HaltReason: "",
	}, nil
}


// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

