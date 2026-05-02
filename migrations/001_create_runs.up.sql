-- 001_create_runs.up.sql
-- runs table: serves as task queue, state machine, and coordination point.
-- MVP-slim: no worker_id, heartbeat, or retry columns (added in Phase 1.5 via ALTER TABLE).

CREATE TABLE IF NOT EXISTS runs (
    id                  TEXT        PRIMARY KEY,

    -- DAG topology stored as metadata (not execution logic).
    -- EtchFlow stores this for validation and display only.
    graph_definition    JSONB       NOT NULL,

    -- Initial input passed to the graph on first run or resume.
    input_data          JSONB       NOT NULL,

    -- Latest graph state, updated atomically per checkpoint.
    -- This is what Python loads on resume. NULL = no checkpoint yet.
    current_state       JSONB,

    -- State machine: PENDING | RUNNING | SUCCESS | FAILED
    status              TEXT        NOT NULL DEFAULT 'PENDING',

    -- Name of the last successfully checkpointed node.
    last_node_completed TEXT,

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_runs_status ON runs (status);
