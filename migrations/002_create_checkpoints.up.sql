-- 002_create_checkpoints.up.sql
-- One row per completed node per run. Source of truth for crash recovery.
-- UNIQUE(run_id, node_name) is the idempotency guarantee:
-- ON CONFLICT DO NOTHING prevents double-writes from retries or network blips.

CREATE TABLE IF NOT EXISTS checkpoints (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      TEXT        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    node_name   TEXT        NOT NULL,

    -- Full LangGraph state after this node completed.
    state_json  JSONB       NOT NULL,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A node can only checkpoint once per run.
    CONSTRAINT uq_checkpoint_run_node UNIQUE (run_id, node_name)
);

CREATE INDEX idx_checkpoints_run_id ON checkpoints (run_id, created_at ASC);
