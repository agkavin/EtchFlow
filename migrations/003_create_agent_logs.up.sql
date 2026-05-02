-- 003_create_agent_logs.up.sql
-- Append-only audit trail. Written on every state transition and checkpoint.
-- Never updated. Never deleted (within a run's lifetime).

CREATE TABLE IF NOT EXISTS agent_logs (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      TEXT        NOT NULL REFERENCES runs(id) ON DELETE CASCADE,

    -- NULL for run-level events (SUBMITTED, SUCCESS, etc.)
    node_name   TEXT,

    -- Event types: SUBMITTED | NODE_COMPLETED | SUCCESS | FAILED
    event_type  TEXT        NOT NULL,

    message     TEXT,

    -- Optional structured metadata, e.g. {"attempt": 2, "error": "rate limit"}
    metadata    JSONB,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_logs_run_id ON agent_logs (run_id, created_at ASC);
