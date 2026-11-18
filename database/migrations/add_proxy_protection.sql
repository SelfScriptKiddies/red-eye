-- Persistent proxy protection desired state (per agent + service)
--
-- One row per (agent_id, service_id): the operator's desired proxy state
-- plus the agent-reported effective state as a diagnostic. Desired columns
-- (enabled, listen/target, protocol, tls_mode, http2_enabled, revision) are
-- written ONLY by the backend API inside the same transaction as the
-- control-plane revision bump; effective / effective_revision / last_error
-- are diagnostics learned from agent heartbeats and can never overwrite the
-- operator's desired state.
--
-- The rule identity is (agent_id, service_id): a mutable service name must
-- never be part of the key. At most one effective proxy rule exists per
-- agent and service - enabling again updates the same row.
--
-- ONLINE RUNBOOK (safe while backend/analyzer/agents run):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--        -f database/migrations/add_proxy_protection.sql
--
-- NOTES:
--   - CREATE TABLE IF NOT EXISTS is metadata-only and idempotent.
--   - The FK on service(id) ON DELETE CASCADE removes desired state when
--     the service row is deleted; the agent learns removal through the
--     snapshot revision and runs the disable transition.
--   - No live ingestion table is touched; no long locks are taken.

CREATE TABLE IF NOT EXISTS proxy_protection (
    id BIGSERIAL PRIMARY KEY,
    agent_id VARCHAR(255) NOT NULL,
    service_id BIGINT NOT NULL REFERENCES service(id) ON DELETE CASCADE,
    -- Operator desired state.
    enabled BOOLEAN NOT NULL,
    listen_address TEXT,
    listen_port INT,
    target_address TEXT,
    target_port INT,
    protocol TEXT NOT NULL CHECK (protocol IN ('TCP', 'UDP')),
    tls_mode TEXT,
    http2_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    -- Control-plane revision this desired state was issued at.
    revision BIGINT NOT NULL,
    -- Agent-reported diagnostics (never operator inputs).
    effective BOOLEAN NOT NULL DEFAULT FALSE,
    effective_revision BIGINT,
    -- Agent-reported capture phase: "effective" | "transitioning" |
    -- "capture_unavailable" | "failed"; NULL = nothing reported yet.
    reported_state TEXT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT unique_proxy_agent_service UNIQUE (agent_id, service_id)
);

CREATE INDEX IF NOT EXISTS idx_proxy_protection_agent
    ON proxy_protection (agent_id);
