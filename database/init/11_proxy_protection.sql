-- Persistent proxy protection desired state (per agent + service)
-- Desired proxy columns are written by the protect API together with the
-- control-plane revision bump; effective/effective_revision/last_error are
-- agent-reported diagnostics and never overwrite desired state.
-- Migration path: database/migrations/add_proxy_protection.sql (keep both
-- files in sync; the migration is idempotent).
CREATE TABLE IF NOT EXISTS proxy_protection (
    id BIGSERIAL PRIMARY KEY,
    agent_id VARCHAR(255) NOT NULL,
    service_id BIGINT NOT NULL REFERENCES service(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL,
    listen_address TEXT,
    listen_port INT,
    target_address TEXT,
    target_port INT,
    protocol TEXT NOT NULL CHECK (protocol IN ('TCP', 'UDP')),
    tls_mode TEXT,
    http2_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL,
    effective BOOLEAN NOT NULL DEFAULT FALSE,
    effective_revision BIGINT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT unique_proxy_agent_service UNIQUE (agent_id, service_id)
);

CREATE INDEX IF NOT EXISTS idx_proxy_protection_agent
    ON proxy_protection (agent_id);
