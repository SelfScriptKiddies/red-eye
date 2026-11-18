-- LLM-SOC triage queue + MCP token management (fresh-init twin of
-- database/migrations/add_soc_and_mcp.sql).
-- tag_name.enabled comes from 11_unified_filter_model.sql.

CREATE TABLE IF NOT EXISTS mcp_token (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    capability TEXT NOT NULL CHECK (capability IN ('read_only', 'full_control')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS soc_job (
    id BIGSERIAL PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    -- The exact stream revision (stream.last_packet_id) this job covers.
    stream_revision BIGINT NOT NULL,
    -- Originating JEV verdict for automatic jobs; NULL for manual ones.
    jev_verdict_id BIGINT,
    trigger TEXT NOT NULL CHECK (trigger IN ('automatic', 'manual')),
    requested_by TEXT,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    priority INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    model TEXT,
    prompt_version INT NOT NULL,
    settings_generation BIGINT NOT NULL DEFAULT 0,
    -- Packet/message ids the MCP service actually fetched for this job;
    -- result evidence ids must be a subset of this set.
    fetched_evidence JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    -- Sanitized failure text (no payloads, tokens or provider keys).
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_soc_job_claim
    ON soc_job (status, next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_soc_job_stream_revision
    ON soc_job (stream_id, stream_revision)
    WHERE status IN ('queued', 'running');

CREATE UNIQUE INDEX IF NOT EXISTS idx_soc_job_automatic_unique
    ON soc_job (jev_verdict_id)
    WHERE trigger = 'automatic';

CREATE TABLE IF NOT EXISTS soc_triage_result (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL UNIQUE,
    stream_id BIGINT NOT NULL,
    stream_revision BIGINT NOT NULL,
    verdict TEXT NOT NULL
        CHECK (verdict IN ('benign', 'suspicious', 'likely_attack', 'unknown')),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    severity INT NOT NULL CHECK (severity >= 0 AND severity <= 3),
    summary TEXT NOT NULL CHECK (length(summary) <= 2000),
    evidence JSONB NOT NULL,
    indicators JSONB NOT NULL,
    recommended_action TEXT NOT NULL
        CHECK (recommended_action IN ('none', 'monitor', 'manual_review', 'block_remote_ip')),
    model TEXT NOT NULL,
    prompt_version INT NOT NULL,
    input_tokens BIGINT,
    output_tokens BIGINT,
    duration_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_soc_result_stream
    ON soc_triage_result (stream_id, stream_revision DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_soc_result_created
    ON soc_triage_result (created_at);

INSERT INTO app_settings (key, value) VALUES
    ('soc_enabled', 'false'),
    ('soc_auto_enabled', 'false'),
    ('soc_base_url', 'http://llm:8080/v1'),
    ('soc_model', 'qwen3.8-flash-next-iq3'),
    ('soc_alert_threshold', '0.7'),
    ('soc_auto_min_jev_severity', '2'),
    ('soc_max_context_tokens', '16384'),
    ('soc_max_output_tokens', '1024'),
    ('soc_max_tool_calls', '8'),
    ('soc_max_tool_result_bytes', '16384'),
    ('soc_job_timeout_secs', '120'),
    ('soc_max_attempts', '3'),
    ('soc_concurrency', '1'),
    ('soc_daily_job_limit', '500')
ON CONFLICT (key) DO NOTHING;
