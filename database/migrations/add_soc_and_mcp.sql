-- ONLINE MIGRATION: LLM-SOC triage queue + MCP token management.
--
-- Adds mcp_token (MCP harness credentials, hashed at rest), soc_job (the
-- persistent triage queue leased to soc-worker instances) and
-- soc_triage_result (validated immutable triage records), plus the soc_* /
-- mcp app_settings defaults.
--
-- Can run on a LIVE database (ingest up, game running):
--   * all three tables are new and empty - plain CREATE TABLE;
--   * all indexes are plain CREATE INDEX on new tables (no rewrite of
--     existing data);
--   * app_settings uses INSERT ON CONFLICT DO NOTHING.
--
-- Idempotent: safe to re-run (IF NOT EXISTS guards).
--
-- Nothing is queued or analyzed by this migration itself: SOC starts
-- disabled (soc_enabled=false), no existing streams or JEV verdicts are
-- retro-enqueued, and MCP tokens must be created explicitly by an
-- operator after deploy.

-- MCP harness tokens. Only the SHA-256 hash is persisted; the plaintext
-- is shown once at creation. Capability is enforced server-side on every
-- call (revocation and expiry take effect immediately).
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

-- Persistent triage queue. One row per (stream, revision) analysis work
-- item; workers claim rows transactionally with a bounded lease.
CREATE TABLE IF NOT EXISTS soc_job (
    id BIGSERIAL PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    -- The exact stream revision (stream.last_packet_id) this job covers.
    stream_revision BIGINT NOT NULL,
    -- Originating JEV verdict for automatic jobs; NULL for manual ones.
    jev_verdict_id BIGINT,
    trigger TEXT NOT NULL CHECK (trigger IN ('automatic', 'manual')),
    -- Operator identity for manual jobs (session-derived when available).
    requested_by TEXT,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    -- Manual jobs start at 100; automatic jobs use JEV severity 0..3.
    -- Aged jobs (waiting >= 60s) outrank every priority level.
    priority INT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    model TEXT,
    prompt_version INT NOT NULL,
    -- Generation of SOC enablement at enqueue; results from older
    -- generations (disabled and re-enabled SOC) are rejected.
    settings_generation BIGINT NOT NULL DEFAULT 0,
    -- Packet/message ids the MCP service actually fetched for this job
    -- (["packet:123", "message:45", ...], bounded). Result evidence ids
    -- must be a subset; anything else is rejected as fabricated.
    fetched_evidence JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    -- Sanitized failure text (no payloads, tokens or provider keys).
    last_error TEXT
);

-- Claim scan: queued jobs by next attempt time. Priority and aging are
-- applied in the ORDER BY of the claim query; the index serves the
-- status + due-time filter.
CREATE INDEX IF NOT EXISTS idx_soc_job_claim
    ON soc_job (status, next_attempt_at);

-- Manual dedup lookups: an existing queued/running job for the same
-- (stream, revision) suppresses duplicate submissions.
CREATE INDEX IF NOT EXISTS idx_soc_job_stream_revision
    ON soc_job (stream_id, stream_revision)
    WHERE status IN ('queued', 'running');

-- Exactly one automatic job per JEV verdict identity. Partial: manual
-- jobs (NULL jev_verdict_id) are unaffected (NULLs never conflict).
CREATE UNIQUE INDEX IF NOT EXISTS idx_soc_job_automatic_unique
    ON soc_job (jev_verdict_id)
    WHERE trigger = 'automatic';

-- Validated triage output. Immutable history: one result per job, a
-- newer stream revision creates a new row rather than replacing.
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
    -- Bounded list of {packet_id | message_id, explanation} items; every
    -- id must have been fetched by the MCP service for this job.
    evidence JSONB NOT NULL,
    -- Bounded list of short indicator strings.
    indicators JSONB NOT NULL,
    -- Informational only: nothing in Red Eye executes this.
    recommended_action TEXT NOT NULL
        CHECK (recommended_action IN ('none', 'monitor', 'manual_review', 'block_remote_ip')),
    model TEXT NOT NULL,
    prompt_version INT NOT NULL,
    input_tokens BIGINT,
    output_tokens BIGINT,
    duration_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Revision history page for the stream view.
CREATE INDEX IF NOT EXISTS idx_soc_result_stream
    ON soc_triage_result (stream_id, stream_revision DESC, id DESC);

-- Daily job-limit accounting and retention-style scans cut by created_at.
CREATE INDEX IF NOT EXISTS idx_soc_result_created
    ON soc_triage_result (created_at);

-- SOC + MCP settings defaults. Runtime overrides via PUT
-- /api/settings/{key}; DO NOTHING keeps operator values on re-run.
-- Secrets (soc_api_key, soc_worker_token, mcp_service_token) have no
-- default rows: they exist only once explicitly set/generated and are
-- write-only (GET never returns them).
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

-- Post-migration sanity checks:
--   \d+ mcp_token
--   \d+ soc_job
--   \d+ soc_triage_result
--   SELECT indexname FROM pg_indexes WHERE tablename IN ('mcp_token','soc_job','soc_triage_result') ORDER BY 1;
--   SELECT key FROM app_settings WHERE key LIKE 'soc_%' ORDER BY key;
