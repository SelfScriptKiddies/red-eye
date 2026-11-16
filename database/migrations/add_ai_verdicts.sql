-- ONLINE MIGRATION: JEV incremental stream analysis (ai_verdict table,
-- stream last-packet metadata, AI settings defaults).
--
-- Adds stream.last_packet_id (the revision boundary the JEV scheduler uses),
-- the revisioned ai_verdict table with its identity UNIQUE rule, and the
-- indexes the scheduler scans serve. Depends on nothing beyond the base
-- schema; the starred-retention migration (add_favorite_retention.sql) may
-- run before or after this one.
--
-- Can run on a LIVE database (ingest up, game running):
--   * ALTER TABLE ADD COLUMN is a constant-default backfill-free change
--     (brief ACCESS EXCLUSIVE lock, no table rewrite). Existing rows get
--     last_packet_id = 0 and are never candidates until new packets arrive,
--     so no backfill is needed.
--   * ai_verdict is a new empty table - plain CREATE TABLE.
--   * CREATE INDEX CONCURRENTLY on stream - no write blocking.
--
-- CAVEAT (CONCURRENTLY cannot run inside a transaction block):
--   Run with plain `psql -f`, NOT wrapped in BEGIN/COMMIT or
--   --single-transaction.
--
-- Idempotent: safe to re-run (IF EXISTS / IF NOT EXISTS / pg_constraint
-- guards).
--
-- JEV itself starts disabled (ai_enabled=false): nothing queries OpenRouter
-- until an operator explicitly enables it in the UI after this migration.

-- Stream revision boundary
-- Highest packet id flushed into stream stats by the analyzer batch
-- flusher. Unlike timestamp-only activity this gives JEV an exact
-- scheduling and verdict boundary. Legacy rows stay 0 (not candidates).
ALTER TABLE stream ADD COLUMN IF NOT EXISTS last_packet_id BIGINT NOT NULL DEFAULT 0;

-- JEV candidate scan: recent-activity-first and oldest-waiting-first queues
-- both order by stream activity restricted to rows that have a revision.
-- Partial on last_packet_id > 0: legacy rows (0) are never candidates, and
-- the partial index stays small. Fresh-init twin lives in
-- database/init/00_streams_table.sql.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_ai_candidates
    ON stream (last_packet_time DESC, id DESC) WHERE last_packet_id > 0;

-- Verdict storage
-- One row per analyzed stream revision (analysis identity = stream_id +
-- last_packet_id + model + prompt_version). Only validated provider
-- responses create rows; transport errors and malformed answers never
-- become benign verdicts.
CREATE TABLE IF NOT EXISTS ai_verdict (
    id BIGSERIAL PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    -- Contributing packet range of this revision (inclusive).
    first_packet_id BIGINT NOT NULL,
    last_packet_id BIGINT NOT NULL,
    model TEXT NOT NULL,
    -- Backend-owned prompt format version (constant in the backend code).
    prompt_version INT NOT NULL,
    -- Detects accidental scheduling/formatting differences for one
    -- identity. NOT a privacy feature: the transcript itself leaves the
    -- network during analysis.
    transcript_sha256 TEXT NOT NULL,
    attack_class TEXT NOT NULL,
    confidence REAL NOT NULL,
    severity REAL NOT NULL,
    -- Computed by backend code (threshold rule), never by the model.
    is_attack BOOLEAN NOT NULL,
    -- Threshold used at analysis time; historical record.
    ai_threshold REAL NOT NULL,
    -- Complete validated answer map (nouls + choice + score) for
    -- explanation and later triage.
    answers JSONB NOT NULL,
    input_tokens BIGINT,
    cost REAL,
    analyzed_packet_count INT NOT NULL,
    omitted_packet_count INT NOT NULL,
    omitted_byte_count BIGINT NOT NULL,
    truncated BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Duplicate stored verdicts for one identity are impossible; insert
    -- paths use ON CONFLICT DO NOTHING so restarts re-analyzing an
    -- identity never double-store (or double-charge is limited to the
    -- external call itself, counted separately).
    CONSTRAINT ai_verdict_identity UNIQUE (stream_id, last_packet_id, model, prompt_version)
);

-- Stream history page (keyset cursor id < last, newest first).
CREATE INDEX IF NOT EXISTS idx_ai_verdict_stream
    ON ai_verdict (stream_id, id DESC);

-- Latest verdict per (stream, model, prompt_version): the scheduler's
-- identity lookup and the stream-view badge query.
CREATE INDEX IF NOT EXISTS idx_ai_verdict_latest
    ON ai_verdict (stream_id, model, prompt_version, last_packet_id DESC);

-- Recent attack verdicts (attack-only listing, operator triage).
CREATE INDEX IF NOT EXISTS idx_ai_verdict_recent_attacks
    ON ai_verdict (is_attack, created_at DESC);

-- Retention deletes and daily cost accounting both cut by created_at.
CREATE INDEX IF NOT EXISTS idx_ai_verdict_created
    ON ai_verdict (created_at);

-- AI settings defaults
-- Runtime overrides via PUT /api/settings/{key}; DO NOTHING keeps operator
-- values on re-run. ai_api_key has no default (write-only secret).
INSERT INTO app_settings (key, value) VALUES
    ('ai_enabled', 'false'),
    ('ai_model', 'typesafe/jev-1.13'),
    ('ai_threshold', '0.7'),
    ('ai_max_per_tick', '10'),
    ('ai_stream_debounce_ms', '2000'),
    ('ai_stream_min_interval_secs', '10'),
    ('ai_stream_max_bytes', '65536'),
    ('ai_stream_max_entries', '200'),
    ('ai_entry_max_bytes', '8192'),
    ('ai_daily_cost_limit', '5.0'),
    ('ai_verdict_retention_days', '30')
ON CONFLICT (key) DO NOTHING;

-- Post-migration sanity checks:
--   SELECT last_packet_id, COUNT(*) FROM stream GROUP BY 1 ORDER BY 1 LIMIT 5;
--   \d+ ai_verdict
--   SELECT indexname FROM pg_indexes WHERE tablename = 'ai_verdict';
--   SELECT key FROM app_settings WHERE key LIKE 'ai_%' ORDER BY key;
