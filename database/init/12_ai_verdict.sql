-- JEV verdict storage - revisioned stream analysis results.
-- One row per analyzed stream revision (identity = stream_id +
-- last_packet_id + model + prompt_version). Only validated provider
-- responses create rows. Fresh-init twin of
-- database/migrations/add_ai_verdicts.sql (which also carries the stream
-- last_packet_id column and the AI settings defaults for live databases).

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
