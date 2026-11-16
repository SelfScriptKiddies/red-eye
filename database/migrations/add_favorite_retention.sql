-- ONLINE MIGRATION: starred stream retention (favorites survive cleanup).
--
-- Adds stream retention metadata, non-partitioned saved_* twins of the
-- partitioned content tables, favorite-related indexes and the capacity
-- settings. Star/unstar/archive/cleanup workers (backend) and segment
-- rotation (traffic-analyzer) depend on every object created here.
--
-- Can run on a LIVE database (ingest up, game running):
--   * ALTER TABLE ADD COLUMN takes a brief ACCESS EXCLUSIVE lock that only
--     waits for in-flight queries; no table rewrite (all defaults are
--     constants or NULL).
--   * The CHECK constraint is validated on existing rows; stream is small
--     (bounded by connection churn), so the scan is fast.
--   * CREATE INDEX CONCURRENTLY on stream - no write blocking.
--   * saved_* tables are new and empty - plain CREATE TABLE.
--
-- CAVEAT (CONCURRENTLY cannot run inside a transaction block):
--   Run with plain `psql -f`, NOT wrapped in BEGIN/COMMIT or
--   --single-transaction.
--
-- Idempotent: safe to re-run (IF EXISTS / IF NOT EXISTS / pg_constraint
-- guards).

-- ── Stream retention metadata ───────────────────────────────────────────

ALTER TABLE stream ADD COLUMN IF NOT EXISTS favorite_since TIMESTAMPTZ;
-- Monotonic per-stream token: a late archive/cleanup worker from an older
-- star compares it before touching anything.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS favorite_revision BIGINT NOT NULL DEFAULT 0;
-- Highest packet id known to be represented in saved_packet. Content rows
-- (tag/http/grpc) of packets <= this id are archived in the same commit.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS archived_through_packet_id BIGINT NOT NULL DEFAULT 0;
-- live | archiving | archived | incomplete.
-- archiving on a favorite=false row means "saved content awaiting bounded
-- deletion" (post-unstar pending cleanup), not an active archive.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS retention_state TEXT NOT NULL DEFAULT 'live';
-- FK-free on purpose: the previous segment may be deleted by ordinary
-- orphan cleanup while this row lives on; a dangling link renders as none.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS continuation_of BIGINT;
ALTER TABLE stream ADD COLUMN IF NOT EXISTS segment_index INT NOT NULL DEFAULT 1;
-- Running payload-byte estimate for the stream (maintained by the
-- analyzer flush). Estimate only: capacity accounting treats it as a
-- lower bound and adds reservation margin on top.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS retention_bytes BIGINT NOT NULL DEFAULT 0;
-- Last archive batch failure, sanitized (ids and byte counts only,
-- never payloads). NULL = healthy.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS archive_error TEXT;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'stream_retention_state_check'
    ) THEN
        ALTER TABLE stream ADD CONSTRAINT stream_retention_state_check
            CHECK (retention_state IN ('live', 'archiving', 'archived', 'incomplete'));
    END IF;
END $$;

-- Favorite keyset listing: same ordering/cursor expressions as the main
-- streams list but restricted to favorites (partial index stays tiny).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_favorite_list
    ON stream (COALESCE(last_packet_time, 0) DESC, id DESC) WHERE favorite;

-- Worker scans (archive loop, cleanup guard, capacity accounting).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_favorite_state
    ON stream (favorite, retention_state);

-- Continuation chain walks (prev/next segment links in the UI).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_continuation
    ON stream (continuation_of) WHERE continuation_of IS NOT NULL;

-- ── Saved tables (non-partitioned, PK = original id) ────────────────────
-- Mirror the source row identity: the archive worker copies rows verbatim
-- (ON CONFLICT (id) DO NOTHING keeps retries idempotent), and read paths
-- union live + saved deduplicating by the original id. Timestamps stay
-- original so ordering and body offsets keep working unchanged.

CREATE TABLE IF NOT EXISTS saved_packet (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    timestamp_microseconds BIGINT NOT NULL,
    direction VARCHAR(4) NOT NULL,
    stream_id BIGINT NOT NULL,
    packet_data BYTEA NOT NULL
);

-- Ordered reads and per-stream archive/cleanup batches.
CREATE INDEX IF NOT EXISTS idx_saved_packet_stream_ts ON saved_packet (stream_id, timestamp);

CREATE TABLE IF NOT EXISTS saved_tag (
    id BIGINT PRIMARY KEY,
    packet_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Mirror of the live tag FK: removing a tag_name removes its saved
    -- matches too, so saved and live tag semantics never diverge.
    CONSTRAINT fk_saved_tag
        FOREIGN KEY (tag_id)
            REFERENCES tag_name(id)
            ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saved_tag_stream_ts ON saved_tag (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_tag_packet ON saved_tag (packet_id);

CREATE TABLE IF NOT EXISTS saved_http_message (
    id BIGINT PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    packet_id BIGINT NOT NULL,
    is_request BOOLEAN NOT NULL,
    method VARCHAR(10),
    path TEXT,
    status_code SMALLINT,
    http_version VARCHAR(10),
    content_type VARCHAR(255),
    content_length INTEGER,
    body_offset INTEGER,
    body_length INTEGER,
    header_set_id BIGINT,
    timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_http_stream_ts ON saved_http_message (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_http_packet ON saved_http_message (packet_id);
-- Orphan cleanup of header_set must see saved references (header_set and
-- grpc_body_set stay shared between live and saved rows).
CREATE INDEX IF NOT EXISTS idx_saved_http_header_set ON saved_http_message (header_set_id);

CREATE TABLE IF NOT EXISTS saved_grpc_message (
    id BIGINT PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    http_message_id BIGINT NOT NULL,
    service_name VARCHAR(255),
    method_name VARCHAR(255),
    is_request BOOLEAN NOT NULL,
    body_set_id BIGINT,
    timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_grpc_stream_ts ON saved_grpc_message (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_grpc_http ON saved_grpc_message (http_message_id);

-- ── Capacity settings ───────────────────────────────────────────────────
-- Env (FAVORITE_MAX_TOTAL_BYTES / FAVORITE_SEGMENT_BYTES /
-- FAVORITE_DISK_RESERVE_BYTES) seeds the initial values at backend startup;
-- these rows are the runtime overrides. DO NOTHING keeps operator values.

INSERT INTO app_settings (key, value) VALUES
    ('favorite_max_total_bytes', '10737418240'),
    ('favorite_segment_bytes', '268435456'),
    ('favorite_disk_reserve_bytes', '2147483648')
ON CONFLICT (key) DO NOTHING;

-- Post-migration sanity checks:
--   SELECT retention_state, COUNT(*) FROM stream WHERE favorite GROUP BY 1;
--   SELECT indexname FROM pg_indexes WHERE tablename = 'stream' AND indexname LIKE 'idx_stream_%';
--   \d+ saved_packet
