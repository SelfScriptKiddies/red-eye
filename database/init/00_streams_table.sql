-- Create 'stream' table
-- id is BIGINT: stream ids are referenced by packets/http_message/grpc_message
-- (BIGINT end-to-end) and a 32-bit sequence is a crash class at game load.
CREATE TABLE IF NOT EXISTS stream (
    id BIGSERIAL PRIMARY KEY,
    src_ip INET NOT NULL,
    src_port INTEGER NOT NULL,
    dst_ip INET NOT NULL,
    dst_port INTEGER NOT NULL,
    protocol VARCHAR(3) NOT NULL,
    active BOOLEAN NOT NULL,
    favorite BOOLEAN NOT NULL DEFAULT FALSE,
    packet_count INTEGER NOT NULL DEFAULT 0,
    service_id BIGINT NOT NULL,
    l7_protocol VARCHAR(16) NOT NULL DEFAULT 'unknown',
    first_packet_time BIGINT,
    last_packet_time BIGINT,
    -- Capturing agent (per-agent attribution); NULL for legacy rows and
    -- batches without a known agent.
    agent_id VARCHAR(255),
    -- Starred-stream retention metadata (see
    -- database/migrations/add_favorite_retention.sql). favorite stays the
    -- fast operator-visible predicate; the columns below carry the
    -- retention guarantee.
    favorite_since TIMESTAMPTZ,
    -- Monotonic per-stream token; late workers compare it before touching
    -- anything.
    favorite_revision BIGINT NOT NULL DEFAULT 0,
    -- Highest packet id represented in saved_packet; content rows of
    -- packets <= this id are archived in the same commit.
    archived_through_packet_id BIGINT NOT NULL DEFAULT 0,
    -- live | archiving | archived | incomplete. archiving with
    -- favorite=false means "saved content awaiting bounded deletion".
    retention_state TEXT NOT NULL DEFAULT 'live'
        CHECK (retention_state IN ('live', 'archiving', 'archived', 'incomplete')),
    -- FK-free: the previous segment may be orphan-cleaned while this row
    -- lives on; a dangling link renders as none.
    continuation_of BIGINT,
    segment_index INT NOT NULL DEFAULT 1,
    -- Running payload-byte estimate maintained by the analyzer flush.
    retention_bytes BIGINT NOT NULL DEFAULT 0,
    -- Last archive batch failure, sanitized (ids/byte counts, no payloads).
    archive_error TEXT
);

-- Streams list keyset pagination: the list orders by
-- COALESCE(last_packet_time, 0) DESC (NULL last_time = 0 = oldest) with id
-- DESC as the unique tiebreaker, and pages via the row-value cursor
-- (COALESCE(last_packet_time, 0), id) < (cursor_time, cursor_id). The
-- expression index matches both the ORDER BY and the cursor predicate
-- (a plain (last_packet_time) index cannot serve the COALESCE expression).
-- Online migration for existing databases:
-- database/migrations/stream_keyset_index.sql
CREATE INDEX IF NOT EXISTS idx_stream_last_time_id
    ON stream (COALESCE(last_packet_time, 0) DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_stream_service_id ON stream(service_id);
-- Per-packet stream lookup (analyzer find_stream_id: WHERE 4-tuple AND
-- active ORDER BY id DESC LIMIT 1). Without it every cache miss/TTL-expiry
-- is a full scan — measured on scratch PG16, 200k stream rows: seq scan
-- 5.8-6.7 ms vs 0.04 ms with this index (~150x), and the seq scan cost
-- grows linearly with table size over a 24h game. Partial (WHERE active)
-- matches the query exactly and shrinks as the FIN/RST lifecycle retires
-- streams (identical speed to a full 4-col index at 100% active).
CREATE INDEX IF NOT EXISTS idx_stream_tuple_active ON stream(src_ip, src_port, dst_ip, dst_port) WHERE active;
-- Favorite keyset listing: the favorites view reuses the main list's
-- ordering/cursor expressions restricted to favorites (tiny partial index).
CREATE INDEX IF NOT EXISTS idx_stream_favorite_list
    ON stream (COALESCE(last_packet_time, 0) DESC, id DESC) WHERE favorite;
-- Archive/cleanup worker scans and capacity accounting.
CREATE INDEX IF NOT EXISTS idx_stream_favorite_state ON stream (favorite, retention_state);
-- Continuation chain walks (prev/next segment links).
CREATE INDEX IF NOT EXISTS idx_stream_continuation
    ON stream (continuation_of) WHERE continuation_of IS NOT NULL;
