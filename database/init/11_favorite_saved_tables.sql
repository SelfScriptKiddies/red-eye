-- Saved (favorite) content tables - non-partitioned twins of the
-- partitioned content tables. Starred streams are archived into these
-- before any cleanup path may drop their live source rows.
--
-- PK is the ORIGINAL id (single column): saved rows are verbatim copies,
-- archive batches are idempotent via ON CONFLICT (id) DO NOTHING, and
-- read paths union live + saved deduplicating by the original id.
-- Timestamps stay original so ordering and body offsets are unchanged.
--
-- Fresh-init twin of database/migrations/add_favorite_retention.sql
-- (which also carries the stream retention columns for live databases).

CREATE TABLE IF NOT EXISTS saved_packet (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    timestamp_microseconds BIGINT NOT NULL,
    direction VARCHAR(4) NOT NULL,
    stream_id BIGINT NOT NULL,
    packet_data BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_packet_stream_ts ON saved_packet (stream_id, timestamp);

CREATE TABLE IF NOT EXISTS saved_tag (
    id BIGINT PRIMARY KEY,
    packet_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
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
