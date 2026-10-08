-- L7 protocol analysis tables
-- All ids BIGINT end-to-end: http_message/grpc_message are high-volume
-- (int4 sequence exhaustion = ingest stops) and their FK columns must not
-- mix int4/int8 with the referenced BIGINT ids.

-- Deduplicated header sets (small table, not partitioned)
CREATE TABLE IF NOT EXISTS header_set (
    id BIGSERIAL PRIMARY KEY,
    hash BIGINT UNIQUE NOT NULL,
    headers JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_header_set_headers ON header_set USING GIN (headers);

-- HTTP messages (partitioned by timestamp)
CREATE TABLE IF NOT EXISTS http_message (
    id BIGSERIAL,
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
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_http_message_stream ON http_message(stream_id);
CREATE INDEX IF NOT EXISTS idx_http_message_method_path ON http_message(method, path);
CREATE INDEX IF NOT EXISTS idx_http_message_path ON http_message(path);
CREATE INDEX IF NOT EXISTS idx_http_message_status ON http_message(status_code);
CREATE INDEX IF NOT EXISTS idx_http_message_header_set ON http_message(header_set_id);

-- Deduplicated gRPC body set (like header_set)
CREATE TABLE IF NOT EXISTS grpc_body_set (
    id BIGSERIAL PRIMARY KEY,
    hash BIGINT UNIQUE NOT NULL,
    decoded_json TEXT NOT NULL
);

-- gRPC messages (partitioned by timestamp)
CREATE TABLE IF NOT EXISTS grpc_message (
    id BIGSERIAL,
    stream_id BIGINT NOT NULL,
    http_message_id BIGINT NOT NULL,
    service_name VARCHAR(255),
    method_name VARCHAR(255),
    is_request BOOLEAN NOT NULL,
    body_set_id BIGINT,
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_grpc_message_stream ON grpc_message(stream_id);
CREATE INDEX IF NOT EXISTS idx_grpc_message_method ON grpc_message(service_name, method_name);
CREATE INDEX IF NOT EXISTS idx_grpc_message_http_msg ON grpc_message(http_message_id);

-- Proto file storage for gRPC decoding
CREATE TABLE IF NOT EXISTS proto_file (
    id BIGSERIAL PRIMARY KEY,
    service_id BIGINT REFERENCES service(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
