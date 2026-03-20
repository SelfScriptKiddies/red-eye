-- L7 protocol analysis tables

-- Deduplicated header sets (small table, not partitioned)
CREATE TABLE IF NOT EXISTS header_set (
    id SERIAL PRIMARY KEY,
    hash BIGINT UNIQUE NOT NULL,
    headers JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_header_set_headers ON header_set USING GIN (headers);

-- HTTP messages (partitioned by timestamp)
CREATE TABLE IF NOT EXISTS http_message (
    id SERIAL,
    stream_id INTEGER NOT NULL,
    packet_id INTEGER NOT NULL,
    is_request BOOLEAN NOT NULL,
    method VARCHAR(10),
    path TEXT,
    status_code SMALLINT,
    http_version VARCHAR(10),
    content_type VARCHAR(255),
    content_length INTEGER,
    body_offset INTEGER,
    body_length INTEGER,
    header_set_id INTEGER,
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_http_message_stream ON http_message(stream_id);
CREATE INDEX IF NOT EXISTS idx_http_message_method_path ON http_message(method, path);
CREATE INDEX IF NOT EXISTS idx_http_message_status ON http_message(status_code);
CREATE INDEX IF NOT EXISTS idx_http_message_header_set ON http_message(header_set_id);

-- Proto file storage for gRPC decoding
CREATE TABLE IF NOT EXISTS proto_file (
    id SERIAL PRIMARY KEY,
    service_id INTEGER REFERENCES service(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
