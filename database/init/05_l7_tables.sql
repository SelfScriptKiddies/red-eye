-- L7 protocol analysis tables (partitioned by timestamp)

-- HTTP messages (request or response)
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
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_http_message_stream ON http_message(stream_id);
CREATE INDEX IF NOT EXISTS idx_http_message_method_path ON http_message(method, path);
CREATE INDEX IF NOT EXISTS idx_http_message_status ON http_message(status_code);

-- HTTP headers (key-value pairs for each message)
CREATE TABLE IF NOT EXISTS http_header (
    id SERIAL,
    message_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_http_header_name ON http_header(name);
CREATE INDEX IF NOT EXISTS idx_http_header_name_value ON http_header(name, value);

-- Proto file storage for gRPC decoding
CREATE TABLE IF NOT EXISTS proto_file (
    id SERIAL PRIMARY KEY,
    service_id INTEGER REFERENCES service(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
