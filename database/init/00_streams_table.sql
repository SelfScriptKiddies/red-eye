-- Create 'stream' table
CREATE TABLE IF NOT EXISTS stream (
    id SERIAL PRIMARY KEY,
    src_ip INET NOT NULL,
    src_port INTEGER NOT NULL,
    dst_ip INET NOT NULL,
    dst_port INTEGER NOT NULL,
    protocol VARCHAR(3) NOT NULL,
    active BOOLEAN NOT NULL,
    favorite BOOLEAN NOT NULL DEFAULT FALSE,
    packet_count INTEGER NOT NULL DEFAULT 0,
    service_id INTEGER NOT NULL,
    l7_protocol VARCHAR(16) NOT NULL DEFAULT 'unknown',
    first_packet_time BIGINT,
    last_packet_time BIGINT
);

CREATE INDEX IF NOT EXISTS idx_stream_last_packet ON stream(last_packet_time DESC);
CREATE INDEX IF NOT EXISTS idx_stream_service_id ON stream(service_id);
