-- Create 'stream' table
CREATE TABLE IF NOT EXISTS stream (
    id SERIAL PRIMARY KEY,
    src_ip INET NOT NULL,
    src_port INTEGER NOT NULL,
    dst_ip INET NOT NULL,
    dst_port INTEGER NOT NULL,
    protocol VARCHAR(3) NOT NULL,
    active BOOLEAN NOT NULL,
    packet_count INTEGER NOT NULL DEFAULT 0,
    service_id INTEGER NOT NULL
);