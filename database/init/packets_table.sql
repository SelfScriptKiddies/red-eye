-- Create 'packets' table
CREATE TABLE IF NOT EXISTS packets (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sequence BIGINT NOT NULL,
    stream_id INTEGER NOT NULL,
    packet_data BYTEA NOT NULL,
    CONSTRAINT fk_stream
        FOREIGN KEY(stream_id) 
            REFERENCES stream(id)
            ON DELETE CASCADE
);

-- Create index on stream_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_packets_stream_id ON packets(stream_id);

-- Optional: Create index on timestamp if queries frequently filter by time
CREATE INDEX IF NOT EXISTS idx_packets_timestamp ON packets(timestamp);
