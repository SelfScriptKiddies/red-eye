-- Create partitioned 'packets' table
CREATE TABLE IF NOT EXISTS packets (
    id SERIAL,
    timestamp BIGINT NOT NULL,
    timestamp_microseconds BIGINT NOT NULL,
    direction VARCHAR(4) NOT NULL,
    stream_id INTEGER NOT NULL,
    packet_data BYTEA NOT NULL,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Indexes (applied to all partitions automatically)
-- Note: a single-column index on stream_id is redundant with idx_packets_stream_ts
-- (leading column stream_id), so it is intentionally omitted to cut write amplification.
CREATE INDEX IF NOT EXISTS idx_packets_timestamp ON packets(timestamp);
CREATE INDEX IF NOT EXISTS idx_packets_stream_ts ON packets(stream_id, timestamp DESC);
