-- Create partitioned 'packets' table
-- id BIGINT: at ~100k pkt/s an int4 sequence exhausts 2^31 in ~6h and every
-- INSERT fails (ingest stops). All id/FK columns are BIGINT end-to-end so
-- JOINs never mix int4/int8.
CREATE TABLE IF NOT EXISTS packets (
    id BIGSERIAL,
    timestamp BIGINT NOT NULL,
    timestamp_microseconds BIGINT NOT NULL,
    direction VARCHAR(4) NOT NULL,
    stream_id BIGINT NOT NULL,
    packet_data BYTEA NOT NULL,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Indexes (applied to all partitions automatically)
-- Note: a single-column index on stream_id is redundant with idx_packets_stream_ts
-- (leading column stream_id), so it is intentionally omitted to cut write amplification.
CREATE INDEX IF NOT EXISTS idx_packets_timestamp ON packets(timestamp);
CREATE INDEX IF NOT EXISTS idx_packets_stream_ts ON packets(stream_id, timestamp DESC);
