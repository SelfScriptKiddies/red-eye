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
    last_packet_time BIGINT
);

CREATE INDEX IF NOT EXISTS idx_stream_last_packet ON stream(last_packet_time DESC);
CREATE INDEX IF NOT EXISTS idx_stream_service_id ON stream(service_id);
-- Per-packet stream lookup (analyzer find_stream_id: WHERE 4-tuple AND
-- active ORDER BY id DESC LIMIT 1). Without it every cache miss/TTL-expiry
-- is a full scan — measured on scratch PG16, 200k stream rows: seq scan
-- 5.8-6.7 ms vs 0.04 ms with this index (~150x), and the seq scan cost
-- grows linearly with table size over a 24h game. Partial (WHERE active)
-- matches the query exactly and shrinks as the FIN/RST lifecycle retires
-- streams (identical speed to a full 4-col index at 100% active).
CREATE INDEX IF NOT EXISTS idx_stream_tuple_active ON stream(src_ip, src_port, dst_ip, dst_port) WHERE active;
