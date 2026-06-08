-- Materialized view for stream listing (replaces live aggregate join)
-- Refreshed via pg_notify from the traffic-analyzer batch flusher
-- Uses stream.packet_count / stream.last_packet_time (maintained by batch flusher)
-- to avoid a full scan of the packets table.

CREATE MATERIALIZED VIEW IF NOT EXISTS streams_list_view AS
SELECT
    str.id AS stream_id,
    str.src_ip,
    str.dst_ip,
    str.src_port,
    str.dst_port,
    str.protocol,
    str.favorite,
    str.l7_protocol,
    s.id AS service_id,
    s.name AS service_name,
    COALESCE(
        (SELECT MIN(p.timestamp) FROM packets p WHERE p.stream_id = str.id),
        0
    ) AS first_time,
    COALESCE(str.last_packet_time, 0) AS last_time,
    str.packet_count,
    COALESCE(
        (SELECT array_agg(DISTINCT tn.name)
         FROM tag t
         JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
         JOIN tag_name tn ON tn.id = t.tag_id
         WHERE p.stream_id = str.id AND tn.name IS NOT NULL),
        ARRAY[]::text[]
    ) AS tags
FROM stream str
JOIN service s ON s.id = str.service_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_slv_stream_id ON streams_list_view(stream_id);
CREATE INDEX IF NOT EXISTS idx_slv_last_time ON streams_list_view(last_time DESC);
CREATE INDEX IF NOT EXISTS idx_slv_service_id ON streams_list_view(service_id);

-- Function to refresh the view (called by backend LISTEN handler)
CREATE OR REPLACE FUNCTION refresh_streams_view()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY streams_list_view;
END;
$$ LANGUAGE plpgsql;
