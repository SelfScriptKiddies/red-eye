-- Materialized view for stream listing (replaces live aggregate join)
-- Refreshed via pg_notify from the traffic-analyzer batch flusher

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
    COALESCE(MIN(p.timestamp), 0) AS first_time,
    COALESCE(MAX(p.timestamp), 0) AS last_time,
    COUNT(DISTINCT p.id) AS packet_count,
    COALESCE(
        array_agg(DISTINCT tn.name) FILTER (WHERE tn.name IS NOT NULL),
        ARRAY[]::text[]
    ) AS tags
FROM stream str
JOIN service s ON s.id = str.service_id
LEFT JOIN packets p ON p.stream_id = str.id
LEFT JOIN tag t ON t.packet_id = p.id AND t.timestamp = p.timestamp
LEFT JOIN tag_name tn ON tn.id = t.tag_id
GROUP BY str.id, str.src_ip, str.dst_ip, str.src_port, str.dst_port,
         str.protocol, str.favorite, str.l7_protocol, s.id, s.name;

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
