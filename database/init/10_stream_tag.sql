-- Per-stream tag list, maintained INCREMENTALLY by the traffic-analyzer
-- batch flusher (INSERT ... ON CONFLICT DO NOTHING in the same flush cycle).
-- Replaces the streams_list_view materialized view: the streams list reads
-- stream LEFT JOIN aggregated stream_tag and never recomputes the
-- tag x packets join over history (the old REFRESH froze the UI on
-- statement_timeout at game volumes).
--
-- tag_name values are the same tag_name.name strings the matview produced
-- (the analyzer's filter pattern names), so UI semantics don't change.
-- No FK to stream on purpose, mirroring the tag table: stream rows are
-- never deleted mid-game, and an FK would couple the hot flush path to
-- stream row locks.
CREATE TABLE IF NOT EXISTS stream_tag (
    stream_id BIGINT NOT NULL,
    tag_name TEXT NOT NULL,
    PRIMARY KEY (stream_id, tag_name)
);
