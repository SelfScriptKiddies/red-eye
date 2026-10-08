-- ONLINE MIGRATION: keyset pagination index for the streams list
-- (TODO.md §2.4 item 3).
--
-- The streams list now pages with a stable keyset cursor:
--     WHERE (COALESCE(last_packet_time, 0), id) < ($cursor_time, $cursor_id)
--     ORDER BY COALESCE(last_packet_time, 0) DESC, id DESC
--     LIMIT $limit
-- instead of LIMIT/OFFSET on a sort key (last_time) that changes with every
-- flushed batch — OFFSET pages re-served shifted rows on every refetch.
--
-- Can run on a LIVE database (ingest up, game running):
--   * CREATE INDEX CONCURRENTLY — no exclusive lock on stream; concurrent
--     INSERT/UPDATE proceed.
--   * DROP INDEX CONCURRENTLY — same non-blocking behavior.
--
-- CAVEAT (CONCURRENTLY cannot run inside a transaction block):
--   Run with plain `psql -f`, NOT wrapped in BEGIN/COMMIT or
--   --single-transaction.
--
-- Idempotent: safe to re-run (IF EXISTS / IF NOT EXISTS).
--
-- Why an expression index: the list has ALWAYS ordered by
-- COALESCE(last_packet_time, 0) (NULL last_time sorts as 0, i.e. oldest —
-- that mapping lives in the query's subselect). A plain index on
-- last_packet_time cannot serve an ORDER BY/predicate over COALESCE(...),
-- so idx_stream_last_packet was never usable by the list query — it is
-- dropped here and replaced by the matching expression index. The `id`
-- column is included as the unique tiebreaker so the row-value comparison
-- `(last_time, id) < (x, y)` has a total order (no row is skipped or
-- duplicated across page boundaries when many streams share one last_time).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_last_time_id
    ON stream (COALESCE(last_packet_time, 0) DESC, id DESC);

DROP INDEX CONCURRENTLY IF EXISTS idx_stream_last_packet;

-- Post-migration sanity checks:
--   SELECT indexname FROM pg_indexes WHERE tablename = 'stream';
--   EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM stream
--     WHERE (COALESCE(last_packet_time, 0), id) < (0, 0)
--     ORDER BY COALESCE(last_packet_time, 0) DESC, id DESC LIMIT 20;
--   → should show an Index Scan using idx_stream_last_time_id.
