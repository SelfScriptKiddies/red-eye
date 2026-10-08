-- ONLINE MIGRATION: stream 4-tuple index + stream_tag replaces the
-- streams_list_view materialized view (TODO.md §2 шаг 3).
--
-- Can run on a LIVE database (ingest up, game running):
--   * CREATE INDEX CONCURRENTLY — no exclusive lock on stream; concurrent
--     INSERTs/UPDATEs proceed (it takes SHARE UPDATE EXCLUSIVE, blocking
--     only other DDL/VACUUM on the table).
--   * CREATE TABLE stream_tag — catalog-only.
--   * Backfill INSERT ... SELECT — a single statement; reads tag/packets
--     (ACCESS SHARE only, no read lock against writers) and writes the new
--     empty stream_tag. It is the heavy part: it scans the tag x packets
--     join ONCE (the very computation the matview used to repeat every
--     refresh). If it exceeds lock_timeout/statement_timeout, raise the
--     timeouts for this session or run the backfill separately (statement 4)
--     in batches; the migration is ordered so everything before the backfill
--     is safe to re-run.
--   * DROP MATERIALIZED VIEW / DROP FUNCTION — ACCESS EXCLUSIVE on the
--     matview/function only, not on stream/packets.
--
-- ROLLOUT ORDER (one release window):
--   1. Run this migration (old backend keeps working: it reads the matview
--      until it is dropped; after the drop the OLD backend's streams list
--      500s — deploy the new backend in the same window).
--   2. Deploy new traffic-analyzer + backend binaries. The new analyzer
--      maintains stream_tag and stops sending pg_notify('refresh_streams').
--
-- ORDERING with other migrations: run AFTER database/migrations/bigint_ids.sql
-- on pre-bigint databases (that script recreates streams_list_view; this one
-- then removes it for good). A database already on current origin/main
-- (BIGINT ids) needs only this file.
--
-- CAVEAT (CONCURRENTLY cannot run inside a transaction block):
--   Run with plain `psql -f`, NOT with `psql -1` / --single-transaction /
--   ON_ERROR_STOP-in-transaction wrappers — statement 1 must run in its
--   own implicit transaction. The remaining statements are wrapped in an
--   explicit BEGIN/COMMIT below.
--
-- Idempotent: safe to re-run (IF NOT EXISTS / ON CONFLICT DO NOTHING /
-- IF EXISTS on drops). If re-run after completion, the backfill SELECT
-- returns 0 rows (tag rows for streams already covered — ON CONFLICT
-- absorbs them).

-- 1) 4-tuple lookup index for find_stream_id (WHERE 4-tuple AND active
--    ORDER BY id DESC LIMIT 1). Partial (WHERE active): matches the query
--    exactly and shrinks as the FIN/RST lifecycle retires streams.
--    NOTE: outside the transaction on purpose — CONCURRENTLY is not
--    allowed in a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_tuple_active
    ON stream(src_ip, src_port, dst_ip, dst_port) WHERE active;

BEGIN;

-- 2) The incremental tag store (same definition as
--    database/init/10_stream_tag.sql for fresh deployments).
CREATE TABLE IF NOT EXISTS stream_tag (
    stream_id BIGINT NOT NULL,
    tag_name TEXT NOT NULL,
    PRIMARY KEY (stream_id, tag_name)
);

-- 3) Backfill: project the CURRENT tag x packets join (the exact query the
--    matview ran per stream) into stream_tag once. Distinct pairs only;
--    ON CONFLICT DO NOTHING dedups against any rows the new analyzer may
--    already be writing if it was deployed first.
--    Locks: ACCESS SHARE on tag/packets/tag_name (does not block writers),
--    plain inserts into stream_tag.
INSERT INTO stream_tag (stream_id, tag_name)
SELECT DISTINCT p.stream_id, tn.name
FROM tag t
JOIN packets p ON p.id = t.packet_id AND p.timestamp = t.timestamp
JOIN tag_name tn ON tn.id = t.tag_id
WHERE tn.name IS NOT NULL
ON CONFLICT (stream_id, tag_name) DO NOTHING;

-- 4) Teardown of the matview machinery. Verified consumers (repo-wide
--    grep, monitoring included): backend stream_repository
--    (get_streams_with_tags, get_stream_histogram — now rewritten), the
--    backend LISTEN 'refresh_streams' task (removed), traffic-analyzer
--    pg_notify call (removed). No Grafana/test/agent references.
DROP MATERIALIZED VIEW IF EXISTS streams_list_view;
DROP FUNCTION IF EXISTS refresh_streams_view();

COMMIT;

-- Post-migration sanity checks:
--   SELECT count(*) FROM stream_tag;                    -- ~= distinct pairs
--   SELECT indexname FROM pg_indexes WHERE tablename = 'stream';
--   \d streams_list_view                                -- should not exist
