-- ONLINE MIGRATION: starred stream retention (favorites survive cleanup).
--
-- Adds stream retention metadata, non-partitioned saved_* twins of the
-- partitioned content tables, favorite-related indexes and the capacity
-- settings. Star/unstar/archive/cleanup workers (backend) and segment
-- rotation (traffic-analyzer) depend on every object created here.
--
-- Can run on a LIVE database (ingest up, game running):
--   * ALTER TABLE ADD COLUMN takes a brief ACCESS EXCLUSIVE lock that only
--     waits for in-flight queries; no table rewrite (all defaults are
--     constants or NULL).
--   * The CHECK constraint is validated on existing rows; stream is small
--     (bounded by connection churn), so the scan is fast.
--   * CREATE INDEX CONCURRENTLY on stream - no write blocking.
--   * saved_* tables are new and empty - plain CREATE TABLE.
--
-- CAVEAT (CONCURRENTLY cannot run inside a transaction block):
--   Run with plain `psql -f`, NOT wrapped in BEGIN/COMMIT or
--   --single-transaction.
--
-- Idempotent: safe to re-run (IF EXISTS / IF NOT EXISTS / pg_constraint
-- guards).

-- ── Stream retention metadata ───────────────────────────────────────────

ALTER TABLE stream ADD COLUMN IF NOT EXISTS favorite_since TIMESTAMPTZ;
-- Monotonic per-stream token: a late archive/cleanup worker from an older
-- star compares it before touching anything.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS favorite_revision BIGINT NOT NULL DEFAULT 0;
-- Highest packet id known to be represented in saved_packet. Content rows
-- (tag/http/grpc) of packets <= this id are archived in the same commit.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS archived_through_packet_id BIGINT NOT NULL DEFAULT 0;
-- live | archiving | archived | incomplete.
-- archiving on a favorite=false row means "saved content awaiting bounded
-- deletion" (post-unstar pending cleanup), not an active archive.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS retention_state TEXT NOT NULL DEFAULT 'live';
-- FK-free on purpose: the previous segment may be deleted by ordinary
-- orphan cleanup while this row lives on; a dangling link renders as none.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS continuation_of BIGINT;
ALTER TABLE stream ADD COLUMN IF NOT EXISTS segment_index INT NOT NULL DEFAULT 1;
-- Running payload-byte estimate for the stream (maintained by the
-- analyzer flush). Estimate only: capacity accounting treats it as a
-- lower bound and adds reservation margin on top.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS retention_bytes BIGINT NOT NULL DEFAULT 0;
-- Last archive batch failure, sanitized (ids and byte counts only,
-- never payloads). NULL = healthy.
ALTER TABLE stream ADD COLUMN IF NOT EXISTS archive_error TEXT;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'stream_retention_state_check'
    ) THEN
        ALTER TABLE stream ADD CONSTRAINT stream_retention_state_check
            CHECK (retention_state IN ('live', 'archiving', 'archived', 'incomplete'));
    END IF;
END $$;

-- Favorite keyset listing: same ordering/cursor expressions as the main
-- streams list but restricted to favorites (partial index stays tiny).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_favorite_list
    ON stream (COALESCE(last_packet_time, 0) DESC, id DESC) WHERE favorite;

-- Worker scans (archive loop, cleanup guard, capacity accounting).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_favorite_state
    ON stream (favorite, retention_state);

-- Continuation chain walks (prev/next segment links in the UI).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stream_continuation
    ON stream (continuation_of) WHERE continuation_of IS NOT NULL;

-- ── Saved tables (non-partitioned, PK = original id) ────────────────────
-- Mirror the source row identity: the archive worker copies rows verbatim
-- (ON CONFLICT (id) DO NOTHING keeps retries idempotent), and read paths
-- union live + saved deduplicating by the original id. Timestamps stay
-- original so ordering and body offsets keep working unchanged.

CREATE TABLE IF NOT EXISTS saved_packet (
    id BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    timestamp_microseconds BIGINT NOT NULL,
    direction VARCHAR(4) NOT NULL,
    stream_id BIGINT NOT NULL,
    packet_data BYTEA NOT NULL
);

-- Ordered reads and per-stream archive/cleanup batches.
CREATE INDEX IF NOT EXISTS idx_saved_packet_stream_ts ON saved_packet (stream_id, timestamp);

CREATE TABLE IF NOT EXISTS saved_tag (
    id BIGINT PRIMARY KEY,
    packet_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Mirror of the live tag FK: removing a tag_name removes its saved
    -- matches too, so saved and live tag semantics never diverge.
    CONSTRAINT fk_saved_tag
        FOREIGN KEY (tag_id)
            REFERENCES tag_name(id)
            ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saved_tag_stream_ts ON saved_tag (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_tag_packet ON saved_tag (packet_id);

CREATE TABLE IF NOT EXISTS saved_http_message (
    id BIGINT PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    packet_id BIGINT NOT NULL,
    is_request BOOLEAN NOT NULL,
    method VARCHAR(10),
    path TEXT,
    status_code SMALLINT,
    http_version VARCHAR(10),
    content_type VARCHAR(255),
    content_length INTEGER,
    body_offset INTEGER,
    body_length INTEGER,
    header_set_id BIGINT,
    timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_http_stream_ts ON saved_http_message (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_http_packet ON saved_http_message (packet_id);
-- Orphan cleanup of header_set must see saved references (header_set and
-- grpc_body_set stay shared between live and saved rows).
CREATE INDEX IF NOT EXISTS idx_saved_http_header_set ON saved_http_message (header_set_id);

CREATE TABLE IF NOT EXISTS saved_grpc_message (
    id BIGINT PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    http_message_id BIGINT NOT NULL,
    service_name VARCHAR(255),
    method_name VARCHAR(255),
    is_request BOOLEAN NOT NULL,
    body_set_id BIGINT,
    timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_grpc_stream_ts ON saved_grpc_message (stream_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_saved_grpc_http ON saved_grpc_message (http_message_id);

-- ── Capacity settings ───────────────────────────────────────────────────
-- Env (FAVORITE_MAX_TOTAL_BYTES / FAVORITE_SEGMENT_BYTES /
-- FAVORITE_DISK_RESERVE_BYTES) seeds the initial values at backend startup;
-- these rows are the runtime overrides. DO NOTHING keeps operator values.

INSERT INTO app_settings (key, value) VALUES
    ('favorite_max_total_bytes', '10737418240'),
    ('favorite_segment_bytes', '268435456'),
    ('favorite_disk_reserve_bytes', '2147483648')
ON CONFLICT (key) DO NOTHING;

-- ── Cleanup guard functions ─────────────────────────────────────────────
-- These REPLACE the versions from database/init/06_partitions.sql so a live
-- database picks up favorite-safe cleanup together with the schema. The
-- 5-minute partition interval (300s) must stay in sync with the Rust
-- cleanup loop (PARTITION_INTERVAL_SECS).

-- True when the half-open [lo, hi) window of a content table still holds
-- rows of starred streams with no saved copy. Presence in the saved twin
-- is the only authoritative proof; progress markers cannot bypass it.
CREATE OR REPLACE FUNCTION partition_has_unarchived_favorites(
    parent_table TEXT,
    lo BIGINT,
    hi BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
    found BOOLEAN := false;
BEGIN
    IF parent_table = 'packets' THEN
        SELECT EXISTS (
            SELECT 1 FROM packets p
            JOIN stream s ON s.id = p.stream_id
            WHERE s.favorite
              AND p.timestamp >= lo AND p.timestamp < hi
              AND NOT EXISTS (SELECT 1 FROM saved_packet sp WHERE sp.id = p.id)
        ) INTO found;
    ELSIF parent_table = 'tag' THEN
        SELECT EXISTS (
            SELECT 1 FROM tag t
            JOIN stream s ON s.id = t.stream_id
            WHERE s.favorite
              AND t.timestamp >= lo AND t.timestamp < hi
              AND NOT EXISTS (SELECT 1 FROM saved_tag st WHERE st.id = t.id)
        ) INTO found;
    ELSIF parent_table = 'http_message' THEN
        SELECT EXISTS (
            SELECT 1 FROM http_message m
            JOIN stream s ON s.id = m.stream_id
            WHERE s.favorite
              AND m.timestamp >= lo AND m.timestamp < hi
              AND NOT EXISTS (SELECT 1 FROM saved_http_message sm WHERE sm.id = m.id)
        ) INTO found;
    ELSIF parent_table = 'grpc_message' THEN
        SELECT EXISTS (
            SELECT 1 FROM grpc_message g
            JOIN stream s ON s.id = g.stream_id
            WHERE s.favorite
              AND g.timestamp >= lo AND g.timestamp < hi
              AND NOT EXISTS (SELECT 1 FROM saved_grpc_message sg WHERE sg.id = g.id)
        ) INTO found;
    END IF;
    RETURN found;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_old_partitions(
    parent_table TEXT,
    before_ts BIGINT,
    interval_secs INT DEFAULT 300
) RETURNS TABLE(dropped INT, guard_skipped INT) AS $$
DECLARE
    r RECORD;
    dropped_count INT := 0;
    skipped_count INT := 0;
    part_ts BIGINT;
    matches TEXT[];
    protected_from BIGINT := (EXTRACT(EPOCH FROM NOW())::BIGINT / interval_secs) * interval_secs;
    guarded BOOLEAN := parent_table IN ('packets', 'tag', 'http_message', 'grpc_message');
BEGIN
    FOR r IN
        SELECT c.relname AS partition_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = parent_table::regclass
    LOOP
        matches := regexp_match(r.partition_name, '_p(\d+)$');
        IF matches IS NOT NULL THEN
            part_ts := matches[1]::BIGINT;
            IF part_ts < protected_from AND part_ts < before_ts THEN
                IF guarded AND partition_has_unarchived_favorites(
                    parent_table, part_ts, part_ts + interval_secs
                ) THEN
                    skipped_count := skipped_count + 1;
                    RAISE WARNING 'partition % of % retained: starred streams have unarchived rows in it', r.partition_name, parent_table;
                    CONTINUE;
                END IF;
                EXECUTE format('DROP TABLE IF EXISTS %I', r.partition_name);
                dropped_count := dropped_count + 1;
            END IF;
        END IF;
    END LOOP;
    dropped := dropped_count;
    guard_skipped := skipped_count;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Bounded batched DELETE with the favorite guard: rows of starred streams
-- without a saved copy are excluded from every chunk. only_default=true
-- restricts the pass to the <t>_default partition (DEFAULT pruning).
CREATE OR REPLACE FUNCTION delete_old_rows_batched(
    base_table TEXT,
    only_default BOOLEAN,
    before_ts BIGINT,
    batch_size INT DEFAULT 10000
) RETURNS BIGINT AS $$
DECLARE
    total BIGINT := 0;
    removed INT;
    chunks INT := 0;
    guarded BOOLEAN;
    twin TEXT := CASE base_table
        WHEN 'packets' THEN 'saved_packet'
        WHEN 'tag' THEN 'saved_tag'
        WHEN 'http_message' THEN 'saved_http_message'
        WHEN 'grpc_message' THEN 'saved_grpc_message'
    END;
    target TEXT := base_table || (CASE WHEN only_default THEN '_default' ELSE '' END);
BEGIN
    IF twin IS NULL THEN
        RAISE EXCEPTION 'delete_old_rows_batched: unsupported table %', base_table;
    END IF;
    SELECT EXISTS (SELECT 1 FROM stream WHERE favorite) INTO guarded;
    LOOP
        IF guarded THEN
            EXECUTE format(
                'DELETE FROM %I d WHERE d.ctid IN (
                    SELECT d2.ctid FROM %I d2
                    WHERE d2.timestamp < %s
                      AND NOT EXISTS (
                        SELECT 1 FROM stream s
                        WHERE s.id = d2.stream_id AND s.favorite
                          AND NOT EXISTS (
                            SELECT 1 FROM %I sp WHERE sp.id = d2.packet_id
                          )
                      )
                    LIMIT %s
                )',
                target, target, before_ts, twin, batch_size
            );
        ELSE
            EXECUTE format(
                'DELETE FROM %I WHERE ctid IN (
                    SELECT ctid FROM %I WHERE timestamp < %s LIMIT %s
                )',
                target, target, before_ts, batch_size
            );
        END IF;
        GET DIAGNOSTICS removed = ROW_COUNT;
        total := total + removed;
        chunks := chunks + 1;
        EXIT WHEN removed = 0 OR chunks >= 100;
        PERFORM pg_sleep(0.05);
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_old_data(retention_minutes INT DEFAULT 20)
RETURNS TABLE(partitions_dropped INT, guard_skipped INT, streams_deleted BIGINT) AS $$
DECLARE
    cutoff_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT - (retention_minutes * 60);
    dropped INT := 0;
    skipped INT := 0;
    part_result RECORD;
    streams_removed BIGINT;
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['grpc_message', 'http_message', 'tag', 'packets'] LOOP
        SELECT * INTO part_result FROM drop_old_partitions(t, cutoff_ts);
        dropped := dropped + part_result.dropped;
        skipped := skipped + part_result.guard_skipped;
    END LOOP;

    PERFORM ensure_default_partitions();
    FOREACH t IN ARRAY ARRAY['packets', 'tag', 'http_message', 'grpc_message'] LOOP
        PERFORM delete_old_rows_batched(t, true, cutoff_ts);
    END LOOP;

    -- Shared header/body sets survive while live OR saved rows reference
    -- them (the archive worker revives sets deleted in a race window).
    DELETE FROM header_set hs
    WHERE NOT EXISTS (
        SELECT 1 FROM http_message hm WHERE hm.header_set_id = hs.id
    )
      AND NOT EXISTS (
        SELECT 1 FROM saved_http_message shm WHERE shm.header_set_id = hs.id
    );
    DELETE FROM grpc_body_set gbs
    WHERE NOT EXISTS (
        SELECT 1 FROM grpc_message gm WHERE gm.body_set_id = gbs.id
    )
      AND NOT EXISTS (
        SELECT 1 FROM saved_grpc_message sgm WHERE sgm.body_set_id = gbs.id
    );

    -- A stream row is orphaned only when neither live nor saved content
    -- remains (post-unstar pending cleanup keeps the row alive).
    DELETE FROM stream s
    WHERE NOT s.favorite
      AND NOT EXISTS (SELECT 1 FROM packets p WHERE p.stream_id = s.id)
      AND NOT EXISTS (SELECT 1 FROM saved_packet sp WHERE sp.stream_id = s.id)
      AND NOT EXISTS (SELECT 1 FROM saved_tag st WHERE st.stream_id = s.id)
      AND NOT EXISTS (SELECT 1 FROM saved_http_message shm WHERE shm.stream_id = s.id)
      AND NOT EXISTS (SELECT 1 FROM saved_grpc_message sgm WHERE sgm.stream_id = s.id);
    GET DIAGNOSTICS streams_removed = ROW_COUNT;

    partitions_dropped := dropped;
    guard_skipped := skipped;
    streams_deleted := streams_removed;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION trim_old_rows_from_current(
    parent_table TEXT,
    keep_seconds INT DEFAULT 300,
    batch_size INT DEFAULT 10000
) RETURNS BIGINT AS $$
DECLARE
    before_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT - keep_seconds;
BEGIN
    RETURN delete_old_rows_batched(parent_table, false, before_ts, batch_size);
END;
$$ LANGUAGE plpgsql;

-- Post-migration sanity checks:
--   SELECT retention_state, COUNT(*) FROM stream WHERE favorite GROUP BY 1;
--   SELECT indexname FROM pg_indexes WHERE tablename = 'stream' AND indexname LIKE 'idx_stream_%';
--   \d+ saved_packet
--   SELECT drop_old_partitions('packets', 0); -- must not drop favorites' partitions
