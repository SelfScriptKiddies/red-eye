-- Partition management functions
--
-- ANTI-CRASH INVARIANTS (degradation beats failure: lose a row, never the
-- ingest path):
--
-- 1. Every partitioned table has a DEFAULT partition. An INSERT whose
--    timestamp falls outside all ensured range partitions (agent/DB clock
--    skew beyond the ensured window, backend down for hours) then lands in
--    <t>_default instead of erroring — one such error used to drop the whole
--    500-packet batch after retry (batch_flusher has no DLQ).
--
-- 2. drop_old_partitions NEVER drops the partition containing now() nor any
--    future partition — even at retention 0 / disk pressure the writer always
--    has a landing zone. The protected floor uses the same interval and
--    boundary math as ensure_current_partitions (see protected_from there).
--
-- 3. OVERLAP HAZARD: creating a range partition FAILS if rows for that range
--    already sit in the DEFAULT partition (PG must re-check the default's
--    implied constraint, SQLSTATE 23514). We deliberately do NOT move rows
--    automatically: the failure is contained as a WARNING naming the
--    partition, and DEFAULT keeps catching those rows until an operator
--    resolves the overlap (move rows out, then let the next ensure tick
--    create the partition).

-- Create time-range partitions for a table
CREATE OR REPLACE FUNCTION create_time_partitions(
    parent_table TEXT,
    from_ts BIGINT,
    to_ts BIGINT,
    interval_secs INT DEFAULT 300
) RETURNS INT AS $$
DECLARE
    boundary BIGINT;
    part_name TEXT;
    created INT := 0;
BEGIN
    boundary := (from_ts / interval_secs) * interval_secs;
    WHILE boundary < to_ts LOOP
        part_name := parent_table || '_p' || boundary;
        BEGIN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                part_name, parent_table, boundary, boundary + interval_secs
            );
            created := created + 1;
        EXCEPTION
            WHEN duplicate_table THEN
                NULL; -- already exists
            WHEN check_violation THEN
                -- Rows for this range already live in the DEFAULT partition:
                -- keep them there (degradation, not crash) and let the next
                -- ensure tick retry once an operator has moved them out.
                RAISE WARNING 'partition % not created: rows for its range already live in the DEFAULT partition of % (leaving DEFAULT to catch them; no automatic row moves)', part_name, parent_table;
        END;
        boundary := boundary + interval_secs;
    END LOOP;
    RETURN created;
END;
$$ LANGUAGE plpgsql;

-- Ensure DEFAULT partitions exist for all partitioned tables (idempotent).
-- Pure catalog check when everything exists — no locks on the data path.
CREATE OR REPLACE FUNCTION ensure_default_partitions() RETURNS INT AS $$
DECLARE
    t TEXT;
    created INT := 0;
BEGIN
    FOREACH t IN ARRAY ARRAY['packets', 'tag', 'http_message', 'grpc_message'] LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = t::regclass
              AND c.relname = t || '_default'
        ) THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF %I DEFAULT',
                           t || '_default', t);
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END;
$$ LANGUAGE plpgsql;

-- Drop partitions older than a given timestamp.
--
-- INVARIANT: the partition covering now() and every future partition are
-- never dropped, even when before_ts >= now() (retention 0 / disk pressure).
-- protected_from is the start of the CURRENT partition, computed with the
-- SAME bounds logic ensure_current_partitions uses to create partitions:
-- interval_secs MUST match the interval used at creation time (default 300).
-- Partitions with part_ts >= protected_from are the current and future ones.
--
-- FAVORITE GUARD: partitions of the content tables are checked for rows of
-- starred streams that have no saved copy yet. If any such row exists the
-- partition is retained (counted in `skipped`) - degradation, never the
-- deletion of unprotected favorite content. Presence in the saved twin is
-- the only authoritative proof, so no progress marker can bypass this.
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

-- Bounded batched DELETE of rows older than before_ts (ctid chunks).
-- Used for the DEFAULT partitions (they never age out via partition drops)
-- and, under disk pressure, for old rows of the current write window.
-- Chunking keeps every transaction short so a rare large default partition
-- (long outage + skewed clocks) cannot produce one giant locked DELETE.
-- NOTE: this relies on the assumption that skewed-timestamp volume is small
-- (clock drift, not bulk traffic); normal traffic never lands here.
--
-- FAVORITE GUARD: rows of starred streams without a saved copy are
-- EXCLUDED from every chunk (the twin lookup is a PK probe). The guard is
-- active only while at least one starred stream exists, so the common
-- no-favorites path keeps its original cost.
CREATE OR REPLACE FUNCTION delete_old_rows_batched(
    base_table TEXT,      -- packets | tag | http_message | grpc_message
    only_default BOOLEAN, -- true = restrict to the <t>_default partition
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
    -- Bounded per call: chunks stop when a full chunk yields no deletable
    -- row (a favorite-protected wall) or the chunk cap is reached; the next
    -- call resumes. Excluded rows make "removed < batch_size" a normal
    -- outcome, so progress - not chunk fullness - drives the loop.
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
        -- Small pause between chunks to stay friendly to concurrent writers.
        PERFORM pg_sleep(0.05);
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;

-- Ensure partitions exist for current time window (all partitioned tables).
-- Also ensures the DEFAULT partitions exist (invariant 1 above).
CREATE OR REPLACE FUNCTION ensure_current_partitions(
    behind_minutes INT DEFAULT 60,
    ahead_minutes INT DEFAULT 60,
    interval_secs INT DEFAULT 300
) RETURNS INT AS $$
DECLARE
    now_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT;
    from_ts BIGINT := now_ts - (behind_minutes * 60);
    to_ts BIGINT := now_ts + (ahead_minutes * 60);
    total INT := 0;
BEGIN
    total := total + ensure_default_partitions();
    total := total + create_time_partitions('packets', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('tag', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('http_message', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('grpc_message', from_ts, to_ts, interval_secs);
    RETURN total;
END;
$$ LANGUAGE plpgsql;

-- Cleanup: drop old partitions, prune old rows from the DEFAULT partitions
-- and delete orphaned streams.
-- DEFAULT partitions accumulate rows that missed every range partition
-- (clock skew, outages) and would otherwise never be cleaned: prune them
-- with the SAME retention cutoff, in bounded batches.
-- The current/future partitions are protected inside drop_old_partitions,
-- so even cleanup_old_data(0) (disk pressure) cannot drop the write target.
--
-- FAVORITE GUARDS: unarchived favorite rows retain their partitions and are
-- excluded from chunked deletes; header/body set orphan cleanup considers
-- saved references; a stream row is only deleted once no live AND no saved
-- content remains.
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
    -- Drop old partitions (instant, no VACUUM needed); guard skips are
    -- reported per partition so the operator sees the retention backlog.
    FOREACH t IN ARRAY ARRAY['grpc_message', 'http_message', 'tag', 'packets'] LOOP
        SELECT * INTO part_result FROM drop_old_partitions(t, cutoff_ts);
        dropped := dropped + part_result.dropped;
        skipped := skipped + part_result.guard_skipped;
    END LOOP;

    -- Prune old skewed rows from DEFAULT partitions (bounded batches).
    PERFORM ensure_default_partitions();
    FOREACH t IN ARRAY ARRAY['packets', 'tag', 'http_message', 'grpc_message'] LOOP
        PERFORM delete_old_rows_batched(t, true, cutoff_ts);
    END LOOP;

    -- Cleanup orphaned header/body sets. Sets referenced by SAVED rows of
    -- starred (or pending-cleanup) streams must survive alongside the ones
    -- referenced by live messages.
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

    -- Delete streams with no remaining packets (excluding favorites and
    -- streams whose saved content is still pending post-unstar cleanup).
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

-- Disk-pressure helper: delete OLD rows from the current write window of a
-- partitioned table (parent routes the DELETE to the relevant partitions;
-- the timestamp index makes the ctid subquery cheap per partition).
-- keep_seconds MUST be well above the batch flusher's buffer window so the
-- writer never loses its fresh-data landing zone (bounded degradation: we
-- sacrifice old rows, never the INSERT path). Favorite rows without a
-- saved copy are excluded by delete_old_rows_batched.
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

-- Create initial partitions (1 hour back, 2 hours ahead) + DEFAULT partitions
SELECT ensure_current_partitions(60, 120, 300);
