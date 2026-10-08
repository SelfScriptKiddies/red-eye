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
CREATE OR REPLACE FUNCTION drop_old_partitions(
    parent_table TEXT,
    before_ts BIGINT,
    interval_secs INT DEFAULT 300
) RETURNS INT AS $$
DECLARE
    r RECORD;
    dropped INT := 0;
    part_ts BIGINT;
    matches TEXT[];
    protected_from BIGINT := (EXTRACT(EPOCH FROM NOW())::BIGINT / interval_secs) * interval_secs;
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
                EXECUTE format('DROP TABLE IF EXISTS %I', r.partition_name);
                dropped := dropped + 1;
            END IF;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$ LANGUAGE plpgsql;

-- Bounded batched DELETE of rows older than before_ts (ctid chunks).
-- Used for the DEFAULT partitions (they never age out via partition drops)
-- and, under disk pressure, for old rows of the current write window.
-- Chunking keeps every transaction short so a rare large default partition
-- (long outage + skewed clocks) cannot produce one giant locked DELETE.
-- NOTE: this relies on the assumption that skewed-timestamp volume is small
-- (clock drift, not bulk traffic); normal traffic never lands here.
CREATE OR REPLACE FUNCTION delete_old_rows_batched(
    target_table TEXT,   -- already-quoted table name (regclass or "name")
    before_ts BIGINT,
    batch_size INT DEFAULT 10000
) RETURNS BIGINT AS $$
DECLARE
    total BIGINT := 0;
    removed INT;
BEGIN
    LOOP
        EXECUTE format(
            'DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE timestamp < %s LIMIT %s)',
            target_table, target_table, before_ts, batch_size
        );
        GET DIAGNOSTICS removed = ROW_COUNT;
        total := total + removed;
        EXIT WHEN removed < batch_size;
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
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_minutes INT DEFAULT 20)
RETURNS TABLE(partitions_dropped INT, streams_deleted BIGINT) AS $$
DECLARE
    cutoff_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT - (retention_minutes * 60);
    dropped INT := 0;
    streams_removed BIGINT;
    t TEXT;
BEGIN
    -- Drop old partitions (instant, no VACUUM needed)
    dropped := dropped + drop_old_partitions('grpc_message', cutoff_ts);
    dropped := dropped + drop_old_partitions('http_message', cutoff_ts);
    dropped := dropped + drop_old_partitions('tag', cutoff_ts);
    dropped := dropped + drop_old_partitions('packets', cutoff_ts);

    -- Prune old skewed rows from DEFAULT partitions (bounded batches).
    PERFORM ensure_default_partitions();
    FOREACH t IN ARRAY ARRAY['packets', 'tag', 'http_message', 'grpc_message'] LOOP
        PERFORM delete_old_rows_batched(quote_ident(t || '_default'), cutoff_ts);
    END LOOP;

    -- Cleanup orphaned header/body sets
    DELETE FROM header_set hs
    WHERE NOT EXISTS (
        SELECT 1 FROM http_message hm WHERE hm.header_set_id = hs.id
    );
    DELETE FROM grpc_body_set gbs
    WHERE NOT EXISTS (
        SELECT 1 FROM grpc_message gm WHERE gm.body_set_id = gbs.id
    );

    -- Delete streams with no remaining packets (excluding favorites)
    DELETE FROM stream s
    WHERE NOT s.favorite
      AND NOT EXISTS (SELECT 1 FROM packets p WHERE p.stream_id = s.id);
    GET DIAGNOSTICS streams_removed = ROW_COUNT;

    partitions_dropped := dropped;
    streams_deleted := streams_removed;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Disk-pressure helper: delete OLD rows from the current write window of a
-- partitioned table (parent routes the DELETE to the relevant partitions;
-- the timestamp index makes the ctid subquery cheap per partition).
-- keep_seconds MUST be well above the batch flusher's buffer window so the
-- writer never loses its fresh-data landing zone (bounded degradation: we
-- sacrifice old rows, never the INSERT path).
CREATE OR REPLACE FUNCTION trim_old_rows_from_current(
    parent_table TEXT,
    keep_seconds INT DEFAULT 300,
    batch_size INT DEFAULT 10000
) RETURNS BIGINT AS $$
DECLARE
    before_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT - keep_seconds;
BEGIN
    RETURN delete_old_rows_batched(parent_table, before_ts, batch_size);
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions (1 hour back, 2 hours ahead) + DEFAULT partitions
SELECT ensure_current_partitions(60, 120, 300);
