-- Partition management functions

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
        EXCEPTION WHEN duplicate_table THEN
            NULL; -- already exists
        END;
        boundary := boundary + interval_secs;
    END LOOP;
    RETURN created;
END;
$$ LANGUAGE plpgsql;

-- Drop partitions older than a given timestamp
CREATE OR REPLACE FUNCTION drop_old_partitions(
    parent_table TEXT,
    before_ts BIGINT
) RETURNS INT AS $$
DECLARE
    r RECORD;
    dropped INT := 0;
    part_ts BIGINT;
    matches TEXT[];
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
            IF part_ts < before_ts THEN
                EXECUTE format('DROP TABLE IF EXISTS %I', r.partition_name);
                dropped := dropped + 1;
            END IF;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$ LANGUAGE plpgsql;

-- Ensure partitions exist for current time window (all partitioned tables)
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
    total := total + create_time_partitions('packets', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('tag', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('http_message', from_ts, to_ts, interval_secs);
    total := total + create_time_partitions('http_header', from_ts, to_ts, interval_secs);
    RETURN total;
END;
$$ LANGUAGE plpgsql;

-- Cleanup: drop all old partitions and orphaned streams
CREATE OR REPLACE FUNCTION cleanup_old_data(retention_minutes INT DEFAULT 20)
RETURNS TABLE(partitions_dropped INT, streams_deleted BIGINT) AS $$
DECLARE
    cutoff_ts BIGINT := EXTRACT(EPOCH FROM NOW())::BIGINT - (retention_minutes * 60);
    dropped INT := 0;
    streams_removed BIGINT;
BEGIN
    -- Drop old partitions (instant, no VACUUM needed)
    dropped := dropped + drop_old_partitions('http_header', cutoff_ts);
    dropped := dropped + drop_old_partitions('http_message', cutoff_ts);
    dropped := dropped + drop_old_partitions('tag', cutoff_ts);
    dropped := dropped + drop_old_partitions('packets', cutoff_ts);

    -- Delete streams with no remaining packets (excluding favorites)
    DELETE FROM stream
    WHERE NOT favorite
      AND id NOT IN (SELECT DISTINCT stream_id FROM packets);
    GET DIAGNOSTICS streams_removed = ROW_COUNT;

    partitions_dropped := dropped;
    streams_deleted := streams_removed;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions (1 hour back, 2 hours ahead)
SELECT ensure_current_partitions(60, 120, 300);
