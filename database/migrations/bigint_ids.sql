-- OPTIONAL OFFLINE MIGRATION: int4 -> bigint ids
--
-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║  DO NOT RUN THIS ON A LIVE GAME / WITH INGEST RUNNING.               ║
-- ║  This file is NOT wired into any startup path on purpose.             ║
-- ╚══════════════════════════════════════════════════════════════════════╝
--
-- WHY OFFLINE ONLY: `ALTER COLUMN ... TYPE BIGINT` on int4 columns REWRITES
-- every row and takes an ACCESS EXCLUSIVE lock on each altered table. On
-- the partitioned parents (packets/tag/http_message/grpc_message) PostgreSQL
-- (>= 11) recurses into ALL existing partitions, so each statement locks
-- parent + every partition and rewrites them — at 100k pkt/s that stops
-- ingestion for the whole rewrite. That is exactly the crash class this
-- change exists to prevent, so the fix ships via database/init/*.sql for new
-- deployments (per-game re-init per DEPLOY.md), and this script exists only
-- for upgrading a long-lived DB between games.
--
-- HOW PG PROPAGATES THE TYPE CHANGE ON PARTITIONED TABLES:
--   ALTER TABLE <parent> ALTER COLUMN ... TYPE BIGINT;
--   is applied recursively to all partitions (no explicit CASCADE keyword is
--   needed or accepted for column type changes; recursion is automatic for
--   partitions). Each partition is rewritten under ACCESS EXCLUSIVE lock;
--   the partition key column itself (timestamp) is NOT touched here.
--   Parent indexes (incl. the composite PK (id, timestamp)) are rebuilt
--   automatically on every partition.
--
-- FK ORDER: alter referenced tables before referencing ones (tag_name before
-- tag, service before stream/agent_service/proto_file) so each constraint is
-- validated once, after both sides are bigint.
--
-- MATERIALIZED VIEW: streams_list_view's stored columns pin the old int4
-- types ("cannot alter type of a column used by a view or rule"), so the
-- view is dropped up front and recreated (same definition as
-- database/init/09_streams_list_view.sql) at the end; CREATE ... AS
-- re-derives the column types (now bigint) and repopulates it. The
-- refresh_streams_view() function survives the drop and needs no recreate.
--
-- Run it BETWEEN GAMES with analyzers/backend stopped (approximate duration
-- scales with table sizes; test on a copy first):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/migrations/bigint_ids.sql
--
-- Sequences: SERIAL sequences are created with default AS bigint already
-- (the int4 limit came from the COLUMN type, not the sequence), but ALTER
-- SEQUENCE ... AS BIGINT is issued anyway to make the state explicit.

BEGIN;

DROP MATERIALIZED VIEW streams_list_view;

-- Referenced (lookup) tables first
ALTER TABLE tag_name      ALTER COLUMN id TYPE BIGINT;
ALTER TABLE service       ALTER COLUMN id TYPE BIGINT;
ALTER TABLE header_set    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE grpc_body_set ALTER COLUMN id TYPE BIGINT;

-- stream: PK + FK to service
ALTER TABLE stream ALTER COLUMN id TYPE BIGINT;
ALTER TABLE stream ALTER COLUMN service_id TYPE BIGINT;

-- Tables referencing stream/service
ALTER TABLE agent_service ALTER COLUMN service_id TYPE BIGINT;
ALTER TABLE proto_file    ALTER COLUMN id TYPE BIGINT,
                         ALTER COLUMN service_id TYPE BIGINT;

-- Partitioned high-volume tables: each statement recurses into ALL
-- partitions, rewrites them and rebuilds indexes under ACCESS EXCLUSIVE
-- lock — the reason this file must never run mid-game.
ALTER TABLE packets ALTER COLUMN id TYPE BIGINT,
                    ALTER COLUMN stream_id TYPE BIGINT;
ALTER TABLE tag ALTER COLUMN id TYPE BIGINT,
                ALTER COLUMN packet_id TYPE BIGINT,
                ALTER COLUMN tag_id TYPE BIGINT;
ALTER TABLE http_message ALTER COLUMN id TYPE BIGINT,
                        ALTER COLUMN stream_id TYPE BIGINT,
                        ALTER COLUMN packet_id TYPE BIGINT,
                        ALTER COLUMN header_set_id TYPE BIGINT;
ALTER TABLE grpc_message ALTER COLUMN id TYPE BIGINT,
                        ALTER COLUMN stream_id TYPE BIGINT,
                        ALTER COLUMN http_message_id TYPE BIGINT,
                        ALTER COLUMN body_set_id TYPE BIGINT;

-- Make owned sequences explicitly bigint (SERIAL ones already are by
-- default; this is belt-and-braces and a no-op rewrite-wise).
ALTER SEQUENCE IF EXISTS stream_id_seq        AS BIGINT;
ALTER SEQUENCE IF EXISTS packets_id_seq       AS BIGINT;
ALTER SEQUENCE IF EXISTS tag_name_id_seq      AS BIGINT;
ALTER SEQUENCE IF EXISTS tag_id_seq           AS BIGINT;
ALTER SEQUENCE IF EXISTS service_id_seq       AS BIGINT;
ALTER SEQUENCE IF EXISTS header_set_id_seq    AS BIGINT;
ALTER SEQUENCE IF EXISTS grpc_body_set_id_seq AS BIGINT;
ALTER SEQUENCE IF EXISTS grpc_message_id_seq  AS BIGINT;
ALTER SEQUENCE IF EXISTS http_message_id_seq  AS BIGINT;
ALTER SEQUENCE IF EXISTS proto_file_id_seq    AS BIGINT;

-- Recreate the materialized view (same definition as
-- database/init/09_streams_list_view.sql); CREATE ... AS re-derives the
-- column types (bigint now) and populates it.
CREATE MATERIALIZED VIEW streams_list_view AS
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
    COALESCE(str.first_packet_time, 0) AS first_time,
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

CREATE UNIQUE INDEX idx_slv_stream_id ON streams_list_view(stream_id);
CREATE INDEX idx_slv_last_time ON streams_list_view(last_time DESC);
CREATE INDEX idx_slv_service_id ON streams_list_view(service_id);

COMMIT;

-- Post-migration sanity check (all should report "bigint"):
--   SELECT table_name, column_name, data_type FROM information_schema.columns
--   WHERE column_name IN ('id','packet_id','stream_id','tag_id','service_id',
--                         'header_set_id','http_message_id','body_set_id')
--     AND table_name IN ('packets','tag','tag_name','http_message',
--                        'grpc_message','grpc_body_set','stream','service',
--                        'agent_service','header_set','proto_file')
--   ORDER BY table_name, column_name;
