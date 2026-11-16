-- Service transport protocol + global/explicit agent assignment
--
-- Adds service.protocol ('TCP'/'UDP') and service.is_global, and widens
-- service identity from (name, port) to (name, port, protocol). Existing
-- rows migrate as TCP and global, matching the pre-migration behavior
-- (every service was broadcast to every agent).
--
-- ONLINE RUNBOOK (packet ingestion stays live; run between rounds if
-- possible, but no step holds a long exclusive lock):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--        -f database/migrations/add_service_protocol_and_assignment.sql
--
-- NOTES:
--   - ADD COLUMN with a constant DEFAULT is metadata-only (PG 11+): no
--     rewrite, no long lock.
--   - The replacement unique index is built CONCURRENTLY (no write block),
--     then attached as a constraint. CREATE INDEX CONCURRENTLY cannot run
--     inside a transaction, so this file must stay outside BEGIN/COMMIT.
--   - Idempotent: every statement is safe to re-run.
--
-- The traffic-analyzer keeps inserting packets throughout: stream.service_id
-- values are untouched and the new columns carry defaults.

-- 1. Transport protocol. Existing rows and any analyzer written before the
--    upgrade read TCP, which is what they always handled.
ALTER TABLE service
    ADD COLUMN IF NOT EXISTS protocol TEXT NOT NULL DEFAULT 'TCP'
    CHECK (protocol IN ('TCP', 'UDP'));

-- 2. Global assignment flag. TRUE = applies to all current and future
--    agents; FALSE = agent_service holds the complete explicit set.
ALTER TABLE service
    ADD COLUMN IF NOT EXISTS is_global BOOLEAN NOT NULL DEFAULT TRUE;

-- 3. Replace UNIQUE (name, port) with UNIQUE (name, port, protocol) so a
--    TCP and a UDP service may share name and port as separate rows.
ALTER TABLE service DROP CONSTRAINT IF EXISTS unique_service_port;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS unique_service_name_port_protocol
    ON service (name, port, protocol);

-- Attach the freshly built index as the uniqueness constraint (brief
-- metadata lock; skipped when a previous run already attached it).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'unique_service_name_port_protocol'
          AND conrelid = 'service'::regclass
    ) THEN
        ALTER TABLE service
            ADD CONSTRAINT unique_service_name_port_protocol
            UNIQUE USING INDEX unique_service_name_port_protocol;
    END IF;
END $$;
