-- Unified filter control plane: one persistent Filter entity for payload,
-- IP/CIDR and expression matching with common targets, actions, thresholds,
-- expiry and revision (2026-11-16-unified-filter-control-plane-design.md).
--
-- The tag_name table and its existing ids are KEPT: historical tag rows
-- reference tag_name.id through tag.fk_tag and must keep resolving, so the
-- unified model evolves this table in place. special_type is mapped onto
-- action (flag/alert/block/block_conn -> flag/alert/block/block_connection,
-- NULL -> tag); the special_type column itself stays for historical rows
-- and is no longer written.
--
-- Current memory-only agent IP bans have no durable state to migrate:
-- operators recreate desired bans through unified IP/CIDR filters. No
-- attempt is made to infer persistent intent from a running agent.
--
-- ONLINE RUNBOOK (safe while backend/analyzer/agents run):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--        -f database/migrations/unified_filter_model.sql
--
-- NOTES:
--   - Every statement is idempotent and metadata-only (constant DEFAULT,
--     no rewrites). The backfill UPDATE touches only tag_name, which is
--     written rarely (filter CRUD), never by ingestion.
--   - Existing rows migrate as enabled, targeting all services and all
--     agents, with no threshold and no expiry: exactly the old behavior.

-- 1. Action replaces special_type. Backfill from the legacy column, then
--    enforce NOT NULL + the value set.
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS action TEXT;

UPDATE tag_name
SET action = CASE special_type
    WHEN 'flag'   THEN 'flag'
    WHEN 'alert'  THEN 'alert'
    WHEN 'block'  THEN 'block'
    WHEN 'block_conn' THEN 'block_connection'
    ELSE 'tag'
END
WHERE action IS NULL;

ALTER TABLE tag_name ALTER COLUMN action SET DEFAULT 'tag';
ALTER TABLE tag_name ALTER COLUMN action SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_tag_name_action'
          AND conrelid = 'tag_name'::regclass
    ) THEN
        ALTER TABLE tag_name ADD CONSTRAINT ck_tag_name_action
            CHECK (action IN ('tag', 'flag', 'alert', 'block', 'block_connection'));
    END IF;
END $$;

-- 2. Matcher-specific payload. ip_network is the normalized CIDR (a single
--    address becomes its host prefix); ip_endpoint selects src/dst of the
--    ORIGINAL packet before stream tuple normalization.
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS ip_network INET;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS ip_endpoint TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_tag_name_ip_endpoint'
          AND conrelid = 'tag_name'::regclass
    ) THEN
        ALTER TABLE tag_name ADD CONSTRAINT ck_tag_name_ip_endpoint
            CHECK (ip_endpoint IS NULL OR ip_endpoint IN ('src', 'dst'));
    END IF;
END $$;

-- CEL-style expression source for expression matchers (validated before
-- persistence by the backend).
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS expression_source TEXT;

-- 3. Common entity state: enable/disable, independent service and agent
--    targets, threshold configuration, expiry, control-plane revision.
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS service_targets_global BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS agent_targets_global BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS threshold_count INT;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS threshold_window_secs INT;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS threshold_group_by TEXT;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS threshold_cooldown_secs INT;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS threshold_block_secs INT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_tag_name_threshold_group'
          AND conrelid = 'tag_name'::regclass
    ) THEN
        ALTER TABLE tag_name ADD CONSTRAINT ck_tag_name_threshold_group
            CHECK (threshold_group_by IS NULL
                   OR threshold_group_by IN ('remote_ip', 'stream', 'service'));
    END IF;
END $$;

ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE tag_name ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0;

-- 4. Explicit targets. A filter with *_targets_global = FALSE uses the
--    corresponding table as its complete target set. CASCADE keeps
--    "deleting a service removes it from filter target sets" true at the
--    storage level.
CREATE TABLE IF NOT EXISTS filter_service_target (
    filter_id BIGINT NOT NULL REFERENCES tag_name(id) ON DELETE CASCADE,
    service_id BIGINT NOT NULL,
    PRIMARY KEY (filter_id, service_id)
);

CREATE TABLE IF NOT EXISTS filter_agent_target (
    filter_id BIGINT NOT NULL REFERENCES tag_name(id) ON DELETE CASCADE,
    agent_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (filter_id, agent_id)
);

-- 5. Active temporary blocks (threshold block actions and IP/CIDR blocks).
--    Expiry is honored everywhere: the analyzer only writes unexpired
--    rows, the backend cleanup removes expired ones, agents unban locally
--    at expiry even if the backend is unreachable.
CREATE TABLE IF NOT EXISTS active_block (
    id BIGSERIAL PRIMARY KEY,
    filter_id BIGINT NOT NULL REFERENCES tag_name(id) ON DELETE CASCADE,
    agent_id VARCHAR(255) NOT NULL,
    remote_ip INET NOT NULL,
    blocked_until TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (filter_id, agent_id, remote_ip)
);

CREATE INDEX IF NOT EXISTS idx_active_block_agent ON active_block (agent_id, blocked_until);

-- 6. Threshold alert/audit events. No FK on filter_id: events survive
--    filter deletion for audit; normal retention cleanup applies and must
--    never delete active_block rows.
CREATE TABLE IF NOT EXISTS filter_event (
    id BIGSERIAL PRIMARY KEY,
    filter_id BIGINT NOT NULL,
    agent_id VARCHAR(255) NOT NULL,
    service_id BIGINT,
    stream_id BIGINT,
    remote_ip INET,
    kind TEXT NOT NULL,
    window_secs INT,
    match_count INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_filter_event_created ON filter_event (created_at DESC);

-- 7. Per-agent enforcement-failure reporting: the agent heartbeat carries
--    the count of filters/blocks it could not enforce and a sanitized
--    last error, so the UI reports per-agent failures instead of global
--    blocking success.
ALTER TABLE agent_config ADD COLUMN IF NOT EXISTS enforcement_failures BIGINT NOT NULL DEFAULT 0;
ALTER TABLE agent_config ADD COLUMN IF NOT EXISTS last_enforcement_error TEXT;
