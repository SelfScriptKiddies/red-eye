-- HTTP User-Agent aggregate buckets (design: docs/superpowers/specs/
-- 2026-11-16-http-user-agent-listing-design.md).
--
-- Bounded per-(service, agent, 5-minute bucket) counters maintained by the
-- analyzer batch flush. The listing UI reads ONLY this table, so opening it
-- never scans the partitioned http_message history.
--
-- Online/idempotent: CREATE TABLE IF NOT EXISTS + indexes IF NOT EXISTS;
-- safe to re-run; no data backfill and no locks on the HTTP ingest path.
--
-- Reserved overflow value: overflow rows use the internal marker value
-- chr(10) || 'overflow' (a string starting with LF). The analyzer never
-- classifies an LF-containing header value as an ordinary row, so the
-- marker cannot collide with a real User-Agent. It is never returned by
-- the API: overflow rows are aggregated separately and reported only as
-- is_overflow=true without their stored value.

CREATE TABLE IF NOT EXISTS http_user_agent_bucket (
    service_id BIGINT NOT NULL,
    agent_id VARCHAR(255) NOT NULL,
    bucket_start BIGINT NOT NULL,
    user_agent TEXT NOT NULL,
    message_count BIGINT NOT NULL DEFAULT 0,
    first_seen BIGINT NOT NULL,
    last_seen BIGINT NOT NULL,
    is_overflow BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (service_id, agent_id, bucket_start, is_overflow, user_agent)
);

-- Listing sorts: count and last_seen keyset pages (service+agent scope is
-- in every predicate; user_agent is the final tiebreaker applied on the
-- grouped result, not in these indexes).
CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_agent_last_seen
    ON http_user_agent_bucket (service_id, agent_id, last_seen);
CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_agent_count
    ON http_user_agent_bucket (service_id, agent_id, message_count);

-- Retention deletion by fully-expired bucket and per-service cleanup on
-- service deletion.
CREATE INDEX IF NOT EXISTS idx_ua_bucket_bucket_start
    ON http_user_agent_bucket (bucket_start);
CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_id
    ON http_user_agent_bucket (service_id);

-- First successful aggregate write, set once by the analyzer; the backend
-- exposes it so the UI can say the listing covers only data after this
-- moment. The row is created here with an empty value so the key exists
-- even before the first aggregate write.
INSERT INTO app_settings (key, value) VALUES
    ('http_user_agent_aggregation_start', '')
ON CONFLICT (key) DO NOTHING;
