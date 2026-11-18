-- HTTP User-Agent aggregate buckets: bounded per-(service, agent,
-- 5-minute bucket) counters maintained by the analyzer batch flush. The
-- listing UI reads ONLY this table, so opening it never scans the
-- partitioned http_message history.
--
-- Overflow rows use the reserved internal marker value chr(10) ||
-- 'overflow' (a string starting with LF). The analyzer never classifies an
-- LF-containing header value as an ordinary row, so the marker cannot
-- collide with a real User-Agent; the API never returns it.
--
-- Fresh-init twin of database/migrations/add_http_user_agent_buckets.sql.

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

CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_agent_last_seen
    ON http_user_agent_bucket (service_id, agent_id, last_seen);
CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_agent_count
    ON http_user_agent_bucket (service_id, agent_id, message_count);
CREATE INDEX IF NOT EXISTS idx_ua_bucket_bucket_start
    ON http_user_agent_bucket (bucket_start);
CREATE INDEX IF NOT EXISTS idx_ua_bucket_service_id
    ON http_user_agent_bucket (service_id);

-- First successful aggregate write, set once by the analyzer; the backend
-- exposes it so the UI can scope its honesty note. Empty means aggregation
-- has not started.
INSERT INTO app_settings (key, value) VALUES
    ('http_user_agent_aggregation_start', '')
ON CONFLICT (key) DO NOTHING;
