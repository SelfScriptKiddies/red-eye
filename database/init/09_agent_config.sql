-- Per-agent control-plane configuration + global configuration revision
-- (migration twin for existing databases:
--   database/migrations/add_agent_config_and_revision.sql)

-- Persistent per-agent configuration keyed by stable agent_id.
CREATE TABLE IF NOT EXISTS agent_config (
    agent_id VARCHAR(255) PRIMARY KEY,
    -- Server-side overrides; NULL = the agent's reported bootstrap value
    -- is in effect.
    interface_override VARCHAR(64),
    machine_ip_override VARCHAR(45),
    -- Bootstrap values as announced by the agent itself.
    reported_interface VARCHAR(64) NOT NULL DEFAULT '',
    reported_machine_ip VARCHAR(45) NOT NULL DEFAULT '',
    -- Boot identifier of the last heartbeat; a change means the agent
    -- restarted and needs a fresh full snapshot.
    last_boot_id VARCHAR(64) NOT NULL DEFAULT '',
    -- Last configuration revision the agent reported as applied.
    last_acked_revision BIGINT NOT NULL DEFAULT 0,
    -- Last snapshot application error, sanitized by the agent.
    last_sync_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Monotonic control-plane revision, single row (id = 1). The backend
-- advances it in the same transaction as every service, assignment or
-- agent-override mutation.
CREATE TABLE IF NOT EXISTS control_plane_revision (
    id INT PRIMARY KEY CHECK (id = 1),
    revision BIGINT NOT NULL DEFAULT 0
);

INSERT INTO control_plane_revision (id, revision)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
