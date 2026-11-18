-- Per-agent control-plane configuration + global configuration revision
--
-- Three pieces:
--   1. agent_config: persistent per-agent state keyed by stable agent_id.
--      Overrides (server-side) vs reported bootstrap values (learned from
--      heartbeats), the last boot identifier (snapshot trigger), the last
--      configuration revision the agent acknowledged, and its sanitized
--      last sync error.
--   2. control_plane_revision: single-row (id = 1) monotonic revision owned
--      by the backend. Every service/assignment/override mutation advances
--      it in the same transaction; snapshots record the revision they were
--      built from.
--   3. stream.agent_id: optional attribution of a stream to the capturing
--      agent (written by the analyzer on stream creation). Existing rows
--      stay NULL and are never rewritten.
--
-- ONLINE RUNBOOK (safe while backend/analyzer/agents run):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--        -f database/migrations/add_agent_config_and_revision.sql
--
-- All statements are idempotent and metadata-only (no table rewrites, no
-- long locks; the ALTER TABLE ADD COLUMN on stream takes a brief
-- ACCESS EXCLUSIVE lock that waits only for in-flight queries).

CREATE TABLE IF NOT EXISTS agent_config (
    agent_id VARCHAR(255) PRIMARY KEY,
    -- Server-side overrides; NULL = the agent's reported bootstrap value
    -- is in effect.
    interface_override VARCHAR(64),
    machine_ip_override VARCHAR(45),
    -- Bootstrap values as announced by the agent itself (never a server
    -- decision). Empty strings mean "not announced yet".
    reported_interface VARCHAR(64) NOT NULL DEFAULT '',
    reported_machine_ip VARCHAR(45) NOT NULL DEFAULT '',
    -- Boot identifier of the last heartbeat; a change means the agent
    -- restarted and needs a fresh full snapshot.
    last_boot_id VARCHAR(64) NOT NULL DEFAULT '',
    -- Last configuration revision the agent reported as applied.
    last_acked_revision BIGINT NOT NULL DEFAULT 0,
    -- Last snapshot application error, sanitized by the agent (no payload
    -- or credential data). NULL = no error.
    last_sync_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control_plane_revision (
    -- Enforced single row.
    id INT PRIMARY KEY CHECK (id = 1),
    revision BIGINT NOT NULL DEFAULT 0
);

INSERT INTO control_plane_revision (id, revision)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE stream ADD COLUMN IF NOT EXISTS agent_id VARCHAR(255);
