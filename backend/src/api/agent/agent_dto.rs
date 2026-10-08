use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Agent status as reported by heartbeat
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AgentInfo {
    pub agent_id: String,
    pub uptime_secs: u64,
    pub sniffer_active: bool,
    pub sniffer_ports: Vec<u16>,
    pub harpoon_rules: Vec<String>,
    #[serde(default)]
    pub active_filters: usize,
    #[serde(default)]
    pub nft_redirects: Vec<NftRedirectStatus>,
    #[serde(default)]
    pub tls_available: bool,
    pub timestamp_ms: u64,
    /// Computed by the backend: is the agent currently reachable
    #[serde(default)]
    pub online: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct NftRedirectStatus {
    pub protocol: String,
    pub from_port: u16,
    pub to_port: u16,
}

/// Docker containers reply from an agent, published on `agent_docker.{agent_id}`.
///
/// Field names MUST stay in sync with `DockerContainersStatus` /
/// `ContainerInfo` / `ContainerPort` in agent/src/docker.rs — the wire format
/// between the two crates is serde JSON (same as heartbeats).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AgentDockerStatus {
    pub agent_id: String,
    pub timestamp_ms: u64,
    /// Docker error reported by the agent (socket missing, daemon down,
    /// permission denied). When set, `containers` is empty.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub containers: Vec<AgentContainerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AgentContainerInfo {
    pub name: String,
    pub image: String,
    /// Docker container state ("running", ...).
    pub state: String,
    pub ports: Vec<AgentContainerPort>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AgentContainerPort {
    /// The port to register: host-published when available, else exposed.
    pub port: u16,
    /// "tcp" | "udp" | "sctp".
    pub protocol: String,
    /// True when Docker publishes this port on the host.
    pub published: bool,
}

/// Response of `GET /api/agent/{id}/containers`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AgentContainersResponse {
    pub agent_id: String,
    /// "ok" — fresh successful listing; "error" — fresh reply where the
    /// agent reported a Docker error; "pending" — no reply yet, the
    /// DockerListContainers command was dispatched, re-poll in ~1s;
    /// "stale" — cached reply older than the freshness window, command
    /// re-dispatched, stale data still included for display.
    pub state: &'static str,
    pub containers: Vec<AgentContainerInfo>,
    /// Agent-reported Docker error message, when `state` is "error" (or
    /// "stale" with the previous reply being an error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Age of the cached data in milliseconds, `None` when nothing cached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
}

/// Command payload sent to an agent (forwarded as-is to NATS)
#[derive(Debug, Deserialize, ToSchema)]
pub struct AgentCommandPayload {
    /// The raw command JSON — must have a "type" field matching AgentCommand variants
    #[serde(flatten)]
    pub inner: serde_json::Value,
}

/// Request to send a command to multiple agents
#[derive(Debug, Deserialize, ToSchema)]
pub struct BroadcastCommandPayload {
    /// Agent IDs to target. If empty, sends to all known agents.
    #[serde(default)]
    pub agent_ids: Vec<String>,
    /// The command
    #[serde(flatten)]
    pub command: serde_json::Value,
}
