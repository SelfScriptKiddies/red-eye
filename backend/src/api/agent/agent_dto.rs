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
