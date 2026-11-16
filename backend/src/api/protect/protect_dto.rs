use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct ProtectRequest {
    pub service_id: i64,
    pub agent_id: String,
    pub enable: bool,
    #[serde(default)]
    pub tls_mitm: bool,
    /// Custom proxy listen port. Must not collide with another proxy rule
    /// on the agent; when absent a port near service_port + 10000 is
    /// allocated from persisted desired state.
    pub proxy_port: Option<u16>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct IpBanRequest {
    pub agent_id: String,
    pub ip: String,
    pub ban: bool,
}

/// Response of POST /api/protect. `desired_saved` = committed to
/// postgres; the effective capture source is reported by the agent later
/// (GET /api/protect/{agent}/state), so this response never claims the
/// proxy is fully enabled.
#[derive(Debug, Serialize, ToSchema)]
pub struct ProtectResponse {
    pub desired_saved: bool,
    pub enabled: bool,
    pub service_id: i64,
    pub agent_id: String,
    pub rule_name: String,
    /// Control-plane revision the desired state was issued at.
    pub revision: i64,
    /// "published" | "pending": NATS command + snapshot delivery status;
    /// pending is repaired by reconciliation without another operator
    /// command.
    pub sync: String,
    pub proxy_port: Option<u16>,
}

/// One proxy protection row for an agent, with the agent-reported
/// capture source.
#[derive(Debug, Serialize, ToSchema)]
pub struct ProtectionStateRow {
    pub agent_id: String,
    pub service_id: i64,
    pub service_name: String,
    pub service_port: i32,
    pub protocol: String,
    pub enabled: bool,
    pub listen_port: Option<i32>,
    pub target_port: Option<i32>,
    pub tls_mode: Option<String>,
    pub http2_enabled: bool,
    pub revision: i64,
    pub effective: bool,
    pub effective_revision: Option<i64>,
    /// passive | proxy | transitioning | proxy_unavailable
    pub capture_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}
