use serde::{Deserialize, Serialize};

/// Transport protocol of a service. Wire- and DB-facing spelling ("TCP"/"UDP").
pub const PROTOCOLS: [&str; 2] = ["TCP", "UDP"];

pub(crate) fn valid_protocol(protocol: &str) -> bool {
    PROTOCOLS.contains(&protocol.to_uppercase().as_str())
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Service {
    pub id: i64,
    pub name: String,
    pub port: i32,
    pub chunked_transfer_encoding: bool,
    pub urldecode: bool,
    pub merge_adjacent_packets: bool,
    pub inflate_websockets: bool,
    pub decrypt_tls: bool,
    pub http_parse: bool,
    pub http2_parse: bool,
    pub alert_flag_leak: bool,
    /// Transport protocol: "TCP" or "UDP"
    pub protocol: String,
    /// True = applies to all agents; false = explicit agent_service set
    pub is_global: bool,
    /// Explicit agent ids when not global (empty for global services).
    /// Filled by the repository after the row query, not a table column.
    #[sqlx(default)]
    #[serde(default)]
    pub agent_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ServiceTransfer {
    pub name: String,
    pub port: i32,
    pub chunked_transfer_encoding: Option<bool>,
    pub urldecode: Option<bool>,
    pub merge_adjacent_packets: Option<bool>,
    pub inflate_websockets: Option<bool>,
    pub decrypt_tls: Option<bool>,
    pub http_parse: Option<bool>,
    pub http2_parse: Option<bool>,
    pub alert_flag_leak: Option<bool>,
    /// Required on create ("TCP" | "UDP"). Optional on update: omitted
    /// keeps the existing row's protocol (pre-migration rows are TCP).
    pub protocol: Option<String>,
    /// Optional on update: omitted keeps the existing assignment mode.
    pub is_global: Option<bool>,
    /// Explicit agent set when is_global is false
    #[serde(default)]
    pub agent_ids: Option<Vec<String>>,
}

impl ServiceTransfer {
    /// Normalized protocol for create: uppercase TCP/UDP, None when absent
    /// or invalid (callers must reject None on create).
    pub fn normalized_protocol(&self) -> Option<String> {
        self.protocol
            .as_deref()
            .map(|p| p.trim().to_uppercase())
            .filter(|p| valid_protocol(p))
    }
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct MutationResult {
    pub service: Service,
    /// New control-plane revision the mutation produced
    pub revision: i64,
    /// "synced" when the post-commit publication succeeded, "pending" when
    /// it failed (periodic reconciliation will deliver the change)
    pub sync: &'static str,
}

impl From<ServiceTransfer> for Service {
    fn from(transfer: ServiceTransfer) -> Self {
        let protocol = transfer.normalized_protocol().unwrap_or_else(|| "TCP".into());
        Service {
            id: 0,
            name: transfer.name,
            port: transfer.port,
            chunked_transfer_encoding: transfer.chunked_transfer_encoding.unwrap_or(false),
            urldecode: transfer.urldecode.unwrap_or(false),
            merge_adjacent_packets: transfer.merge_adjacent_packets.unwrap_or(false),
            inflate_websockets: transfer.inflate_websockets.unwrap_or(false),
            decrypt_tls: transfer.decrypt_tls.unwrap_or(false),
            http_parse: transfer.http_parse.unwrap_or(false),
            http2_parse: transfer.http2_parse.unwrap_or(false),
            alert_flag_leak: transfer.alert_flag_leak.unwrap_or(false),
            protocol,
            is_global: transfer.is_global.unwrap_or(true),
            agent_ids: transfer.agent_ids.unwrap_or_default(),
        }
    }
}
