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
    /// nft binary present on the agent host (proxy redirects / IP bans
    /// depend on it; lets the UI explain a failed "Proxy" click).
    /// None = agent heartbeat didn't report it (older binary) — no verdict.
    #[serde(default)]
    pub nft_available: Option<bool>,
    /// Crate version + building commit reported by the agent heartbeat
    /// (e.g. "0.1.0+df78fe2"). None = older agent binary that didn't
    /// report it yet.
    #[serde(default)]
    pub version: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    // The NATS listeners feed raw bytes straight into from_slice,
    // these pin that garbage on the subject is a clean Err (message dropped),
    // never a panic or a half-applied state change.
    const GARBAGE: [&[u8]; 6] = [
        b"",
        &[0xfc, 0x28, 0x5c, 0x22, 0x06, 0x00],
        b"{\"agent_id\": \"team-1\", \"uptime_secs\"",
        b"[1,2,3]",
        b"\"str\"",
        b"null",
    ];

    #[test]
    fn garbage_heartbeat_is_rejected_not_panicking() {
        for payload in GARBAGE {
            assert!(
                serde_json::from_slice::<AgentInfo>(payload).is_err(),
                "payload {:?} parsed",
                payload
            );
        }
    }

    #[test]
    fn heartbeat_with_unknown_fields_still_parses() {
        let json = serde_json::json!({
            "agent_id": "team-1",
            "uptime_secs": 60,
            "sniffer_active": true,
            "sniffer_ports": [80, 8080],
            "harpoon_rules": ["web-mitm"],
            "timestamp_ms": 1_791_464_509_000u64,
            "future_field": 42,
        });

        let info: AgentInfo = serde_json::from_value(json).unwrap();
        assert_eq!(info.agent_id, "team-1");
        assert_eq!(info.sniffer_ports, vec![80, 8080]);
    }

    #[test]
    fn garbage_docker_listing_is_rejected_not_panicking() {
        for payload in GARBAGE {
            assert!(
                serde_json::from_slice::<AgentDockerStatus>(payload).is_err(),
                "payload {:?} parsed",
                payload
            );
        }
    }

    #[test]
    fn docker_listing_with_unknown_fields_still_parses() {
        let json = serde_json::json!({
            "agent_id": "team-1",
            "timestamp_ms": 123,
            "error": null,
            "containers": [{
                "name": "web",
                "image": "nginx:alpine",
                "state": "running",
                "ports": [{ "port": 80, "protocol": "tcp", "published": true }],
                "future_field": "x",
            }],
        });

        let status: AgentDockerStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status.agent_id, "team-1");
        assert_eq!(status.containers[0].ports[0].port, 80);
    }
}
