//! Docker container discovery for the UI "add service by container" flow.
//!
//! The backend sends a `DockerListContainers` command, the agent asks the
//! local Docker daemon over `/var/run/docker.sock` and publishes the result
//! on `agent_docker.{agent_id}` (same JSON-on-NATS pattern as heartbeats on
//! `agent_status.{agent_id}`).
//!
//! Stability rules (this runs on a live game box):
//! - Docker being absent/unreachable is normal, not fatal: log a warning and
//!   reply with an error payload — never panic, never retry in a loop.
//! - The whole Docker exchange is bounded by [`DOCKER_TIMEOUT`] so a wedged
//!   socket can never hang the agent.
//! - The handler runs as its own task, so even a slow daemon cannot delay
//!   other agent commands (Shutdown included).

use std::time::Duration;

use bollard::models::{ContainerSummary, PortSummary, PortSummaryTypeEnum};
use bollard::query_parameters::ListContainersOptions;
use bollard::Docker;
use serde::{Deserialize, Serialize};

use log::{info, warn};

use crate::nats::NatsClient;

/// Upper bound for the whole Docker API exchange (connect + list).
const DOCKER_TIMEOUT: Duration = Duration::from_secs(5);

/// One candidate port the operator may register a service on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerPort {
    /// The port to register: the host-published port when one exists,
    /// otherwise the port exposed inside the container.
    pub port: u16,
    /// "tcp" | "udp" | "sctp" (as reported by Docker).
    pub protocol: String,
    /// True when Docker publishes this port on the host (PublicPort present).
    pub published: bool,
}

/// Slim container view sent to the backend/UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    pub name: String,
    pub image: String,
    pub state: String,
    pub ports: Vec<ContainerPort>,
}

/// Reply published on `agent_docker.{agent_id}`.
/// `error` set ⇒ `containers` is empty (Docker unreachable on this host).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerContainersStatus {
    pub agent_id: String,
    pub timestamp_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub containers: Vec<ContainerInfo>,
}

/// Serialize + publish the docker listing. Infallible on the publish side:
/// a NATS failure is logged, the agent keeps running (same as heartbeats).
pub async fn handle_list_containers(nats: NatsClient) {
    let agent_id = nats.agent_id.clone();

    let (containers, error) = match list_running_containers().await {
        Ok(c) => {
            info!("Docker: listed {} running containers", c.len());
            (c, None)
        }
        Err(e) => {
            // Once per request, no retry loop — see module docs.
            warn!("Docker container listing failed: {}", e);
            (Vec::new(), Some(e))
        }
    };

    let status = DockerContainersStatus {
        agent_id,
        timestamp_ms: now_ms(),
        error,
        containers,
    };

    match serde_json::to_vec(&status) {
        Ok(json) => nats.send_docker_containers(json).await,
        Err(e) => warn!("Failed to serialize docker status: {}", e),
    }
}

/// Connect to the local Docker daemon and list RUNNING containers only
/// (an operator registers live services; stopped ones would be noise).
/// Every failure mode (missing socket, permission, daemon down, timeout)
/// is returned as an error string for the reply payload.
pub async fn list_running_containers() -> Result<Vec<ContainerInfo>, String> {
    let docker = Docker::connect_with_socket_defaults()
        .map_err(|e| format!("docker connect failed: {}", e))?;

    let options = ListContainersOptions {
        all: false,
        ..Default::default()
    };

    let summaries = tokio::time::timeout(DOCKER_TIMEOUT, docker.list_containers(Some(options)))
        .await
        .map_err(|_| format!("docker API timed out after {:?}", DOCKER_TIMEOUT))?
        .map_err(|e| format!("docker list failed: {}", e))?;

    Ok(summaries.iter().map(summary_to_info).collect())
}

/// Map a Docker `ContainerSummary` to the slim DTO.
fn summary_to_info(c: &ContainerSummary) -> ContainerInfo {
    ContainerInfo {
        name: container_display_name(c),
        image: c.image.clone().unwrap_or_default(),
        state: c.state.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        ports: map_container_ports(c.ports.as_deref().unwrap_or(&[])),
    }
}

/// Container name: first entry of `Names` (Docker prefixes it with `/`),
/// falling back to a short id prefix, then "unknown".
pub fn container_display_name(c: &ContainerSummary) -> String {
    if let Some(name) = c
        .names
        .as_ref()
        .and_then(|names| names.first())
        .map(|n| n.trim_start_matches('/'))
        .filter(|n| !n.is_empty())
    {
        return name.to_string();
    }
    match &c.id {
        Some(id) if id.len() > 12 => id[..12].to_string(),
        Some(id) => id.clone(),
        None => "unknown".to_string(),
    }
}

/// Listening candidates for the operator to pick from.
///
/// Preference: host-published ports (`PublicPort` present) — those are what
/// an external client actually connects to on this host. If the container
/// publishes nothing (host network, or plain EXPOSE), fall back to the
/// container's exposed private ports — in host-network mode those ARE the
/// host ports. Result is deduped by (port, protocol) — Docker reports the
/// same mapping once per bind IP (0.0.0.0 + ::) — and sorted by port, then
/// protocol, for a stable order in the UI.
pub fn map_container_ports(ports: &[PortSummary]) -> Vec<ContainerPort> {
    let mut candidates: Vec<ContainerPort> = ports
        .iter()
        .filter_map(|p| {
            let public = p.public_port?;
            Some(ContainerPort {
                port: public,
                protocol: protocol_str(p.typ),
                published: true,
            })
        })
        .collect();

    if candidates.is_empty() {
        candidates = ports
            .iter()
            .map(|p| ContainerPort {
                port: p.private_port,
                protocol: protocol_str(p.typ),
                published: false,
            })
            .collect();
    }

    candidates.sort_by(|a, b| a.port.cmp(&b.port).then(a.protocol.cmp(&b.protocol)));
    candidates.dedup_by(|a, b| a.port == b.port && a.protocol == b.protocol);
    candidates
}

fn protocol_str(typ: Option<PortSummaryTypeEnum>) -> String {
    match typ {
        Some(PortSummaryTypeEnum::UDP) => "udp",
        Some(PortSummaryTypeEnum::SCTP) => "sctp",
        // Docker defaults to tcp, including a missing/empty Type.
        _ => "tcp",
    }
    .to_string()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn port(private: u16, public: Option<u16>, typ: Option<PortSummaryTypeEnum>) -> PortSummary {
        PortSummary {
            ip: None,
            private_port: private,
            public_port: public,
            typ,
        }
    }

    fn tcp(private: u16, public: Option<u16>) -> PortSummary {
        port(private, public, Some(PortSummaryTypeEnum::TCP))
    }

    #[test]
    fn published_ports_are_preferred() {
        let ports = vec![tcp(8080, None), tcp(5432, Some(55433))];
        let mapped = map_container_ports(&ports);
        assert_eq!(
            mapped,
            vec![ContainerPort {
                port: 55433,
                protocol: "tcp".to_string(),
                published: true
            }]
        );
    }

    #[test]
    fn falls_back_to_exposed_private_ports() {
        let ports = vec![
            tcp(8080, None),
            port(53, None, Some(PortSummaryTypeEnum::UDP)),
        ];
        let mapped = map_container_ports(&ports);
        assert_eq!(
            mapped,
            vec![
                ContainerPort {
                    port: 53,
                    protocol: "udp".to_string(),
                    published: false
                },
                ContainerPort {
                    port: 8080,
                    protocol: "tcp".to_string(),
                    published: false
                },
            ]
        );
    }

    #[test]
    fn mixed_published_and_exposed_keeps_only_published() {
        // A container with at least one published port must not mix in
        // non-published exposed ports: the operator would pick a port
        // nothing listens on (on the host).
        let ports = vec![tcp(3000, Some(3000)), tcp(9000, None)];
        let mapped = map_container_ports(&ports);
        assert_eq!(mapped.len(), 1);
        assert_eq!(mapped[0].port, 3000);
        assert!(mapped[0].published);
    }

    #[test]
    fn duplicates_across_bind_ips_are_deduped() {
        // Same mapping bound on 0.0.0.0 and :: — identical (port, proto).
        let ports = vec![tcp(5432, Some(55433)), tcp(5432, Some(55433))];
        let mapped = map_container_ports(&ports);
        assert_eq!(mapped.len(), 1);
    }

    #[test]
    fn same_public_port_different_protocol_kept() {
        let ports = vec![
            port(53, Some(53), Some(PortSummaryTypeEnum::TCP)),
            port(53, Some(53), Some(PortSummaryTypeEnum::UDP)),
        ];
        let mapped = map_container_ports(&ports);
        assert_eq!(mapped.len(), 2);
    }

    #[test]
    fn result_is_sorted_by_port_then_protocol() {
        let ports = vec![
            tcp(9000, Some(9000)),
            port(53, Some(53), Some(PortSummaryTypeEnum::UDP)),
            port(53, Some(53), Some(PortSummaryTypeEnum::TCP)),
            tcp(80, Some(8080)),
        ];
        let mapped = map_container_ports(&ports);
        let keys: Vec<(u16, &str)> = mapped
            .iter()
            .map(|p| (p.port, p.protocol.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![(53, "tcp"), (53, "udp"), (8080, "tcp"), (9000, "tcp")]
        );
    }

    #[test]
    fn missing_type_defaults_to_tcp() {
        let mapped = map_container_ports(&[port(8080, None, None)]);
        assert_eq!(mapped[0].protocol, "tcp");
    }

    #[test]
    fn empty_ports_give_empty_result() {
        assert!(map_container_ports(&[]).is_empty());
        assert!(map_container_ports(&[port(0, None, None)]).len() == 1); // port 0 still exposed
    }

    #[test]
    fn container_name_strips_leading_slash() {
        let mut c = ContainerSummary::default();
        c.names = Some(vec!["/web-app".to_string()]);
        assert_eq!(container_display_name(&c), "web-app");
    }

    #[test]
    fn container_name_falls_back_to_short_id_then_unknown() {
        let mut c = ContainerSummary::default();
        c.id = Some("abcdef1234567890abcdef".to_string());
        assert_eq!(container_display_name(&c), "abcdef123456");

        let c = ContainerSummary::default();
        assert_eq!(container_display_name(&c), "unknown");
    }

    #[test]
    fn status_serializes_to_wire_shape_shared_with_backend() {
        // The backend deserializes exactly these field names — keep in sync
        // with AgentDockerStatus in backend/src/api/agent/agent_dto.rs.
        let status = DockerContainersStatus {
            agent_id: "agent-1".to_string(),
            timestamp_ms: 1_000,
            error: Some("docker connect failed: no socket".to_string()),
            containers: vec![],
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["agent_id"], "agent-1");
        assert_eq!(json["timestamp_ms"], 1000);
        assert_eq!(json["error"], "docker connect failed: no socket");
        assert_eq!(json["containers"], serde_json::json!([]));

        let ok = DockerContainersStatus {
            agent_id: "agent-1".to_string(),
            timestamp_ms: 2_000,
            error: None,
            containers: vec![ContainerInfo {
                name: "web".to_string(),
                image: "nginx:alpine".to_string(),
                state: "running".to_string(),
                ports: vec![ContainerPort {
                    port: 8080,
                    protocol: "tcp".to_string(),
                    published: true,
                }],
            }],
        };
        let json = serde_json::to_value(&ok).unwrap();
        assert!(json.get("error").is_none());
        assert_eq!(json["containers"][0]["name"], "web");
        assert_eq!(json["containers"][0]["ports"][0]["port"], 8080);
        assert_eq!(json["containers"][0]["ports"][0]["published"], true);
    }

    /// Full agent-side path against real infrastructure:
    /// `handle_list_containers` → `NatsClient::send_docker_containers` →
    /// broker → the exact bytes the backend's `agent_docker.*` subscriber
    /// receives. The received payload must parse back into the agent DTO
    /// (the backend parses the same shape — see
    /// `agent_wire_payload_deserializes_into_backend_dto` in the backend).
    ///
    /// Run manually with a scratch broker + real Docker:
    ///   docker run --rm -d --name reg-nats -p 4223:4222 nats:alpine
    ///   cargo test -p red-eye-agent --lib --ignored docker_live -- --nocapture
    #[tokio::test]
    #[ignore = "needs a NATS broker on 127.0.0.1:4223 and the local Docker socket"]
    async fn docker_live_round_trip_through_nats() {
        use crate::config::AgentConfig;
        use crate::nats::NatsClient;
        use clap::Parser;
        use futures::StreamExt;

        let config =
            AgentConfig::parse_from(["red-eye-agent", "--nats-host", "nats://127.0.0.1:4223"]);
        let nats = NatsClient::connect(&config, "live-test-agent")
            .await
            .expect("connect to scratch NATS on 127.0.0.1:4223");

        let mut sub = nats
            .inner()
            .subscribe("agent_docker.live-test-agent".to_string())
            .await
            .expect("subscribe");

        crate::docker::handle_list_containers(nats.clone()).await;

        let msg = tokio::time::timeout(std::time::Duration::from_secs(15), sub.next())
            .await
            .expect("reply published within 15s")
            .expect("non-empty message");

        println!(
            "raw wire payload: {}",
            String::from_utf8_lossy(&msg.payload)
        );
        let status: DockerContainersStatus =
            serde_json::from_slice(&msg.payload).expect("round-trip through the broker parses");
        assert_eq!(status.agent_id, "live-test-agent");
        // Docker reachable on this machine ⇒ listing; unreachable ⇒ error
        // reply — either is a valid live outcome, but never both empty-handed.
        assert!(status.error.is_some() || !status.containers.is_empty());
        if let Some(err) = &status.error {
            println!(
                "agent reported docker error (expected when Docker is absent): {}",
                err
            );
        } else {
            for c in &status.containers {
                println!("container: {} ({}), ports: {:?}", c.name, c.image, c.ports);
            }
        }
    }
}
