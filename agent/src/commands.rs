use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::sync::Mutex;

use harpoon_core::types::endpoint::Protocol;

use crate::harpoon::{HarpoonEngine, ProxyRuleSpec};
use crate::nats::NatsClient;
use crate::nftables::RedirectRule;
use crate::proxy_state::ProxyDesired;
use crate::supervisor::ProxySupervisor;
use crate::updates::{AppState, TransportProto};

fn default_tcp() -> String {
    "tcp".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum AgentCommand {
    Shutdown,

    // Proxy management. Commands carrying a service_id drive the
    // desired-state machine (protect flow): the agent keeps them in its
    // ledger and converges via supervised transitions. Commands without
    // one are ad-hoc manual/replay rules (no capture-source exclusion).
    ProxyAdd {
        name: String,
        listen: String,
        target: String,
        protocol: String,
        tls_mode: Option<String>,
        nft_redirect_port: Option<u16>,
        duplicate_to: Option<String>,
        #[serde(default)]
        http2: bool,
        #[serde(default)]
        service_id: Option<u64>,
        /// Desired revision (0 = legacy command without revision).
        #[serde(default)]
        revision: u64,
    },
    ProxyUpdate {
        name: String,
        listen: String,
        target: String,
        protocol: String,
        tls_mode: Option<String>,
        nft_redirect_port: Option<u16>,
        duplicate_to: Option<String>,
        #[serde(default)]
        http2: bool,
        #[serde(default)]
        service_id: Option<u64>,
        #[serde(default)]
        revision: u64,
    },
    ProxyRemove {
        name: String,
        #[serde(default)]
        service_id: Option<u64>,
    },

    // nftables manual management
    NftablesApply {
        rules: Vec<RedirectRule>,
    },
    NftablesClear,

    // Replay: forward to another team
    ReplayForward {
        name: String,
        listen_port: u16,
        target_addr: String,
        #[serde(default = "default_tcp")]
        protocol: String,
    },
    ReplayStop {
        name: String,
    },

    // IP ban via nftables
    IpBan {
        ip: String,
    },
    IpUnban {
        ip: String,
    },

    // Docker container discovery (UI "add service by container").
    // The agent replies on `agent_docker.{agent_id}` — see docker.rs.
    DockerListContainers,
}

pub fn spawn_command_listener(
    nats: NatsClient,
    state: Arc<AppState>,
    harpoon: Arc<Mutex<HarpoonEngine>>,
    supervisor: Arc<ProxySupervisor>,
) {
    tokio::spawn(async move {
        let mut sub = match nats.subscribe_commands().await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to subscribe to commands: {}", e);
                return;
            }
        };

        info!("Listening for commands on agent_commands.{}", nats.agent_id);

        while let Some(msg) = sub.next().await {
            let cmd: AgentCommand = match serde_json::from_slice(&msg.payload) {
                Ok(c) => c,
                Err(e) => {
                    warn!("Invalid command payload: {}", e);
                    continue;
                }
            };

            debug!("Received command: {:?}", cmd);
            handle_command(cmd, &nats, &state, &harpoon, &supervisor).await;
        }
    });
}

async fn handle_command(
    cmd: AgentCommand,
    nats: &NatsClient,
    _state: &Arc<AppState>,
    harpoon: &Arc<Mutex<HarpoonEngine>>,
    supervisor: &Arc<ProxySupervisor>,
) {
    match cmd {
        AgentCommand::Shutdown => {
            info!("Shutdown command received");
            // Shared with SIGTERM/SIGINT: bounded cleanup + exit(0).
            crate::graceful_shutdown(harpoon.clone(), nats).await;
        }

        AgentCommand::ProxyAdd {
            name,
            listen,
            target,
            protocol,
            tls_mode,
            nft_redirect_port,
            duplicate_to,
            http2,
            service_id,
            revision,
        } => {
            // Desired-state path: the command updates the ledger; the
            // supervised transition does the rest. Redelivery is
            // idempotent (same desired -> ApplyKind::None).
            if let Some(service_id) = service_id {
                match desired_from_command(
                    service_id,
                    &name,
                    &listen,
                    &target,
                    &protocol,
                    tls_mode.as_deref(),
                    nft_redirect_port,
                    http2,
                    revision,
                ) {
                    Ok(desired) => supervisor.command_set_desired(desired),
                    Err(e) => error!("Invalid desired ProxyAdd for service {}: {}", service_id, e),
                }
                return;
            }
            let spec = match parse_proxy_spec(
                &name,
                &listen,
                &target,
                &protocol,
                tls_mode,
                nft_redirect_port,
                duplicate_to,
                http2,
            ) {
                Ok(s) => s,
                Err(e) => {
                    error!("Invalid ProxyAdd '{}': {}", name, e);
                    return;
                }
            };
            if let Err(e) = harpoon.lock().await.add_adhoc_rule(spec).await {
                error!("Failed to add proxy '{}': {}", name, e);
            }
        }

        AgentCommand::ProxyUpdate {
            name,
            listen,
            target,
            protocol,
            tls_mode,
            nft_redirect_port,
            duplicate_to,
            http2,
            service_id,
            revision,
        } => {
            if let Some(service_id) = service_id {
                // Protect flow updates are just new desired state.
                match desired_from_command(
                    service_id,
                    &name,
                    &listen,
                    &target,
                    &protocol,
                    tls_mode.as_deref(),
                    nft_redirect_port,
                    http2,
                    revision,
                ) {
                    Ok(desired) => supervisor.command_set_desired(desired),
                    Err(e) => error!(
                        "Invalid desired ProxyUpdate for service {}: {}",
                        service_id, e
                    ),
                }
                return;
            }
            let spec = match parse_proxy_spec(
                &name,
                &listen,
                &target,
                &protocol,
                tls_mode,
                nft_redirect_port,
                duplicate_to,
                http2,
            ) {
                Ok(s) => s,
                Err(e) => {
                    error!("Invalid ProxyUpdate '{}': {}", name, e);
                    return;
                }
            };
            if let Err(e) = harpoon.lock().await.update_adhoc_rule(spec).await {
                error!("Failed to update proxy '{}': {}", name, e);
            }
        }

        AgentCommand::ProxyRemove { name, service_id } => {
            if let Some(service_id) = service_id {
                // Desired removal: the disable transition retires the rule
                // and returns the original key to passive capture.
                supervisor.command_remove_desired(service_id);
                return;
            }
            if let Err(e) = harpoon.lock().await.remove_adhoc_rule(&name).await {
                error!("Failed to remove proxy '{}': {}", name, e);
            }
        }

        AgentCommand::NftablesApply { rules } => {
            info!("Applying {} manual nftables rules", rules.len());
            if let Err(e) = harpoon.lock().await.apply_nft_rules(rules) {
                error!("Failed to apply nftables rules: {}", e);
            }
        }

        AgentCommand::NftablesClear => {
            info!("Clearing all nftables rules");
            if let Err(e) = harpoon.lock().await.clear_nft_rules() {
                error!("Failed to clear nftables rules: {}", e);
            }
        }

        AgentCommand::ReplayForward {
            name,
            listen_port,
            target_addr,
            protocol,
        } => {
            let target: SocketAddr = match target_addr.parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("Invalid replay target '{}': {}", target_addr, e);
                    return;
                }
            };
            let proto = if protocol.eq_ignore_ascii_case("udp") {
                Protocol::Udp
            } else {
                Protocol::Tcp
            };
            let spec = ProxyRuleSpec {
                name: format!("replay-{}", name),
                listen: match format!("0.0.0.0:{}", listen_port).parse() {
                    Ok(a) => a,
                    Err(e) => {
                        error!("Invalid replay listen port {}: {}", listen_port, e);
                        return;
                    }
                },
                target,
                protocol: proto,
                tls_mode: None,
                nft_redirect_port: None,
                duplicate_to: None,
                http2: false,
            };
            info!(
                "Starting replay forward '{}' ({}): :{} -> {}",
                name, protocol, listen_port, target_addr
            );
            if let Err(e) = harpoon.lock().await.add_adhoc_rule(spec).await {
                error!("Failed to start replay '{}': {}", name, e);
            }
        }

        AgentCommand::ReplayStop { name } => {
            let rule_name = format!("replay-{}", name);
            info!("Stopping replay forward '{}'", name);
            if let Err(e) = harpoon.lock().await.remove_adhoc_rule(&rule_name).await {
                error!("Failed to stop replay '{}': {}", name, e);
            }
        }

        AgentCommand::IpBan { ip } => {
            info!("Banning IP: {}", ip);
            if let Err(e) = harpoon.lock().await.ban_ip(&ip) {
                error!("Failed to ban IP '{}': {}", ip, e);
            }
        }

        AgentCommand::IpUnban { ip } => {
            info!("Unbanning IP: {}", ip);
            if let Err(e) = harpoon.lock().await.unban_ip(&ip) {
                error!("Failed to unban IP '{}': {}", ip, e);
            }
        }

        AgentCommand::DockerListContainers => {
            info!("DockerListContainers command received");
            // Own task: Docker may be absent/slow on this host, and that must
            // never delay the command loop (Shutdown included). The handler
            // is timeout-bounded and answers with an error reply on failure.
            let nats = nats.clone();
            tokio::spawn(async move {
                crate::docker::handle_list_containers(nats).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The backend dispatches exactly this JSON (agent_service.rs:
    /// `serde_json::json!({ "type": "DockerListContainers" })`) on
    /// `agent_commands.{agent_id}` — lock the wire contract.
    #[test]
    fn backend_dispatch_json_parses_as_agent_command() {
        let cmd: AgentCommand =
            serde_json::from_str(r#"{"type":"DockerListContainers"}"#).expect("parse dispatch");
        assert!(matches!(cmd, AgentCommand::DockerListContainers));
    }

    /// Service ports no longer travel through commands: SnifferConfig was
    /// removed with the PORTS bootstrap and must not come back as a
    /// decode-able variant.
    #[test]
    fn sniffer_config_command_is_gone() {
        assert!(
            serde_json::from_str::<AgentCommand>(r#"{"type":"SnifferConfig","ports":[80]}"#)
                .is_err()
        );
    }

    // The command listener feeds raw bytes straight into from_slice
    // garbage on the subject must be a clean Err (warn + drop),
    // never a panic or a misapplied command.
    const GARBAGE: [&[u8]; 6] = [
        b"",
        &[0xfc, 0x28, 0x5c, 0x22, 0x06, 0x00],
        b"{\"type\":\"SnifferConfig\",\"ports\":[80",
        b"[1,2,3]",
        b"\"str\"",
        b"null",
    ];

    #[test]
    fn garbage_command_is_rejected_not_panicking() {
        for payload in GARBAGE {
            assert!(
                serde_json::from_slice::<AgentCommand>(payload).is_err(),
                "payload {:?} parsed",
                payload
            );
        }
    }

    #[test]
    fn command_without_type_tag_is_rejected() {
        assert!(serde_json::from_str::<AgentCommand>(r#"{"ports":[80]}"#).is_err());
        assert!(serde_json::from_str::<AgentCommand>(r#"{}"#).is_err());
    }

    /// A backend newer than this agent may dispatch command variants this agent does not know:
    /// they must be dropped (warn in the listener), never decoded into a wrong variant.
    #[test]
    fn unknown_command_type_is_rejected() {
        assert!(
            serde_json::from_str::<AgentCommand>(r#"{"type":"FutureCommand","foo":1}"#).is_err()
        );
    }

    /// Known variant carrying extra fields from a newer backend still parses
    #[test]
    fn command_with_unknown_fields_still_parses() {
        let cmd: AgentCommand = serde_json::from_str(r#"{"type":"NftablesClear","future":true}"#)
            .expect("parses with extra fields");
        assert!(matches!(cmd, AgentCommand::NftablesClear));
    }

    /// The protect flow dispatches service-keyed commands: they parse and
    /// carry the original port + revision for the desired-state ledger.
    #[test]
    fn backend_protect_json_parses_as_desired_command() {
        let wire = r#"{
            "type": "ProxyAdd",
            "service_id": 42,
            "revision": 7,
            "name": "protect-42-team-site",
            "listen": "0.0.0.0:18080",
            "target": "127.0.0.1:8080",
            "protocol": "tcp",
            "nft_redirect_port": 8080,
            "http2": true
        }"#;
        let cmd: AgentCommand = serde_json::from_str(wire).expect("parse protect dispatch");
        let AgentCommand::ProxyAdd {
            service_id,
            revision,
            ..
        } = cmd
        else {
            panic!("wrong variant");
        };
        assert_eq!(service_id, Some(42));
        assert_eq!(revision, 7);
    }

    /// Desired commands without the original port are rejected: exclusion
    /// and redirect both depend on it.
    #[test]
    fn desired_command_without_original_port_is_rejected() {
        let parsed: AgentCommand =
            serde_json::from_str(r#"{"type":"ProxyAdd","service_id":1,"name":"x","listen":"0.0.0.0:10080","target":"127.0.0.1:80","protocol":"tcp"}"#)
                .expect("parses");
        let AgentCommand::ProxyAdd {
            service_id,
            name,
            listen,
            target,
            protocol,
            tls_mode,
            nft_redirect_port,
            http2,
            revision,
            ..
        } = parsed
        else {
            panic!("wrong variant");
        };
        assert!(desired_from_command(
            service_id.unwrap(),
            &name,
            &listen,
            &target,
            &protocol,
            tls_mode.as_deref(),
            nft_redirect_port,
            http2,
            revision
        )
        .is_err());
    }

    #[test]
    fn desired_command_parses_into_desired_state() {
        let parsed: AgentCommand = serde_json::from_str(
            r#"{"type":"ProxyAdd","service_id":3,"name":"protect-3-dns","listen":"0.0.0.0:10053","target":"127.0.0.1:53","protocol":"udp","nft_redirect_port":53,"revision":9}"#,
        )
        .expect("parses");
        let AgentCommand::ProxyAdd {
            service_id,
            name,
            listen,
            target,
            protocol,
            tls_mode,
            nft_redirect_port,
            http2,
            revision,
            ..
        } = parsed
        else {
            panic!("wrong variant");
        };
        let desired = desired_from_command(
            service_id.unwrap(),
            &name,
            &listen,
            &target,
            &protocol,
            tls_mode.as_deref(),
            nft_redirect_port,
            http2,
            revision,
        )
        .expect("valid desired");
        assert_eq!(desired.service_id, 3);
        assert_eq!(desired.protocol, TransportProto::Udp);
        assert_eq!(desired.original_port, 53);
        assert_eq!(desired.listen.port(), 10053);
        assert_eq!(desired.revision, 9);
        assert_eq!(desired.tls_mode, None);
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_proxy_spec(
    name: &str,
    listen: &str,
    target: &str,
    protocol: &str,
    tls_mode: Option<String>,
    nft_redirect_port: Option<u16>,
    duplicate_to: Option<String>,
    http2: bool,
) -> anyhow::Result<ProxyRuleSpec> {
    let protocol = match protocol.to_lowercase().as_str() {
        "tcp" => Protocol::Tcp,
        "udp" => Protocol::Udp,
        other => anyhow::bail!("Unknown protocol: {}", other),
    };

    let duplicate_to = duplicate_to.map(|s| s.parse::<SocketAddr>()).transpose()?;

    Ok(ProxyRuleSpec {
        name: name.to_string(),
        listen: listen.parse()?,
        target: target.parse()?,
        protocol,
        tls_mode,
        nft_redirect_port,
        duplicate_to,
        http2,
    })
}

/// Parse a service-keyed command into desired proxy state. The original
/// service port is mandatory: without it the agent could not exclude the
/// right passive key or install the redirect.
#[allow(clippy::too_many_arguments)]
fn desired_from_command(
    service_id: u64,
    name: &str,
    listen: &str,
    target: &str,
    protocol: &str,
    tls_mode: Option<&str>,
    nft_redirect_port: Option<u16>,
    http2: bool,
    revision: u64,
) -> anyhow::Result<ProxyDesired> {
    let Some(original_port) = nft_redirect_port else {
        anyhow::bail!(
            "desired proxy for service {} needs nft_redirect_port (the original service port)",
            service_id
        );
    };
    if original_port == 0 {
        anyhow::bail!("desired proxy original port must not be 0");
    }
    let spec = parse_proxy_spec(
        name,
        listen,
        target,
        protocol,
        tls_mode.map(String::from),
        Some(original_port),
        None,
        http2,
    )?;
    Ok(ProxyDesired {
        service_id,
        name: spec.name,
        listen: spec.listen,
        target: spec.target,
        protocol: match spec.protocol {
            Protocol::Tcp => TransportProto::Tcp,
            Protocol::Udp => TransportProto::Udp,
        },
        original_port,
        tls_mode: spec.tls_mode,
        http2: spec.http2,
        revision,
    })
}
