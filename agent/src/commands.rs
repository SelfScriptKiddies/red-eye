use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::sync::Mutex;

use harpoon_core::types::endpoint::Protocol;

use crate::harpoon::{self, HarpoonEngine, ProxyRuleSpec};
use crate::nats::NatsClient;
use crate::nftables::RedirectRule;
use crate::updates::{PortsCache, RestartNotifier};

#[derive(Debug, Deserialize)]
pub struct FilterDef {
    pub name: String,
    /// "substr", "bsubstr", "regex"
    pub kind: String,
    /// Pattern string (or hex for bsubstr)
    pub pattern: String,
    /// "c2s", "s2c", "both"
    #[serde(default = "default_direction")]
    pub direction: String,
    /// "drop", "alert", "pass"
    #[serde(default = "default_action")]
    pub action: String,
}

fn default_direction() -> String {
    "both".to_string()
}

fn default_action() -> String {
    "drop".to_string()
}

fn default_tcp() -> String {
    "tcp".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum AgentCommand {
    SnifferConfig {
        ports: Vec<u16>,
    },
    Shutdown,

    // Proxy management
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
    },
    ProxyRemove {
        name: String,
    },

    // nftables manual management
    NftablesApply {
        rules: Vec<RedirectRule>,
    },
    NftablesClear,

    // Firewall filters
    FilterSync {
        filters: Vec<FilterDef>,
    },

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
    ports_cache: PortsCache,
    restart_tx: RestartNotifier,
    harpoon: Arc<Mutex<HarpoonEngine>>,
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
            handle_command(cmd, &nats, &ports_cache, &restart_tx, &harpoon).await;
        }
    });
}

async fn handle_command(
    cmd: AgentCommand,
    nats: &NatsClient,
    ports_cache: &PortsCache,
    restart_tx: &RestartNotifier,
    harpoon: &Arc<Mutex<HarpoonEngine>>,
) {
    match cmd {
        AgentCommand::SnifferConfig { ports } => {
            info!("Updating sniffer ports: {:?}", ports);
            let mut cache = ports_cache.write().unwrap();
            *cache = ports.into_iter().collect();
            let _ = restart_tx.send(());
        }

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
        } => {
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
            if let Err(e) = harpoon.lock().await.add_rule(spec).await {
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
        } => {
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
            if let Err(e) = harpoon.lock().await.update_rule(spec).await {
                error!("Failed to update proxy '{}': {}", name, e);
            }
        }

        AgentCommand::ProxyRemove { name } => {
            if let Err(e) = harpoon.lock().await.remove_rule(&name).await {
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

        AgentCommand::FilterSync { filters } => {
            info!("Syncing {} firewall filters", filters.len());
            let mut parsed = Vec::new();
            let mut errors = 0;
            for f in &filters {
                match harpoon::parse_filter(&f.kind, &f.pattern, &f.direction, &f.action) {
                    Ok(filter) => {
                        debug!(
                            "Filter '{}': {} {} -> {}",
                            f.name, f.kind, f.direction, f.action
                        );
                        parsed.push(filter);
                    }
                    Err(e) => {
                        error!("Invalid filter '{}': {}", f.name, e);
                        errors += 1;
                    }
                }
            }
            if errors > 0 {
                warn!(
                    "{} filters failed to parse, applying {} valid ones",
                    errors,
                    parsed.len()
                );
            }
            if let Err(e) = harpoon.lock().await.sync_filters(parsed).await {
                error!("Failed to sync filters: {}", e);
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
                listen: format!("0.0.0.0:{}", listen_port).parse().unwrap(),
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
            if let Err(e) = harpoon.lock().await.add_rule(spec).await {
                error!("Failed to start replay '{}': {}", name, e);
            }
        }

        AgentCommand::ReplayStop { name } => {
            let rule_name = format!("replay-{}", name);
            info!("Stopping replay forward '{}'", name);
            if let Err(e) = harpoon.lock().await.remove_rule(&rule_name).await {
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
        let cmd: AgentCommand =
            serde_json::from_str(r#"{"type":"SnifferConfig","ports":[80],"future":true}"#)
                .expect("parses with extra fields");
        assert!(matches!(cmd, AgentCommand::SnifferConfig { ports } if ports == vec![80]));
    }
}

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
