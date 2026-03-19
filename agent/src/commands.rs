use std::sync::Arc;

use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::sync::Mutex;

use harpoon_core::types::endpoint::Protocol;

use crate::harpoon::{HarpoonEngine, ProxyRuleSpec};
use crate::nats::NatsClient;
use crate::updates::{PortsCache, RestartNotifier};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum AgentCommand {
    // Phase 1
    SnifferConfig { ports: Vec<u16> },
    Shutdown,

    // Phase 2
    ProxyAdd {
        name: String,
        listen: String,
        target: String,
        protocol: String,
    },
    ProxyUpdate {
        name: String,
        listen: String,
        target: String,
        protocol: String,
    },
    ProxyRemove {
        name: String,
    },

    // Phase 3+
    NftablesApply {
        rules: Vec<String>,
    },
    NftablesClear,
    FilterSync {
        filters: Vec<serde_json::Value>,
    },
    ReplayForward {
        listen_port: u16,
        target_addr: String,
    },
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

        info!(
            "Listening for commands on agent_commands.{}",
            nats.agent_id
        );

        while let Some(msg) = sub.next().await {
            let cmd: AgentCommand = match serde_json::from_slice(&msg.payload) {
                Ok(c) => c,
                Err(e) => {
                    warn!("Invalid command payload: {}", e);
                    continue;
                }
            };

            debug!("Received command: {:?}", cmd);
            handle_command(cmd, &ports_cache, &restart_tx, &harpoon).await;
        }
    });
}

async fn handle_command(
    cmd: AgentCommand,
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
            harpoon.lock().await.shutdown().await;
            std::process::exit(0);
        }

        AgentCommand::ProxyAdd {
            name,
            listen,
            target,
            protocol,
        } => {
            let spec = match parse_proxy_spec(&name, &listen, &target, &protocol) {
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
        } => {
            let spec = match parse_proxy_spec(&name, &listen, &target, &protocol) {
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

        // Phase 3+
        AgentCommand::NftablesApply { .. } => {
            warn!("NftablesApply not implemented yet (Phase 3)");
        }
        AgentCommand::NftablesClear => {
            warn!("NftablesClear not implemented yet (Phase 3)");
        }
        AgentCommand::FilterSync { .. } => {
            warn!("FilterSync not implemented yet (Phase 4)");
        }
        AgentCommand::ReplayForward { .. } => {
            warn!("ReplayForward not implemented yet (Phase 5)");
        }
    }
}

fn parse_proxy_spec(
    name: &str,
    listen: &str,
    target: &str,
    protocol: &str,
) -> anyhow::Result<ProxyRuleSpec> {
    let protocol = match protocol.to_lowercase().as_str() {
        "tcp" => Protocol::Tcp,
        "udp" => Protocol::Udp,
        other => anyhow::bail!("Unknown protocol: {}", other),
    };

    Ok(ProxyRuleSpec {
        name: name.to_string(),
        listen: listen.parse()?,
        target: target.parse()?,
        protocol,
    })
}
