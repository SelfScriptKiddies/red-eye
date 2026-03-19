use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::Deserialize;

use crate::nats::NatsClient;
use crate::updates::{PortsCache, RestartNotifier};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum AgentCommand {
    // Phase 1
    SnifferConfig { ports: Vec<u16> },
    Shutdown,

    // Phase 2+ (skeleton)
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
            handle_command(cmd, &ports_cache, &restart_tx);
        }
    });
}

fn handle_command(cmd: AgentCommand, ports_cache: &PortsCache, restart_tx: &RestartNotifier) {
    match cmd {
        AgentCommand::SnifferConfig { ports } => {
            info!("Updating sniffer ports: {:?}", ports);
            let mut cache = ports_cache.write().unwrap();
            *cache = ports.into_iter().collect();
            let _ = restart_tx.send(());
        }

        AgentCommand::Shutdown => {
            info!("Shutdown command received");
            std::process::exit(0);
        }

        // Phase 2+ stubs
        AgentCommand::ProxyAdd { name, .. } => {
            warn!("ProxyAdd '{}' not implemented yet (Phase 2)", name);
        }
        AgentCommand::ProxyUpdate { name, .. } => {
            warn!("ProxyUpdate '{}' not implemented yet (Phase 2)", name);
        }
        AgentCommand::ProxyRemove { name } => {
            warn!("ProxyRemove '{}' not implemented yet (Phase 2)", name);
        }
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
