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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum AgentCommand {
    // Phase 1
    SnifferConfig { ports: Vec<u16> },
    Shutdown,

    // Phase 2: proxy management
    ProxyAdd {
        name: String,
        listen: String,
        target: String,
        protocol: String,
        /// TLS mode: "mitm", "terminate", "passthrough", or omit for no TLS
        tls_mode: Option<String>,
        /// If set, create nftables redirect from this port to the listen port
        nft_redirect_port: Option<u16>,
    },
    ProxyUpdate {
        name: String,
        listen: String,
        target: String,
        protocol: String,
        tls_mode: Option<String>,
        nft_redirect_port: Option<u16>,
    },
    ProxyRemove {
        name: String,
    },

    // Phase 3: nftables (manual management)
    NftablesApply {
        rules: Vec<RedirectRule>,
    },
    NftablesClear,

    // Phase 4: firewall filters
    FilterSync {
        filters: Vec<FilterDef>,
    },

    // Phase 5+
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
            tls_mode,
            nft_redirect_port,
        } => {
            let spec = match parse_proxy_spec(
                &name,
                &listen,
                &target,
                &protocol,
                tls_mode,
                nft_redirect_port,
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
        } => {
            let spec = match parse_proxy_spec(
                &name,
                &listen,
                &target,
                &protocol,
                tls_mode,
                nft_redirect_port,
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
                        debug!("Filter '{}': {} {} -> {}", f.name, f.kind, f.direction, f.action);
                        parsed.push(filter);
                    }
                    Err(e) => {
                        error!("Invalid filter '{}': {}", f.name, e);
                        errors += 1;
                    }
                }
            }
            if errors > 0 {
                warn!("{} filters failed to parse, applying {} valid ones", errors, parsed.len());
            }
            if let Err(e) = harpoon.lock().await.sync_filters(parsed).await {
                error!("Failed to sync filters: {}", e);
            }
        }

        // Phase 5+
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
    tls_mode: Option<String>,
    nft_redirect_port: Option<u16>,
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
        tls_mode,
        nft_redirect_port,
    })
}
