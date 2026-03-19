use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use log::{debug, info, warn};

use harpoon_core::capture::CaptureManager;
use harpoon_core::engine::run_with_capture;
use harpoon_core::types::endpoint::{Endpoint, Protocol};
use harpoon_core::types::rule::{Rule, TlsConfig, TlsMode, UdpSourceMode};
use harpoon_core::{CoreConfig, EngineHandle};

use crate::nftables::{NftablesManager, RedirectRule};
use crate::packet_synthesis;
use crate::tls_certs::TlsCerts;
use crate::updates::{PortsCache, RestartNotifier};

/// Maximum capture session duration (seconds). Effectively infinite.
const CAPTURE_TIMEOUT_SECS: u64 = 60 * 60 * 24 * 365;
/// Ring buffer size per rule. We consume via broadcast, so this is just a safety buffer.
const CAPTURE_MAX_PACKETS: usize = 1000;
/// Max payload bytes to capture per packet.
const CAPTURE_MAX_PAYLOAD: usize = 65535;

/// Proxy rule definition received from the central server.
#[derive(Debug, Clone)]
pub struct ProxyRuleSpec {
    pub name: String,
    pub listen: SocketAddr,
    pub target: SocketAddr,
    pub protocol: Protocol,
    /// TLS mode: None = no TLS, Some("mitm"|"terminate"|"passthrough")
    pub tls_mode: Option<String>,
    /// If set, create nftables redirect from this port to listen port
    pub nft_redirect_port: Option<u16>,
}

impl ProxyRuleSpec {
    /// Convert to a harpoon-core Rule.
    fn to_harpoon_rule(&self, tls_certs: &Option<TlsCerts>) -> Rule {
        let endpoint = match self.protocol {
            Protocol::Tcp => Endpoint::tcp,
            Protocol::Udp => Endpoint::udp,
        };

        let tls = self.tls_mode.as_deref().and_then(|mode| {
            let tls_certs = tls_certs.as_ref()?;
            let mode = match mode {
                "mitm" => TlsMode::Mitm,
                "terminate" => TlsMode::Terminate,
                "passthrough" => TlsMode::Passthrough,
                _ => return None,
            };
            Some(TlsConfig {
                mode,
                ca_cert_path: tls_certs.ca_cert_path.clone(),
                ca_key_path: tls_certs.ca_key_path.clone(),
            })
        });

        Rule {
            name: self.name.clone(),
            listen: endpoint(self.listen),
            target: endpoint(self.target),
            filters: vec![],
            duplicate: None,
            exporter: None,
            tls,
            udp_source_mode: UdpSourceMode::Proxy,
            idle_timeout_secs: Rule::default_idle_timeout(),
        }
    }

    pub fn listen_port(&self) -> u16 {
        self.listen.port()
    }
}

pub struct HarpoonEngine {
    ports_cache: PortsCache,
    restart_tx: RestartNotifier,

    handle: Option<EngineHandle>,
    capture_mgr: Arc<CaptureManager>,
    rules: HashMap<String, ProxyRuleSpec>,
    tls_certs: Option<TlsCerts>,
    nft: NftablesManager,
}

impl HarpoonEngine {
    pub fn new(
        ports_cache: PortsCache,
        restart_tx: RestartNotifier,
        tls_certs: Option<TlsCerts>,
    ) -> Self {
        Self {
            ports_cache,
            restart_tx,
            handle: None,
            capture_mgr: CaptureManager::new(),
            rules: HashMap::new(),
            tls_certs,
            nft: NftablesManager::new(),
        }
    }

    /// Add a proxy rule, set up nftables if needed, and restart the engine.
    pub async fn add_rule(&mut self, spec: ProxyRuleSpec) -> Result<()> {
        info!(
            "Adding proxy rule '{}': {} -> {}{}{}",
            spec.name,
            spec.listen,
            spec.target,
            spec.tls_mode
                .as_deref()
                .map(|m| format!(" (TLS: {})", m))
                .unwrap_or_default(),
            spec.nft_redirect_port
                .map(|p| format!(" (nft: {} -> {})", p, spec.listen_port()))
                .unwrap_or_default(),
        );

        // Set up nftables redirect if requested
        if let Some(original_port) = spec.nft_redirect_port {
            self.nft.add_redirect(RedirectRule {
                protocol: match spec.protocol {
                    Protocol::Tcp => "tcp".to_string(),
                    Protocol::Udp => "udp".to_string(),
                },
                original_port,
                redirect_port: spec.listen_port(),
            })?;
        }

        let port = spec.listen_port();
        self.rules.insert(spec.name.clone(), spec);
        self.restart_engine().await?;
        self.exclude_port_from_sniffer(port);

        Ok(())
    }

    /// Remove a proxy rule, clean up nftables, and restart the engine.
    pub async fn remove_rule(&mut self, name: &str) -> Result<()> {
        let spec = self
            .rules
            .remove(name)
            .context(format!("Rule '{}' not found", name))?;

        info!("Removing proxy rule '{}'", name);

        // Remove nftables redirect
        if let Some(original_port) = spec.nft_redirect_port {
            let _ = self.nft.remove_redirect(original_port);
        }

        let port = spec.listen_port();
        self.restart_engine().await?;
        self.return_port_to_sniffer(port);

        Ok(())
    }

    /// Update a proxy rule.
    pub async fn update_rule(&mut self, spec: ProxyRuleSpec) -> Result<()> {
        // Remove old nftables rule if present
        if let Some(old_spec) = self.rules.get(&spec.name) {
            if let Some(old_nft_port) = old_spec.nft_redirect_port {
                let _ = self.nft.remove_redirect(old_nft_port);
            }
        }

        let old_port = self.rules.get(&spec.name).map(|r| r.listen_port());
        let new_port = spec.listen_port();

        info!(
            "Updating proxy rule '{}': {} -> {}",
            spec.name, spec.listen, spec.target
        );

        // Add new nftables rule if requested
        if let Some(original_port) = spec.nft_redirect_port {
            self.nft.add_redirect(RedirectRule {
                protocol: match spec.protocol {
                    Protocol::Tcp => "tcp".to_string(),
                    Protocol::Udp => "udp".to_string(),
                },
                original_port,
                redirect_port: spec.listen_port(),
            })?;
        }

        self.rules.insert(spec.name.clone(), spec);
        self.restart_engine().await?;

        if let Some(old) = old_port {
            if old != new_port {
                self.return_port_to_sniffer(old);
            }
        }
        self.exclude_port_from_sniffer(new_port);

        Ok(())
    }

    /// Apply a set of manual nftables redirect rules (replaces all current manual rules).
    pub fn apply_nft_rules(&mut self, rules: Vec<RedirectRule>) -> Result<()> {
        self.nft.apply_rules(rules)
    }

    /// Clear all nftables rules.
    pub fn clear_nft_rules(&mut self) -> Result<()> {
        self.nft.clear()
    }

    /// Names of currently active proxy rules.
    pub fn active_rule_names(&self) -> Vec<String> {
        self.rules.keys().cloned().collect()
    }

    /// Get the shared capture manager (for spawning the forwarder).
    pub fn capture_manager(&self) -> Arc<CaptureManager> {
        self.capture_mgr.clone()
    }

    /// Current nftables rules (for status reporting).
    pub fn nft_rules(&self) -> Vec<RedirectRule> {
        self.nft.active_rules().to_vec()
    }

    /// Stop the engine, clean up nftables.
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.stop();
            let _ = handle.shutdown().await;
            info!("Harpoon engine shut down");
        }
        let _ = self.nft.clear();
    }

    async fn restart_engine(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            handle.stop();
            let _ = handle.shutdown().await;
            debug!("Previous harpoon engine stopped");
        }

        if self.rules.is_empty() {
            info!("No proxy rules, harpoon engine idle");
            return Ok(());
        }

        let rules: Vec<Rule> = self
            .rules
            .values()
            .map(|s| s.to_harpoon_rule(&self.tls_certs))
            .collect();
        let config = CoreConfig {
            rules,
            pipelines: vec![],
            ..Default::default()
        };

        let handle: EngineHandle = run_with_capture(config, self.capture_mgr.clone())
            .await
            .context("Failed to start harpoon engine")?;

        for name in self.rules.keys() {
            if let Err(e) = self
                .capture_mgr
                .start(
                    name.clone(),
                    CAPTURE_MAX_PACKETS,
                    CAPTURE_MAX_PAYLOAD,
                    CAPTURE_TIMEOUT_SECS,
                )
                .await
            {
                warn!("Failed to start capture for rule '{}': {}", name, e);
            }
        }

        self.handle = Some(handle);
        info!(
            "Harpoon engine started with {} proxy rules",
            self.rules.len()
        );

        Ok(())
    }

    fn exclude_port_from_sniffer(&self, port: u16) {
        let removed = self.ports_cache.write().unwrap().remove(&port);
        if removed {
            let _ = self.restart_tx.send(());
            debug!("Port {} excluded from sniffer (now proxied)", port);
        }
    }

    fn return_port_to_sniffer(&self, port: u16) {
        let still_proxied = self.rules.values().any(|r| r.listen_port() == port);
        if !still_proxied {
            self.ports_cache.write().unwrap().insert(port);
            let _ = self.restart_tx.send(());
            debug!("Port {} returned to sniffer", port);
        }
    }
}

/// Spawn a background task that forwards captured packets from harpoon to NATS.
pub fn spawn_capture_forwarder(
    capture_mgr: Arc<CaptureManager>,
    nats: crate::nats::NatsClient,
) {
    tokio::spawn(async move {
        let mut rx = capture_mgr.subscribe();
        info!("Harpoon capture forwarder started");

        loop {
            match rx.recv().await {
                Ok(packet) => {
                    if let Some(encoded) = packet_synthesis::encode_for_nats(&packet) {
                        nats.send_packet(encoded).await;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Capture forwarder lagged, missed {} packets", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    debug!("Capture broadcast channel closed");
                    break;
                }
            }
        }
    });
}
