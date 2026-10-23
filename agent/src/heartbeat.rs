use std::sync::Arc;
use std::time::Instant;

use log::{debug, error};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::config::AgentConfig;
use crate::harpoon::HarpoonEngine;
use crate::nats::NatsClient;
use crate::sniffer::SharedSnifferHealth;
use crate::updates::PortsCache;

#[derive(Serialize)]
pub struct AgentStatus {
    pub agent_id: String,
    pub uptime_secs: u64,
    pub sniffer_active: bool,
    pub sniffer_ports: Vec<u16>,
    pub harpoon_rules: Vec<String>,
    pub active_filters: usize,
    pub nft_redirects: Vec<NftRedirectStatus>,
    pub banned_ips: Vec<String>,
    pub tls_available: bool,
    /// nft binary present on this host (proxy redirect / IP bans need it).
    pub nft_available: bool,
    /// Crate version + building commit, e.g. "0.1.0+df78fe2" — lets the
    /// backend's /api/version surface drift across deployed agents.
    pub version: String,
    pub broadcast_drops: u64,
    /// Packets dropped because the sniffer's capture -> NATS queue was full.
    pub sniffer_queue_drops: u64,
    pub timestamp_ms: u64,
}

#[derive(Serialize)]
pub struct NftRedirectStatus {
    pub protocol: String,
    pub from_port: u16,
    pub to_port: u16,
}

pub fn spawn_heartbeat(
    nats: NatsClient,
    config: &AgentConfig,
    ports_cache: PortsCache,
    harpoon: Arc<Mutex<HarpoonEngine>>,
    tls_available: bool,
    sniffer_health: SharedSnifferHealth,
) {
    let interval_secs = config.heartbeat_interval_secs;
    let agent_id = nats.agent_id.clone();
    let started_at = Instant::now();

    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            let ports: Vec<u16> = ports_cache.read().unwrap().iter().cloned().collect();

            let harpoon_guard = harpoon.lock().await;
            let harpoon_rules = harpoon_guard.active_rule_names();
            let active_filters = harpoon_guard.filter_count();
            let banned_ips = harpoon_guard.banned_ips();
            let broadcast_drops = harpoon_guard.capture_manager().broadcast_drops();
            let nft_redirects: Vec<NftRedirectStatus> = harpoon_guard
                .nft_rules()
                .iter()
                .map(|r| NftRedirectStatus {
                    protocol: r.protocol.clone(),
                    from_port: r.original_port,
                    to_port: r.redirect_port,
                })
                .collect();
            drop(harpoon_guard);

            let status = AgentStatus {
                agent_id: agent_id.clone(),
                uptime_secs: started_at.elapsed().as_secs(),
                sniffer_active: sniffer_health.active(),
                sniffer_ports: ports,
                harpoon_rules,
                active_filters,
                nft_redirects,
                banned_ips,
                tls_available,
                nft_available: crate::nftables::nft_binary_available(),
                version: crate::version::FULL.to_string(),
                broadcast_drops,
                sniffer_queue_drops: sniffer_health.queue_drops(),
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            };

            match serde_json::to_vec(&status) {
                Ok(json) => {
                    nats.send_heartbeat(json).await;
                    debug!("Heartbeat sent (uptime: {}s)", status.uptime_secs);
                }
                Err(e) => error!("Failed to serialize heartbeat: {}", e),
            }
        }
    });
}
