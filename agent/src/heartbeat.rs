use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use log::{debug, error};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::harpoon::HarpoonEngine;
use crate::nats::NatsClient;
use crate::updates::AppState;

#[derive(Serialize)]
pub struct AgentStatus {
    pub agent_id: String,
    pub uptime_secs: u64,
    pub sniffer_active: bool,
    /// Monitored (protocol, port) pairs, e.g. "8080/tcp"
    pub sniffer_ports: Vec<String>,
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
    /// Fresh UUID per agent process start; the backend publishes a full
    /// configuration snapshot whenever it changes.
    pub boot_id: String,
    /// Bootstrap interface (env/CLI value this agent was started with)
    pub interface: String,
    /// Bootstrap machine IP (env/CLI value)
    pub machine_ip: String,
    /// Configuration revision the agent has fully applied
    pub applied_revision: u64,
    /// Effective interface in use (override or bootstrap)
    pub effective_interface: String,
    /// Effective machine IP in use
    pub effective_machine_ip: String,
    /// Sanitized snapshot application error (absent = ok)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync_error: Option<String>,
    /// Capture is idle because no services are assigned (healthy)
    pub sniffer_idle: bool,
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
    harpoon: Arc<Mutex<HarpoonEngine>>,
    state: Arc<AppState>,
    tls_available: bool,
) {
    let interval_secs = state.config_source.heartbeat_interval_secs;
    let agent_id = nats.agent_id.clone();
    let started_at = Instant::now();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            let mut ports: Vec<String> = state
                .ports
                .read()
                .unwrap()
                .iter()
                .map(|(proto, port)| format!("{}/{}", port, proto.as_str()))
                .collect();
            ports.sort();

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

            let (effective_interface, effective_machine_ip) = {
                let config = state.config.read().unwrap();
                (config.interface.clone(), config.machine_ip.to_string())
            };
            let last_sync_error = state.last_sync_error.read().unwrap().clone();
            let status = AgentStatus {
                agent_id: agent_id.clone(),
                uptime_secs: started_at.elapsed().as_secs(),
                sniffer_active: state.health.active(),
                sniffer_idle: state.health.idle_no_services(),
                sniffer_ports: ports,
                harpoon_rules,
                active_filters,
                nft_redirects,
                banned_ips,
                tls_available,
                nft_available: crate::nftables::nft_binary_available(),
                version: crate::version::FULL.to_string(),
                broadcast_drops,
                sniffer_queue_drops: state.health.queue_drops(),
                boot_id: state.boot_id.clone(),
                interface: state.config_source.interface.clone(),
                machine_ip: state.config_source.machine_ip.clone(),
                applied_revision: state.applied_revision.load(Ordering::SeqCst),
                effective_interface,
                effective_machine_ip,
                last_sync_error,
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
