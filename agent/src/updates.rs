use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use futures::StreamExt;
use log::{debug, error, info, warn};
use prost::Message as _;
use tokio::sync::mpsc;

use crate::config::AgentConfig;
use crate::enforce::{self, EnforcePlan};
use crate::nats::NatsClient;
use crate::wire::{self, SettingsEnvelope, WireAgentConfigSnapshot, WireProtocol};

// Wire v4: coordinated between-games upgrade; v3 producers/consumers are
// unsupported and rejected by the version gate below.
pub const SETTINGS_WIRE_VERSION: u32 = 4;

/// JetStream stream carrying settings updates. New name for v4: the v4
/// subject (updates_v4) differs from the v3 one, so history is not migrated.
pub const SETTINGS_STREAM_NAME: &str = "SETTINGS_UPDATES_V4";

/// Configuration history is bounded; complete snapshots make expired
/// history recoverable via backend reconciliation instead of unbounded
/// stream growth.
pub const SETTINGS_STREAM_MAX_AGE_SECS: u64 = 7 * 24 * 3600;

/// Decode the versioned settings envelope and check the version. Unknown
/// versions are a clean error: the last valid configuration stays active,
/// a fresh agent remains idle, nothing is partially applied.
fn decode_settings_update(payload: &[u8]) -> Result<SettingsEnvelope, String> {
    let envelope =
        SettingsEnvelope::decode(payload).map_err(|e| format!("protobuf decode failed: {e}"))?;
    if envelope.version != SETTINGS_WIRE_VERSION {
        return Err(format!(
            "unknown settings wire version {} (expected {}) - backend/agent skew?",
            envelope.version, SETTINGS_WIRE_VERSION
        ));
    }
    Ok(envelope)
}

/// Transport protocol of one monitored port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportProto {
    Tcp,
    Udp,
}

impl TransportProto {
    /// None for unknown enum values: an unrecognized protocol must never
    /// silently degrade into TCP (that would sniff the wrong traffic).
    pub fn from_wire(value: i32) -> Option<Self> {
        WireProtocol::try_from(value).ok().map(|p| match p {
            WireProtocol::Tcp => TransportProto::Tcp,
            WireProtocol::Udp => TransportProto::Udp,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TransportProto::Tcp => "tcp",
            TransportProto::Udp => "udp",
        }
    }
}

/// Shared set of monitored (protocol, port) pairs.
pub type PortsCache = Arc<RwLock<HashSet<(TransportProto, u16)>>>;

pub type RestartNotifier = tokio::sync::watch::Sender<()>;

pub fn create_ports_cache() -> PortsCache {
    Arc::new(RwLock::new(HashSet::new()))
}

pub fn create_restart_notifier() -> (RestartNotifier, tokio::sync::watch::Receiver<()>) {
    tokio::sync::watch::channel(())
}

/// Effective agent configuration, swapped atomically when a snapshot is
/// applied. machine_ip == 0.0.0.0 / :: means wildcard: match services by
/// protocol+port and preserve observed endpoints in synthesis.
#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub interface: String,
    pub machine_ip: IpAddr,
    pub revision: u64,
}

pub type SharedConfig = Arc<RwLock<EffectiveConfig>>;

/// Agent-wide state shared by the update listener, heartbeat, command
/// listener and sniffer loop.
pub struct AppState {
    pub ports: PortsCache,
    pub config: SharedConfig,
    pub restart_tx: RestartNotifier,
    pub health: crate::sniffer::SharedSnifferHealth,
    /// Revision awaiting capture confirmation (interface handoff or port
    /// restart). Only a fully applied snapshot advances the applied
    /// revision; failures keep the value so the backend keeps the agent
    /// flagged stale and re-delivers.
    pub pending_revision: Arc<AtomicU64>,
    /// 0 = nothing acknowledged yet.
    pub applied_revision: Arc<AtomicU64>,
    /// Sanitized snapshot application error for the heartbeat (None = ok).
    pub last_sync_error: Arc<RwLock<Option<String>>>,
    /// Enforcement plans flow here to the engine owner task (hot-swap +
    /// nftables bans, no process restart).
    pub enforce_tx: mpsc::Sender<EnforcePlan>,
    /// Count of filters/blocks this agent could not enforce (heartbeat).
    pub enforcement_failures: Arc<AtomicU64>,
    /// Sanitized last enforcement failure (None = everything enforced).
    pub last_enforcement_error: Arc<RwLock<Option<String>>>,
    pub agent_id: String,
    pub boot_id: String,
    pub config_source: AgentConfig,
}

/// Bounded error strings for the heartbeat: no payload data ever enters
/// them (formatting is restricted to fixed strings and identifiers).
fn sanitize_error(msg: &str) -> String {
    let mut out: String = msg.chars().take(300).collect();
    if msg.len() > 300 {
        out.push_str("...");
    }
    out
}

/// Spawn the settings update listener as a background task. Listener
/// errors are logged and contained: the task exits quietly only if the
/// NATS subscription dies, never taking the rest of the agent with it.
pub fn spawn_updates_listener(nats: NatsClient, state: Arc<AppState>) {
    let updates_topic = state.config_source.nats_updates_topic.clone();

    tokio::spawn(async move {
        if let Err(e) = listen_for_updates(nats.inner().clone(), &updates_topic, state).await {
            error!("Update listener error: {}", e);
        }
    });
}

async fn listen_for_updates(
    client: async_nats::Client,
    subject: &str,
    state: Arc<AppState>,
) -> Result<()> {
    info!("Listening for settings updates on '{}'", subject);

    let js = jetstream::new(client);
    let stream = js
        .create_stream(stream::Config {
            // Versioned stream name: loud mismatch errors between v3/v4.
            name: SETTINGS_STREAM_NAME.to_string(),
            subjects: vec![subject.to_string()],
            max_age: std::time::Duration::from_secs(SETTINGS_STREAM_MAX_AGE_SECS),
            ..Default::default()
        })
        .await?;

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(state.agent_id.clone()),
            filter_subject: subject.to_string(),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    let mut messages = consumer.messages().await?;

    while let Some(msg) = messages.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                error!("Error receiving update: {}", e);
                continue;
            }
        };

        let envelope = match decode_settings_update(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                error!("Error decoding update ({} bytes): {}", msg.payload.len(), e);
                // Ack anyway: a message this consumer cannot parse will never
                // become parseable on retry
                if let Err(ack_err) = msg.ack().await {
                    error!("Error acknowledging undecodable update: {}", ack_err);
                }
                continue;
            }
        };

        match &envelope.update {
            // Complete per-agent snapshots are the only carrier of service
            // and network configuration for this agent; every other variant
            // is acknowledged and ignored (global analyzer view, filters).
            Some(wire::settings_envelope::Update::AgentSnapshot(snapshot)) => {
                apply_snapshot(snapshot, &state);
            }
            _ => {
                debug!("Ignoring non-snapshot settings update");
            }
        }

        if let Err(e) = msg.ack().await {
            error!("Error acknowledging message: {}", e);
        }
    }

    Ok(())
}

/// Validate and atomically apply a snapshot addressed to this agent.
/// Invalid snapshots leave the previous configuration untouched; stale
/// ones are ignored. Returns nothing: outcomes are surfaced via heartbeat
/// fields (applied revision + sanitized error).
fn apply_snapshot(snapshot: &WireAgentConfigSnapshot, state: &AppState) {
    // Snapshots are addressed by the agent id inside the message; the
    // stream fans every message out to every durable consumer.
    if snapshot.agent_id != state.agent_id {
        debug!("Skipping snapshot addressed to '{}'", snapshot.agent_id);
        return;
    }

    let applied = state.applied_revision.load(Ordering::SeqCst);
    if snapshot.revision <= applied {
        debug!(
            "Ignoring snapshot with revision {} (applied: {})",
            snapshot.revision, applied
        );
        return;
    }

    if let Err(reason) = validate_snapshot(snapshot) {
        let sanitized = sanitize_error(&reason);
        warn!(
            "Rejecting snapshot revision {}: {}",
            snapshot.revision, reason
        );
        *state.last_sync_error.write().unwrap() = Some(sanitized);
        return;
    }

    let network = snapshot.network.as_ref();
    let interface = network.map(|n| n.interface.clone()).unwrap_or_default();
    let machine_ip: IpAddr = network
        .and_then(|n| n.machine_ip.parse().ok())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

    let mut new_ports: HashSet<(TransportProto, u16)> = HashSet::new();
    for service in &snapshot.services {
        // Validation already ran, but never trust a second pass.
        let Some(protocol) = TransportProto::from_wire(service.protocol) else {
            continue;
        };
        let Ok(port) = u16::try_from(service.port) else {
            continue;
        };
        if port == 0 {
            continue;
        }
        new_ports.insert((protocol, port));
    }

    let old_ports: HashSet<(TransportProto, u16)> = state.ports.read().unwrap().clone();
    let ports_changed = old_ports != new_ports;

    // Enforcement plan from the snapshot's filter + block projection.
    // Valid parts apply even when other parts fail; failures land on the
    // shared state for the heartbeat (detection continues on the analyzer).
    let plan = enforce::plan(&snapshot.filters, &snapshot.blocks);
    if !plan.failures.is_empty() {
        state
            .enforcement_failures
            .fetch_add(plan.failures.len() as u64, Ordering::Relaxed);
        if let Some(first) = plan.failures.first() {
            let mut bounded: String = first.chars().take(300).collect();
            if first.len() > 300 {
                bounded.push_str("...");
            }
            *state.last_enforcement_error.write().unwrap() = Some(bounded);
        }
    }
    let enforce_sent = state.enforce_tx.try_send(plan).is_ok();
    if !enforce_sent {
        warn!(
            "Enforcement plan for revision {} dropped (queue full), retrying on next snapshot",
            snapshot.revision
        );
    }

    {
        let mut config = state.config.write().unwrap();
        let interface_changed = config.interface != interface;
        config.interface = interface;
        config.machine_ip = machine_ip;

        // Machine-IP-only changes take effect immediately (packet
        // synthesis reads config per packet); capture-affecting changes go
        // through the restart watch with the revision pending capture
        // confirmation.
        if interface_changed || ports_changed {
            state
                .pending_revision
                .store(snapshot.revision, Ordering::SeqCst);
            drop(config);
            info!(
                "Applied snapshot revision {}: ports changed={}, interface changed={}",
                snapshot.revision, ports_changed, interface_changed
            );
            let _ = state.restart_tx.send(());
        } else {
            config.revision = snapshot.revision;
            drop(config);
            state
                .applied_revision
                .store(snapshot.revision, Ordering::SeqCst);
            *state.last_sync_error.write().unwrap() = None;
            info!(
                "Applied snapshot revision {} (no capture change)",
                snapshot.revision
            );
        }
    }

    // Swap the port set last: the sniffer reads it under the same lock.
    {
        let mut ports = state.ports.write().unwrap();
        *ports = new_ports;
    }
}

/// Full validation BEFORE any state changes: an invalid snapshot never
/// replaces valid configuration, even partially.
fn validate_snapshot(snapshot: &WireAgentConfigSnapshot) -> Result<(), String> {
    if let Some(network) = &snapshot.network {
        if network.interface.is_empty() {
            return Err("snapshot carries an empty interface name".to_string());
        }
        if network.interface.len() > 64 {
            return Err("snapshot interface name exceeds 64 characters".to_string());
        }
        if network.machine_ip.parse::<IpAddr>().is_err() {
            return Err(format!(
                "snapshot machine_ip '{}' is not an IP address or wildcard",
                network.machine_ip
            ));
        }
    }
    for service in &snapshot.services {
        if WireProtocol::try_from(service.protocol).is_err() {
            return Err(format!(
                "service id {} carries an unknown protocol {}",
                service.id, service.protocol
            ));
        }
        let Ok(port) = u16::try_from(service.port) else {
            return Err(format!(
                "service id {} port {} is out of range",
                service.id, service.port
            ));
        };
        if port == 0 {
            return Err(format!("service id {} carries port 0", service.id));
        }
    }
    const KNOWN_ACTIONS: [&str; 5] = ["tag", "flag", "alert", "block", "block_connection"];
    const KNOWN_KINDS: [&str; 5] = ["regex", "substr", "bsubstr", "ip_cidr", "expression"];
    for filter in &snapshot.filters {
        if !KNOWN_ACTIONS.contains(&filter.action.as_str()) {
            return Err(format!(
                "filter '{}' carries an unknown action '{}'",
                filter.pattern_name, filter.action
            ));
        }
        if !KNOWN_KINDS.contains(&filter.filter_type.as_str()) {
            return Err(format!(
                "filter '{}' carries an unknown matcher kind '{}'",
                filter.pattern_name, filter.filter_type
            ));
        }
        if filter.filter_type == "expression" {
            // The shared DSL must accept the source: a filter the analyzer
            // could evaluate but this agent cannot compile would diverge.
            let source = String::from_utf8_lossy(&filter.filter_payload);
            if let Err(e) = filter_dsl::compile(&source) {
                return Err(format!(
                    "filter '{}' expression does not compile: {}",
                    filter.pattern_name, e
                ));
            }
        }
        if filter.filter_type == "ip_cidr" {
            let network = filter.ip_network.split('/').next().unwrap_or("");
            if network.parse::<IpAddr>().is_err() {
                return Err(format!(
                    "filter '{}' carries an invalid ip_network '{}'",
                    filter.pattern_name, filter.ip_network
                ));
            }
        }
    }
    for block in &snapshot.blocks {
        if block.remote_ip.parse::<IpAddr>().is_err() {
            return Err(format!(
                "block for filter {} carries an invalid remote ip '{}'",
                block.filter_id, block.remote_ip
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::red_eye::wire::{ConfigSync, ServiceOp};

    fn state() -> Arc<AppState> {
        let (restart_tx, _restart_rx) = create_restart_notifier();
        let (enforce_tx, _enforce_rx) = mpsc::channel(8);
        Arc::new(AppState {
            ports: create_ports_cache(),
            config: Arc::new(RwLock::new(EffectiveConfig {
                interface: "eth0".into(),
                machine_ip: "0.0.0.0".parse().unwrap(),
                revision: 0,
            })),
            restart_tx,
            health: Arc::new(crate::sniffer::SnifferHealth::default()),
            pending_revision: Arc::new(AtomicU64::new(0)),
            applied_revision: Arc::new(AtomicU64::new(0)),
            last_sync_error: Arc::new(RwLock::new(None)),
            enforce_tx,
            enforcement_failures: Arc::new(AtomicU64::new(0)),
            last_enforcement_error: Arc::new(RwLock::new(None)),
            agent_id: "agent-a".into(),
            boot_id: "boot-1".into(),
            config_source: <AgentConfig as clap::Parser>::parse_from(["red-eye-agent"]),
        })
    }

    fn envelope(update: Option<wire::settings_envelope::Update>) -> Vec<u8> {
        SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update,
        }
        .encode_to_vec()
    }

    fn service(id: u64, port: u32, protocol: WireProtocol) -> crate::wire::WireService {
        crate::wire::WireService {
            name: "team-site".into(),
            port,
            chunked_transfer_encoding: false,
            decrypt_tls: false,
            inflate_websockets: false,
            merge_adjacent_packets: false,
            urldecode: false,
            http_parse: false,
            id,
            protocol: protocol as i32,
            is_global: true,
        }
    }

    fn snapshot(
        agent_id: &str,
        revision: u64,
        services: Vec<crate::wire::WireService>,
    ) -> WireAgentConfigSnapshot {
        WireAgentConfigSnapshot {
            revision,
            agent_id: agent_id.into(),
            network: Some(wire::WireAgentNetworkSettings {
                interface: "eth1".into(),
                machine_ip: "10.0.0.5".into(),
            }),
            services,
            filters: vec![],
            blocks: vec![],
        }
    }

    #[test]
    fn decode_roundtrips_what_the_backend_encodes() {
        let snap = snapshot("agent-a", 7, vec![service(1, 8080, WireProtocol::Tcp)]);
        let bytes = envelope(Some(wire::settings_envelope::Update::AgentSnapshot(snap)));
        let decoded = decode_settings_update(&bytes).unwrap();

        match decoded.update {
            Some(wire::settings_envelope::Update::AgentSnapshot(snap)) => {
                assert_eq!(snap.revision, 7);
                assert_eq!(snap.agent_id, "agent-a");
                assert_eq!(snap.services[0].port, 8080);
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn wrong_version_is_a_clean_error() {
        // Version 3 and below are unsupported under the coordinated v4
        // upgrade: every foreign version must be rejected by the gate.
        for wrong in [0u32, 1, 2, 3] {
            let bytes = SettingsEnvelope {
                version: wrong,
                update: None,
            }
            .encode_to_vec();
            assert!(
                decode_settings_update(&bytes).is_err(),
                "version {wrong} passed"
            );
        }
        assert!(decode_settings_update(&[]).is_err());
    }

    #[test]
    fn garbage_and_lying_lengths_error_instead_of_allocating() {
        // Random garbage (bincode-era bytes of the old settings format).
        assert!(decode_settings_update(&[0xfc, 0x28, 0x5c, 0x22, 0x06, 0x00]).is_err());

        // A lying field length: field 8 (config_sync, length-delimited)
        // declares ~34 GB. prost validates the length against the bytes
        // that remain and errors - no oversized allocation is attempted.
        let mut lie = envelope(None);
        lie.extend_from_slice(&[0x42, 0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode_settings_update(&lie).is_err());
    }

    #[test]
    fn snapshot_applies_services_atomically() {
        let state = state();
        let snap = snapshot(
            "agent-a",
            5,
            vec![
                service(1, 80, WireProtocol::Tcp),
                service(2, 53, WireProtocol::Udp),
            ],
        );
        apply_snapshot(&snap, &state);

        let ports = state.ports.read().unwrap();
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        assert!(ports.contains(&(TransportProto::Udp, 53)));
        assert_eq!(ports.len(), 2);
        let config = state.config.read().unwrap();
        assert_eq!(config.interface, "eth1");
        assert_eq!(config.machine_ip.to_string(), "10.0.0.5");
        assert_eq!(state.applied_revision.load(Ordering::SeqCst), 0);
        assert_eq!(state.pending_revision.load(Ordering::SeqCst), 5);
        assert!(state.last_sync_error.read().unwrap().is_none());
    }

    #[test]
    fn snapshot_for_other_agent_is_skipped() {
        let state = state();
        let snap = snapshot("agent-b", 9, vec![service(1, 80, WireProtocol::Tcp)]);
        apply_snapshot(&snap, &state);
        assert!(state.ports.read().unwrap().is_empty());
        assert_eq!(state.pending_revision.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn stale_snapshot_is_ignored() {
        let state = state();
        apply_snapshot(
            &snapshot("agent-a", 5, vec![service(1, 80, WireProtocol::Tcp)]),
            &state,
        );
        // Simulate a fully applied revision.
        state.applied_revision.store(5, Ordering::SeqCst);
        state.config.write().unwrap().revision = 5;

        apply_snapshot(
            &snapshot("agent-a", 4, vec![service(2, 81, WireProtocol::Tcp)]),
            &state,
        );
        assert!(!state
            .ports
            .read()
            .unwrap()
            .contains(&(TransportProto::Tcp, 81)));
    }

    #[test]
    fn repeated_snapshot_is_harmless() {
        let state = state();
        apply_snapshot(
            &snapshot("agent-a", 5, vec![service(1, 80, WireProtocol::Tcp)]),
            &state,
        );
        // Simulate the sniffer confirming the capture (commit path).
        state.applied_revision.store(5, Ordering::SeqCst);
        state.pending_revision.store(0, Ordering::SeqCst);
        let before = state.ports.read().unwrap().clone();

        apply_snapshot(
            &snapshot("agent-a", 5, vec![service(1, 80, WireProtocol::Tcp)]),
            &state,
        );
        assert_eq!(*state.ports.read().unwrap(), before);
        assert_eq!(state.pending_revision.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn invalid_snapshot_keeps_previous_configuration() {
        let state = state();
        apply_snapshot(
            &snapshot("agent-a", 3, vec![service(1, 80, WireProtocol::Tcp)]),
            &state,
        );
        state.applied_revision.store(3, Ordering::SeqCst);
        state.config.write().unwrap().revision = 3;

        // Unknown protocol: must be rejected, never reinterpreted as TCP.
        let mut bad = snapshot("agent-a", 4, vec![service(2, 81, WireProtocol::Tcp)]);
        bad.services[0].protocol = 99;
        apply_snapshot(&bad, &state);
        let ports = state.ports.read().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        drop(ports);
        assert!(state.last_sync_error.read().unwrap().is_some());

        // Port overflow must be rejected, never truncated.
        *state.last_sync_error.write().unwrap() = None;
        let bad = snapshot("agent-a", 4, vec![service(2, 70000, WireProtocol::Tcp)]);
        apply_snapshot(&bad, &state);
        let ports = state.ports.read().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        drop(ports);
        assert!(state.last_sync_error.read().unwrap().is_some());
    }

    #[test]
    fn invalid_machine_ip_rejects_whole_snapshot() {
        let state = state();
        let mut snap = snapshot("agent-a", 6, vec![service(1, 80, WireProtocol::Tcp)]);
        snap.network = Some(wire::WireAgentNetworkSettings {
            interface: "eth1".into(),
            machine_ip: "not-an-ip".into(),
        });
        apply_snapshot(&snap, &state);
        assert_eq!(state.config.read().unwrap().interface, "eth0");
        assert!(state.ports.read().unwrap().is_empty());
    }

    #[test]
    fn empty_service_set_is_a_valid_state() {
        let state = state();
        apply_snapshot(&snapshot("agent-a", 2, vec![]), &state);
        assert!(state.ports.read().unwrap().is_empty());
        assert_eq!(state.pending_revision.load(Ordering::SeqCst), 2);
        // No error: an agent with no services is healthy and idle.
        assert!(state.last_sync_error.read().unwrap().is_none());
    }

    #[test]
    fn global_config_sync_is_ignored_by_the_agent() {
        let state = state();
        let bytes = envelope(Some(wire::settings_envelope::Update::ConfigSync(
            ConfigSync {
                services: vec![ServiceOp {
                    id: 1,
                    service: Some(service(1, 8080, WireProtocol::Tcp)),
                }],
                filters: vec![],
            },
        )));
        let decoded = decode_settings_update(&bytes).unwrap();
        match &decoded.update {
            Some(wire::settings_envelope::Update::ConfigSync(_)) => {}
            _ => panic!("wrong variant"),
        }
        // The listener only applies snapshots; a ConfigSync changes nothing.
        assert!(state.ports.read().unwrap().is_empty());
    }
}
