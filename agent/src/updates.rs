use std::collections::{HashMap, HashSet};
use std::net::{AddrParseError, IpAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use crate::proxy_state::ProxyDesired;
use crate::supervisor::ProxySupervisor;
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

/// Shared set of passive-capture (protocol, port) pairs: assigned services
/// minus effectively proxied keys. Written only by the supervisor's
/// recompute, read by the sniffer.
pub type PortsCache = Arc<RwLock<HashSet<(TransportProto, u16)>>>;

/// Keys withheld from passive capture (proxied or transitioning): the
/// sniffer counts frames matching them as capture-source conflicts.
pub type ExcludedPortsCache = Arc<RwLock<HashSet<(TransportProto, u16)>>>;

pub type RestartNotifier = tokio::sync::watch::Sender<()>;

pub fn create_ports_cache() -> PortsCache {
    Arc::new(RwLock::new(HashSet::new()))
}

pub fn create_restart_notifier() -> (RestartNotifier, tokio::sync::watch::Receiver<()>) {
    tokio::sync::watch::channel(())
}

/// One assigned service from the snapshot: the passive capture key plus
/// the identity the proxy desired state refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignedService {
    pub protocol: TransportProto,
    pub port: u16,
}

/// Effective agent configuration, swapped atomically when a snapshot is
/// applied. machine_ip == 0.0.0.0 / :: means wildcard: match services by
/// protocol+port and preserve observed endpoints in synthesis.
#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub interface: String,
    pub machine_ip: IpAddr,
}

pub type SharedConfig = Arc<RwLock<EffectiveConfig>>;

/// Revision acknowledgement for one snapshot: passive capture and the
/// proxy transitions must BOTH converge before the revision counts as
/// applied. A failed transition keeps the revision unacknowledged so the
/// backend keeps re-delivering while the agent retries.
pub struct RevisionAcker {
    /// Revision currently being applied (0 = none).
    pending: AtomicU64,
    passive_confirmed: AtomicBool,
    /// Set when a proxy transition failed for the pending revision: blocks
    /// the commit until a later transition succeeds.
    transition_failed: AtomicBool,
    /// Proxy transitions outstanding for the pending revision.
    outstanding: AtomicU64,
    pub applied_revision: Arc<AtomicU64>,
    pub last_sync_error: Arc<RwLock<Option<String>>>,
}

impl RevisionAcker {
    pub fn new(
        applied_revision: Arc<AtomicU64>,
        last_sync_error: Arc<RwLock<Option<String>>>,
    ) -> Self {
        Self {
            pending: AtomicU64::new(0),
            passive_confirmed: AtomicBool::new(false),
            transition_failed: AtomicBool::new(false),
            outstanding: AtomicU64::new(0),
            applied_revision,
            last_sync_error,
        }
    }

    /// Start applying a revision: `outstanding` = proxy transitions the
    /// supervisor must run, `passive_changed` = whether the sniffer must
    /// confirm a new port set / interface.
    pub fn begin(&self, revision: u64, outstanding: usize, passive_changed: bool) {
        self.pending.store(revision, Ordering::SeqCst);
        self.passive_confirmed
            .store(!passive_changed, Ordering::SeqCst);
        self.transition_failed.store(false, Ordering::SeqCst);
        self.outstanding.store(outstanding as u64, Ordering::SeqCst);
        self.maybe_commit();
    }

    /// The sniffer applied the current port set (or passive capture is
    /// intentionally idle).
    pub fn confirm_passive(&self) {
        self.passive_confirmed.store(true, Ordering::SeqCst);
        self.maybe_commit();
    }

    /// One proxy transition completed successfully. The accounting is
    /// approximate by design: the supervisor's converge loop is the
    /// source of truth, this only gates the commit.
    pub fn proxy_done(&self, _revision: u64) {
        self.transition_failed.store(false, Ordering::SeqCst);
        let prev = self.outstanding.load(Ordering::SeqCst);
        if prev > 0 {
            self.outstanding.store(prev - 1, Ordering::SeqCst);
        }
        self.maybe_commit();
    }

    /// One proxy transition failed. The revision stays unacknowledged
    /// (backend re-delivers; the supervisor retries) and the sanitized
    /// error becomes visible.
    pub fn proxy_failed(&self, _revision: u64, error: &str) {
        self.transition_failed.store(true, Ordering::SeqCst);
        let prev = self.outstanding.load(Ordering::SeqCst);
        if prev > 0 {
            self.outstanding.store(prev - 1, Ordering::SeqCst);
        }
        *self.last_sync_error.write().unwrap() = Some(sanitize_error(error));
    }

    fn maybe_commit(&self) {
        if self.pending.load(Ordering::SeqCst) == 0
            || !self.passive_confirmed.load(Ordering::SeqCst)
            || self.transition_failed.load(Ordering::SeqCst)
            || self.outstanding.load(Ordering::SeqCst) > 0
        {
            return;
        }
        let revision = self.pending.swap(0, Ordering::SeqCst);
        self.applied_revision.fetch_max(revision, Ordering::SeqCst);
        *self.last_sync_error.write().unwrap() = None;
        debug!("Revision {} fully applied", revision);
    }

    #[cfg(test)]
    pub fn pending_revision(&self) -> u64 {
        self.pending.load(Ordering::SeqCst)
    }
}

/// Bounded error strings for the heartbeat: no payload data ever enters
/// them (formatting is restricted to fixed strings and identifiers).
pub fn sanitize_error(msg: &str) -> String {
    let mut out: String = msg.chars().take(300).collect();
    if msg.len() > 300 {
        out.push_str("...");
    }
    out
}

/// Agent-wide state shared by the update listener, heartbeat, command
/// listener, supervisor and sniffer loop.
pub struct AppState {
    /// Passive capture keys (assigned minus proxied). Recomputed only by
    /// the supervisor.
    pub ports: PortsCache,
    /// Keys withheld from passive capture (conflict detector input).
    pub excluded_ports: ExcludedPortsCache,
    /// Bumped on every ports swap; the sniffer reports the generation it
    /// applied so transitions can confirm filter changes.
    pub ports_generation: Arc<AtomicU64>,
    pub config: SharedConfig,
    pub restart_tx: RestartNotifier,
    pub health: crate::sniffer::SharedSnifferHealth,
    /// Assigned services (id -> key) from the last valid snapshot.
    pub services: Arc<RwLock<HashMap<u64, AssignedService>>>,
    /// Snapshot application accounting (passive + proxy convergence).
    pub acker: Arc<RevisionAcker>,
    /// 0 = nothing acknowledged yet (kept for heartbeat compatibility).
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

/// Spawn the settings update listener as a background task. Listener
/// errors are logged and contained: the task exits quietly only if the
/// NATS subscription dies, never taking the rest of the agent with it.
pub fn spawn_updates_listener(
    nats: NatsClient,
    state: Arc<AppState>,
    supervisor: Arc<ProxySupervisor>,
) {
    let updates_topic = state.config_source.nats_updates_topic.clone();

    tokio::spawn(async move {
        if let Err(e) =
            listen_for_updates(nats.inner().clone(), &updates_topic, state, supervisor).await
        {
            error!("Update listener error: {}", e);
        }
    });
}

async fn listen_for_updates(
    client: async_nats::Client,
    subject: &str,
    state: Arc<AppState>,
    supervisor: Arc<ProxySupervisor>,
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
                apply_snapshot(snapshot, &state, &supervisor);
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

/// Parsed, validated desired proxy state from a snapshot entry.
fn parse_desired_proxy(rule: &wire::WireProxyRule) -> Result<ProxyDesired, String> {
    let protocol = TransportProto::from_wire(rule.protocol).ok_or_else(|| {
        format!(
            "proxy for service {} carries an unknown protocol",
            rule.service_id
        )
    })?;
    if rule.service_id == 0 {
        return Err("proxy rule carries service id 0".to_string());
    }
    if rule.name.is_empty() {
        return Err("proxy rule name is empty".to_string());
    }
    let listen_port = u16::try_from(rule.listen_port)
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| format!("proxy listen port {} out of range", rule.listen_port))?;
    let original_port = u16::try_from(rule.target_port)
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| format!("proxy original port {} out of range", rule.target_port))?;
    let listen_ip: IpAddr = rule.listen_address.parse().map_err(|e: AddrParseError| {
        format!("proxy listen address '{}': {}", rule.listen_address, e)
    })?;
    let target_ip: IpAddr = rule.target_address.parse().map_err(|e: AddrParseError| {
        format!("proxy target address '{}': {}", rule.target_address, e)
    })?;
    let tls_mode = match rule.tls_mode.as_str() {
        "" => None,
        m @ ("mitm" | "terminate" | "passthrough") => Some(m.to_string()),
        other => return Err(format!("proxy rule carries unknown tls mode '{}'", other)),
    };

    Ok(ProxyDesired {
        service_id: rule.service_id,
        name: rule.name.clone(),
        listen: std::net::SocketAddr::new(listen_ip, listen_port),
        target: std::net::SocketAddr::new(target_ip, original_port),
        protocol,
        original_port,
        tls_mode,
        http2: rule.http2_enabled,
        revision: rule.revision,
    })
}

/// Validate and atomically apply a snapshot addressed to this agent.
/// Invalid snapshots leave the previous configuration untouched; stale
/// ones are ignored. Returns nothing: outcomes are surfaced via heartbeat
/// fields (applied revision + sanitized error). pub(crate) so the
/// supervisor state-machine tests drive the full snapshot path.
pub(crate) fn apply_snapshot(
    snapshot: &WireAgentConfigSnapshot,
    state: &AppState,
    supervisor: &Arc<ProxySupervisor>,
) {
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

    let mut new_services: HashMap<u64, AssignedService> = HashMap::new();
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
        new_services.insert(service.id, AssignedService { protocol, port });
    }

    let mut new_desired: HashMap<u64, ProxyDesired> = HashMap::new();
    for rule in &snapshot.desired_proxies {
        // Validation already ran, but never trust a second pass.
        match parse_desired_proxy(rule) {
            Ok(desired) => {
                new_desired.insert(desired.service_id, desired);
            }
            Err(e) => warn!(
                "Skipping undesired proxy rule in revision {}: {}",
                snapshot.revision, e
            ),
        }
    }

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

    let old_ports: HashSet<(TransportProto, u16)> = state.ports.read().unwrap().clone();

    let interface_changed = {
        let mut config = state.config.write().unwrap();
        let changed = config.interface != interface;
        config.interface = interface;
        config.machine_ip = machine_ip;
        changed
    };

    // 1. New service set (recompute and assignment filtering read it).
    *state.services.write().unwrap() = new_services;

    // 2. Publish desired proxies to the supervisor and count the
    //    transitions needed BEFORE the acker begins accounting, so no
    //    completion can slip in uncounted.
    let diff_count = supervisor.set_desired(new_desired, snapshot.revision);

    // 3. Recompute passive keys: assigned services minus proxied or
    //    transitioning originals. The exclusion set is read fresh from the
    //    supervisor ledger, so a proxied original can never re-enter
    //    passive capture - not even while a transition is in flight.
    let excluded = supervisor.excluded_snapshot();
    let (new_ports, _generation) = crate::supervisor::recompute_passive_ports(state, &excluded);
    let capture_changed = old_ports != new_ports || interface_changed;

    // 4. Account the revision, then wake the supervisor to run transitions.
    state
        .acker
        .begin(snapshot.revision, diff_count, capture_changed);

    info!(
        "Applying snapshot revision {} (services={}, desired proxies={}, transitions pending={}, capture changed={})",
        snapshot.revision,
        state.services.read().unwrap().len(),
        snapshot.desired_proxies.len(),
        diff_count,
        capture_changed
    );

    supervisor.wake();
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
    let mut seen_services = HashSet::new();
    for rule in &snapshot.desired_proxies {
        if !seen_services.insert(rule.service_id) {
            return Err(format!(
                "snapshot carries two desired proxies for service {}",
                rule.service_id
            ));
        }
        parse_desired_proxy(rule)?;
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

/// Shared AppState constructor for cross-module state-machine tests
/// (passive ports start empty, like a fresh boot).
#[cfg(test)]
pub(crate) fn test_state() -> Arc<AppState> {
    let (restart_tx, _restart_rx) = create_restart_notifier();
    let applied = Arc::new(AtomicU64::new(0));
    let last_error: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
    Arc::new(AppState {
        ports: create_ports_cache(),
        excluded_ports: Arc::new(RwLock::new(HashSet::new())),
        ports_generation: Arc::new(AtomicU64::new(0)),
        config: Arc::new(RwLock::new(EffectiveConfig {
            interface: "eth0".into(),
            machine_ip: "0.0.0.0".parse().unwrap(),
        })),
        restart_tx,
        health: Arc::new(crate::sniffer::SnifferHealth::default()),
        services: Arc::new(RwLock::new(HashMap::new())),
        acker: Arc::new(RevisionAcker::new(applied.clone(), last_error.clone())),
        applied_revision: applied,
        last_sync_error: last_error,
        enforce_tx: mpsc::channel(8).0,
        enforcement_failures: Arc::new(AtomicU64::new(0)),
        last_enforcement_error: Arc::new(RwLock::new(None)),
        agent_id: "agent-a".into(),
        boot_id: "boot-1".into(),
        config_source: <AgentConfig as clap::Parser>::parse_from(["red-eye-agent"]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::red_eye::wire::{ConfigSync, ServiceOp};

    fn state() -> Arc<AppState> {
        let (restart_tx, _restart_rx) = create_restart_notifier();
        let applied = Arc::new(AtomicU64::new(0));
        let last_error: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
        let (enforce_tx, _enforce_rx) = mpsc::channel(8);
        Arc::new(AppState {
            ports: create_ports_cache(),
            excluded_ports: Arc::new(RwLock::new(HashSet::new())),
            ports_generation: Arc::new(AtomicU64::new(0)),
            config: Arc::new(RwLock::new(EffectiveConfig {
                interface: "eth0".into(),
                machine_ip: "0.0.0.0".parse().unwrap(),
            })),
            restart_tx,
            health: Arc::new(crate::sniffer::SnifferHealth::default()),
            services: Arc::new(RwLock::new(HashMap::new())),
            acker: Arc::new(RevisionAcker::new(applied.clone(), last_error.clone())),
            applied_revision: applied,
            last_sync_error: last_error,
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
        snapshot_with_proxies(agent_id, revision, services, vec![])
    }

    fn snapshot_with_proxies(
        agent_id: &str,
        revision: u64,
        services: Vec<crate::wire::WireService>,
        desired_proxies: Vec<crate::wire::WireProxyRule>,
    ) -> WireAgentConfigSnapshot {
        WireAgentConfigSnapshot {
            revision,
            agent_id: agent_id.into(),
            network: Some(wire::WireAgentNetworkSettings {
                interface: "eth1".into(),
                machine_ip: "10.0.0.5".into(),
            }),
            services,
            desired_proxies,
            filters: vec![],
            blocks: vec![],
        }
    }

    fn proxy_rule(
        service_id: u64,
        name: &str,
        listen_port: u32,
        original_port: u32,
    ) -> crate::wire::WireProxyRule {
        crate::wire::WireProxyRule {
            service_id,
            name: name.into(),
            listen_address: "0.0.0.0".into(),
            listen_port,
            target_address: "127.0.0.1".into(),
            target_port: original_port,
            protocol: WireProtocol::Tcp as i32,
            tls_mode: String::new(),
            http2_enabled: false,
            revision: 0,
        }
    }

    #[test]
    fn decode_roundtrips_what_the_backend_encodes() {
        let snap = snapshot_with_proxies(
            "agent-a",
            7,
            vec![service(1, 8080, WireProtocol::Tcp)],
            vec![proxy_rule(1, "protect-1-x", 18080, 8080)],
        );
        let bytes = envelope(Some(wire::settings_envelope::Update::AgentSnapshot(snap)));
        let decoded = decode_settings_update(&bytes).unwrap();

        match decoded.update {
            Some(wire::settings_envelope::Update::AgentSnapshot(snap)) => {
                assert_eq!(snap.revision, 7);
                assert_eq!(snap.agent_id, "agent-a");
                assert_eq!(snap.services[0].port, 8080);
                assert_eq!(snap.desired_proxies[0].listen_port, 18080);
                assert_eq!(snap.desired_proxies[0].target_port, 8080);
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
    fn snapshot_for_other_agent_is_skipped() {
        let state = state();
        let snap = snapshot("agent-b", 9, vec![service(1, 80, WireProtocol::Tcp)]);
        assert_eq!(snap.agent_id, "agent-b");
        // Addressed to another agent: nothing applied.
        assert!(validate_snapshot(&snap).is_ok());
        assert!(state.ports.read().unwrap().is_empty());
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
        apply_snapshot(&snap, &state, &never_supervisor());

        let ports = state.ports.read().unwrap();
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        assert!(ports.contains(&(TransportProto::Udp, 53)));
        assert_eq!(ports.len(), 2);
        drop(ports);
        let config = state.config.read().unwrap();
        assert_eq!(config.interface, "eth1");
        assert_eq!(config.machine_ip.to_string(), "10.0.0.5");
        drop(config);
        assert!(state.last_sync_error.read().unwrap().is_none());
        // The revision stays pending until capture converges.
        assert_eq!(state.acker.pending_revision(), 5);
    }

    #[test]
    fn invalid_snapshot_keeps_previous_configuration() {
        let state = state();
        apply_snapshot(
            &snapshot("agent-a", 3, vec![service(1, 80, WireProtocol::Tcp)]),
            &state,
            &never_supervisor(),
        );
        state.applied_revision.store(3, Ordering::SeqCst);

        // Unknown protocol: must be rejected, never reinterpreted as TCP.
        let mut bad = snapshot("agent-a", 4, vec![service(2, 81, WireProtocol::Tcp)]);
        bad.services[0].protocol = 99;
        apply_snapshot(&bad, &state, &never_supervisor());
        let ports = state.ports.read().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        drop(ports);
        assert!(state.last_sync_error.read().unwrap().is_some());

        // Port overflow must be rejected, never truncated.
        *state.last_sync_error.write().unwrap() = None;
        let bad = snapshot("agent-a", 4, vec![service(2, 70000, WireProtocol::Tcp)]);
        apply_snapshot(&bad, &state, &never_supervisor());
        let ports = state.ports.read().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains(&(TransportProto::Tcp, 80)));
        drop(ports);
        assert!(state.last_sync_error.read().unwrap().is_some());
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
    fn invalid_proxy_fields_reject_whole_snapshot() {
        // Duplicate desired proxies for one service.
        let mut snap = snapshot_with_proxies(
            "agent-a",
            4,
            vec![service(1, 80, WireProtocol::Tcp)],
            vec![proxy_rule(1, "a", 10080, 80), proxy_rule(1, "b", 10081, 80)],
        );
        assert!(validate_snapshot(&snap).is_err());

        snap.desired_proxies = vec![proxy_rule(1, "a", 70000, 80)];
        assert!(validate_snapshot(&snap).is_err());

        snap.desired_proxies = vec![proxy_rule(1, "a", 10080, 0)];
        assert!(validate_snapshot(&snap).is_err());

        snap.desired_proxies = vec![proxy_rule(1, "a", 10080, 80)];
        snap.desired_proxies[0].tls_mode = "bogus".into();
        assert!(validate_snapshot(&snap).is_err());

        snap.desired_proxies[0].listen_address = "not-an-ip".into();
        assert!(validate_snapshot(&snap).is_err());
    }

    #[test]
    fn stale_snapshot_is_ignored() {
        let state = state();
        let snap = snapshot("agent-a", 5, vec![service(1, 80, WireProtocol::Tcp)]);
        state.applied_revision.store(5, Ordering::SeqCst);
        // Stale revision gate is checked against applied_revision.
        assert!(snap.revision <= state.applied_revision.load(Ordering::SeqCst));
        apply_snapshot(&snap, &state, &never_supervisor());
        assert!(state.ports.read().unwrap().is_empty());
    }

    // The update listener tests drive apply_snapshot through the real
    // supervisor (in-memory, no engine): see supervisor::tests for the
    // state-machine coverage. `never_supervisor` provides a supervisor
    // whose ledger starts empty for the rejection-path tests above.
    fn never_supervisor() -> Arc<ProxySupervisor> {
        crate::supervisor::ProxySupervisor::for_tests()
    }

    #[test]
    fn repeated_snapshot_is_harmless() {
        let state = state();
        let supervisor = never_supervisor();
        let snap = snapshot("agent-a", 5, vec![service(1, 80, WireProtocol::Tcp)]);
        apply_snapshot(&snap, &state, &supervisor);
        let before = state.ports.read().unwrap().clone();

        // Same revision after full application: ignored as stale.
        state.applied_revision.store(5, Ordering::SeqCst);
        apply_snapshot(&snap, &state, &supervisor);
        assert_eq!(*state.ports.read().unwrap(), before);
    }

    #[test]
    fn invalid_machine_ip_rejects_whole_snapshot() {
        let state = state();
        let mut snap = snapshot("agent-a", 6, vec![service(1, 80, WireProtocol::Tcp)]);
        snap.network = Some(wire::WireAgentNetworkSettings {
            interface: "eth1".into(),
            machine_ip: "not-an-ip".into(),
        });
        apply_snapshot(&snap, &state, &never_supervisor());
        assert_eq!(state.config.read().unwrap().interface, "eth0");
        assert!(state.ports.read().unwrap().is_empty());
    }

    #[test]
    fn empty_service_set_is_a_valid_state() {
        let state = state();
        let supervisor = never_supervisor();
        apply_snapshot(&snapshot("agent-a", 2, vec![]), &state, &supervisor);
        assert!(state.ports.read().unwrap().is_empty());
        assert_eq!(state.acker.pending_revision(), 2);
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

    #[test]
    fn acker_commits_only_when_everything_converges() {
        let applied = Arc::new(AtomicU64::new(0));
        let errors: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
        let acker = RevisionAcker::new(applied.clone(), errors.clone());

        // Passive-only change commits immediately.
        acker.begin(5, 0, true);
        acker.confirm_passive();
        assert_eq!(applied.load(Ordering::SeqCst), 5);

        // With a pending transition, commit waits.
        acker.begin(6, 1, false);
        assert_eq!(applied.load(Ordering::SeqCst), 5);
        acker.proxy_done(6);
        assert_eq!(applied.load(Ordering::SeqCst), 6);

        // A failed transition blocks the commit and surfaces the error.
        acker.begin(7, 1, true);
        acker.confirm_passive();
        acker.proxy_failed(7, "nft failed");
        assert_eq!(applied.load(Ordering::SeqCst), 6);
        assert!(errors.read().unwrap().is_some());

        // A later successful transition clears the failure and commits.
        acker.outstanding.store(1, Ordering::SeqCst);
        acker.proxy_done(7);
        assert_eq!(applied.load(Ordering::SeqCst), 7);
        assert!(errors.read().unwrap().is_none());
    }
}
