use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use log::{debug, info, warn};

use harpoon_core::capture::CaptureManager;
use harpoon_core::engine::filter::{CompiledFilter, SharedFilterSet};
use harpoon_core::engine::{run_with_capture, EngineHandle};
use harpoon_core::types::endpoint::{Endpoint, Protocol};
use harpoon_core::types::filter::Filter;
#[cfg(test)]
use harpoon_core::types::filter::{Direction, FilterAction, FilterKind};
use harpoon_core::types::rule::{DuplicateTarget, Rule, TlsConfig, TlsMode, UdpSourceMode};
use harpoon_core::CoreConfig;

use crate::nftables::{NftablesManager, RedirectRule};
use crate::packet_synthesis;
use crate::tls_certs::TlsCerts;
use crate::updates::TransportProto;

/// Maximum capture session duration (seconds). Effectively infinite.
const CAPTURE_TIMEOUT_SECS: u64 = 60 * 60 * 24 * 365;
/// Ring buffer size per rule. We consume via broadcast, so this is just a safety buffer.
const CAPTURE_MAX_PACKETS: usize = 1000;
/// Max payload bytes to capture per packet.
const CAPTURE_MAX_PAYLOAD: usize = 65535;

/// Proxy rule definition (built from desired state or legacy commands).
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
    /// If set, duplicate all traffic to this endpoint (e.g. another team)
    pub duplicate_to: Option<SocketAddr>,
    /// Enable HTTP/2 proxy mode
    pub http2: bool,
}

impl ProxyRuleSpec {
    /// Convert to a harpoon-core Rule.
    ///
    /// The rule carries NO filters of its own: every engine is started
    /// with a [`SharedFilterSet`] that all rules evaluate traffic against,
    /// so filters can be hot-swapped without touching any rule engine.
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

        let duplicate = self.duplicate_to.map(|addr| DuplicateTarget {
            endpoint: endpoint(addr),
        });

        Rule {
            name: self.name.clone(),
            listen: endpoint(self.listen),
            target: endpoint(self.target),
            filters: Vec::new(),
            duplicate,
            exporter: None,
            tls,
            udp_source_mode: UdpSourceMode::Proxy,
            idle_timeout_secs: Rule::default_idle_timeout(),
            http2: self.http2,
        }
    }

    pub fn listen_port(&self) -> u16 {
        self.listen.port()
    }

    pub fn transport(&self) -> TransportProto {
        match self.protocol {
            Protocol::Tcp => TransportProto::Tcp,
            Protocol::Udp => TransportProto::Udp,
        }
    }
}

/// One running proxy rule: its spec plus its own engine instance
pub struct RuleRuntime {
    pub spec: ProxyRuleSpec,
    pub handle: EngineHandle,
}

impl RuleRuntime {
    /// Stop accepting, drain existing sessions within a bounded interval
    /// (bounded by the engine's own drain + join timeouts), then fully
    /// stop. After this returns, the rule cannot capture or publish.
    pub async fn shutdown(self) {
        self.handle.stop();
        let _ = self.handle.shutdown().await;
    }
}

/// Counters for the single capture forwarder (lag events on the broadcast consumer), reported via heartbeat.
#[derive(Default)]
pub struct ForwarderCounters {
    pub lag_events: AtomicU64,
}

pub struct HarpoonEngine {
    /// Protect rules keyed by service id (desired-state path).
    service_rules: HashMap<u64, RuleRuntime>,
    /// Manual/replay rules keyed by rule name
    adhoc_rules: HashMap<String, RuleRuntime>,
    /// Proxy rule name -> transport protocol, read by the capture
    /// forwarder to synthesize the matching header
    rule_protocols: Arc<std::sync::RwLock<HashMap<String, TransportProto>>>,
    capture_mgr: Arc<CaptureManager>,
    /// Last synced filters (parsed form; kept for `filter_count`/status).
    filters: Vec<Filter>,
    /// Live filter set shared with all running proxy pipelines.
    ///
    /// `sync_filters` atomically swaps it (`store`); running proxies pick up
    /// the new set for traffic evaluated after the swap - no engine restart,
    /// no listener churn, no connection drops.
    shared_filters: SharedFilterSet,
    tls_certs: Option<TlsCerts>,
    nft: NftablesManager,
}

impl HarpoonEngine {
    pub fn new(tls_certs: Option<TlsCerts>) -> Self {
        Self {
            service_rules: HashMap::new(),
            adhoc_rules: HashMap::new(),
            rule_protocols: Arc::new(std::sync::RwLock::new(HashMap::new())),
            capture_mgr: CaptureManager::new(),
            filters: Vec::new(),
            shared_filters: SharedFilterSet::empty(),
            tls_certs,
            nft: NftablesManager::new(),
        }
    }

    fn sync_rule_protocols(&self) {
        let mut map = self.rule_protocols.write().unwrap();
        map.clear();
        for runtime in self.service_rules.values() {
            map.insert(runtime.spec.name.clone(), runtime.spec.transport());
        }
        for runtime in self.adhoc_rules.values() {
            map.insert(runtime.spec.name.clone(), runtime.spec.transport());
        }
    }

    /// Protocol map for the capture forwarder.
    pub fn rule_protocols(&self) -> Arc<std::sync::RwLock<HashMap<String, TransportProto>>> {
        self.rule_protocols.clone()
    }

    /// Get the shared capture manager (for spawning the forwarder).
    pub fn capture_manager(&self) -> Arc<CaptureManager> {
        self.capture_mgr.clone()
    }

    /// Spawn a dedicated engine instance for one rule WITHOUT registering
    /// it: the caller decides (via `register_service_runtime`) when the
    /// rule becomes authoritative, so a crash mid-transition rolls back
    /// cleanly.
    pub async fn spawn_engine_for_spec(&mut self, spec: &ProxyRuleSpec) -> Result<EngineHandle> {
        let rule = spec.to_harpoon_rule(&self.tls_certs);
        let config = CoreConfig {
            rules: vec![rule],
            pipelines: vec![],
            shared_filter_set: Some(self.shared_filters.clone()),
            ..Default::default()
        };
        run_with_capture(config, self.capture_mgr.clone())
            .await
            .context(format!(
                "Failed to start harpoon engine for rule '{}'",
                spec.name
            ))
    }

    /// Start the capture session (the publisher side) for a rule. Must be
    /// called before redirected traffic can reach the rule, otherwise
    /// captures would be silently dropped (record needs a session).
    pub fn start_capture_session(&self, rule_name: &str) -> Result<()> {
        self.capture_mgr
            .start(
                rule_name.to_string(),
                CAPTURE_MAX_PACKETS,
                CAPTURE_MAX_PAYLOAD,
                CAPTURE_TIMEOUT_SECS,
            )
            .map_err(anyhow::Error::msg)
    }

    /// Stop the capture session and discard leftover ring-buffer packets:
    /// every captured packet was already broadcast (and forwarded) at
    /// record time, so the buffer must not be replayed after the rule is
    /// retired.
    pub fn stop_capture_session(&self, rule_name: &str) {
        if let Ok(discarded) = self.capture_mgr.stop(rule_name) {
            if !discarded.is_empty() {
                debug!(
                    "Discarded {} already-forwarded capture packets for '{}'",
                    discarded.len(),
                    rule_name
                );
            }
        }
    }

    /// Register a prepared runtime as the authoritative rule for a
    /// service, replacing any previous one.
    pub fn register_service_runtime(&mut self, service_id: u64, runtime: RuleRuntime) {
        info!(
            "Proxy rule '{}' active for service {} ({} {} -> {})",
            runtime.spec.name,
            service_id,
            runtime.spec.transport().as_str(),
            runtime
                .spec
                .nft_redirect_port
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".into()),
            runtime.spec.target,
        );
        self.service_rules.insert(service_id, runtime);
        self.sync_rule_protocols();
    }

    /// Take the service rule out (for teardown or recovery). The caller
    /// owns shutdown afterwards.
    pub fn take_service_runtime(&mut self, service_id: u64) -> Option<RuleRuntime> {
        let runtime = self.service_rules.remove(&service_id);
        if runtime.is_some() {
            self.sync_rule_protocols();
        }
        runtime
    }

    /// The spec of the current service rule, if any (state-machine tests).
    #[cfg(test)]
    pub fn service_spec(&self, service_id: u64) -> Option<ProxyRuleSpec> {
        self.service_rules.get(&service_id).map(|r| r.spec.clone())
    }

    /// Install (or update) the nftables redirect for (protocol, original
    /// port) -> listen port.
    pub fn install_redirect(&mut self, spec: &ProxyRuleSpec) -> Result<()> {
        let Some(original_port) = spec.nft_redirect_port else {
            return Ok(());
        };
        self.nft.add_redirect(RedirectRule {
            protocol: spec.transport().as_str().to_string(),
            original_port,
            redirect_port: spec.listen_port(),
        })
    }

    /// Remove the nftables redirect for (protocol, original port). The
    /// pair is the rule identity: a TCP redirect never removes a UDP one
    /// on the same numeric port. Errors propagate: disable must abort
    /// while a redirect could still blackhole service traffic.
    pub fn remove_redirect(&mut self, transport: TransportProto, original_port: u16) -> Result<()> {
        self.nft.remove_redirect(transport.as_str(), original_port)
    }

    /// Add an ad-hoc rule with its own engine instance. No engine restart:
    /// existing rules keep running.
    pub async fn add_adhoc_rule(&mut self, spec: ProxyRuleSpec) -> Result<()> {
        info!(
            "Adding proxy rule '{}': {} -> {}{}",
            spec.name,
            spec.listen,
            spec.target,
            spec.tls_mode
                .as_deref()
                .map(|m| format!(" (TLS: {})", m))
                .unwrap_or_default(),
        );
        self.install_redirect(&spec)?;
        let handle = self.spawn_engine_for_spec(&spec).await?;
        let name = spec.name.clone();
        self.adhoc_rules
            .insert(name.clone(), RuleRuntime { spec, handle });
        self.sync_rule_protocols();
        let _ = self.start_capture_session(&name);
        Ok(())
    }

    /// Update an ad-hoc rule: retire the old instance fully, then start
    /// the new one (never two producers for one listener).
    pub async fn update_adhoc_rule(&mut self, spec: ProxyRuleSpec) -> Result<()> {
        if let Some(old) = self.adhoc_rules.remove(&spec.name) {
            let transport = old.spec.transport();
            if let Some(port) = old.spec.nft_redirect_port {
                let _ = self.remove_redirect(transport, port);
            }
            self.stop_capture_session(&old.spec.name);
            old.shutdown().await;
        }
        self.add_adhoc_rule(spec).await
    }

    /// Remove an ad-hoc rule by name.
    pub async fn remove_adhoc_rule(&mut self, name: &str) -> Result<()> {
        let runtime = self
            .adhoc_rules
            .remove(name)
            .context(format!("Rule '{}' not found", name))?;
        info!("Removing proxy rule '{}'", name);
        let transport = runtime.spec.transport();
        if let Some(port) = runtime.spec.nft_redirect_port {
            let _ = self.remove_redirect(transport, port);
        }
        self.stop_capture_session(name);
        runtime.shutdown().await;
        self.sync_rule_protocols();
        Ok(())
    }

    /// Sync global filters (applied to all proxy rules) by hot-swapping the
    /// shared filter set.
    ///
    /// Does NOT restart any engine: running proxies keep serving, listeners
    /// stay bound, established connections keep flowing. Traffic evaluated
    /// after the swap is checked against the new filter set - per chunk, so
    /// even pre-existing connections are filtered going forward (a
    /// connection sending newly-blocked content is dropped/reset). There is
    /// never a window where no filters apply: the swap is atomic, and a
    /// chunk mid-evaluation during the swap completes under the old set.
    ///
    /// Filters that fail to compile (e.g. invalid regex) are skipped with a
    /// warning - same behavior as the engine's own static filter path.
    pub async fn sync_filters(&mut self, filters: Vec<Filter>) -> Result<()> {
        info!(
            "Syncing {} firewall filters (hot-swap, no engine restart)",
            filters.len()
        );

        let mut compiled = Vec::with_capacity(filters.len());
        for f in &filters {
            match CompiledFilter::new(f.clone()) {
                Ok(cf) => compiled.push(cf),
                Err(e) => warn!("Skipping filter that failed to compile: {}", e),
            }
        }

        // Swap first, then update the bookkeeping copy: the live set is the
        // effective one and must never lag behind the accepted filter list.
        self.shared_filters.store(compiled);
        self.filters = filters;

        Ok(())
    }

    /// Current filter count (for status).
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }
    /// Apply a set of manual nftables redirect rules (replaces all current manual rules).
    pub fn apply_nft_rules(&mut self, rules: Vec<RedirectRule>) -> Result<()> {
        self.nft.apply_rules(rules)
    }

    /// Ban an IP via nftables.
    pub fn ban_ip(&mut self, ip: &str) -> Result<()> {
        self.nft.ban_ip(ip)
    }

    /// Unban an IP.
    pub fn unban_ip(&mut self, ip: &str) -> Result<()> {
        self.nft.unban_ip(ip)
    }

    /// List banned IPs.
    pub fn banned_ips(&self) -> Vec<String> {
        self.nft.banned_ips().to_vec()
    }

    /// Clear all nftables rules.
    pub fn clear_nft_rules(&mut self) -> Result<()> {
        self.nft.clear()
    }

    /// Names of currently active proxy rules.
    pub fn active_rule_names(&self) -> Vec<String> {
        self.service_rules
            .values()
            .chain(self.adhoc_rules.values())
            .map(|r| r.spec.name.clone())
            .collect()
    }

    /// Current nftables rules (for status reporting).
    pub fn nft_rules(&self) -> Vec<RedirectRule> {
        self.nft.active_rules().to_vec()
    }

    /// Stop all rule engines, clean up nftables.
    pub async fn shutdown(&mut self) {
        let mut runtimes: Vec<RuleRuntime> = self.service_rules.drain().map(|(_, r)| r).collect();
        runtimes.extend(self.adhoc_rules.drain().map(|(_, r)| r));
        let count = runtimes.len();
        for runtime in runtimes {
            self.stop_capture_session(&runtime.spec.name);
            runtime.shutdown().await;
        }
        if count > 0 {
            info!("Stopped {} proxy rule engines", count);
        }
        let _ = self.nft.clear();
    }
}

/// Forwarder counters shared between the forwarder task and the heartbeat.
pub fn new_forwarder_counters() -> Arc<ForwarderCounters> {
    Arc::new(ForwarderCounters::default())
}

/// Spawn the single background task that forwards captured packets from
/// ALL harpoon rules to NATS. There is exactly one forwarder per agent:
/// rule updates never create additional forwarder subscriptions. The
/// machine IP and rule protocol are read per packet from the shared
/// state, so snapshot application takes effect without a restart.
pub fn spawn_capture_forwarder(
    capture_mgr: Arc<CaptureManager>,
    nats: crate::nats::NatsClient,
    rule_protocols: Arc<std::sync::RwLock<HashMap<String, TransportProto>>>,
    config: crate::updates::SharedConfig,
    counters: Arc<ForwarderCounters>,
) {
    tokio::spawn(async move {
        let mut rx = capture_mgr.subscribe();
        info!("Harpoon capture forwarder started");

        loop {
            match rx.recv().await {
                Ok(packet) => {
                    let protocol = rule_protocols
                        .read()
                        .unwrap()
                        .get(&packet.rule_name)
                        .copied()
                        .unwrap_or(TransportProto::Tcp);
                    let machine_ip = config.read().unwrap().machine_ip;
                    if let Some(encoded) =
                        packet_synthesis::encode_for_nats(&packet, machine_ip, protocol)
                    {
                        nats.send_packet(encoded);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    let missed = counters.lag_events.fetch_add(n, Ordering::Relaxed) + n;
                    if missed / 1000 != (missed - n) / 1000 {
                        warn!("Capture forwarder lagged, total missed so far: {}", missed);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    debug!("Capture broadcast channel closed");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn test_engine() -> HarpoonEngine {
        HarpoonEngine::new(None)
    }

    async fn spawn_echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        if stream.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    async fn free_port_addr() -> SocketAddr {
        let tmp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tmp.local_addr().unwrap();
        drop(tmp);
        addr
    }

    fn tcp_spec(name: &str, listen: SocketAddr, target: SocketAddr) -> ProxyRuleSpec {
        ProxyRuleSpec {
            name: name.into(),
            listen,
            target,
            protocol: Protocol::Tcp,
            tls_mode: None,
            nft_redirect_port: None,
            duplicate_to: None,
            http2: false,
        }
    }

    /// Live proof that adding a SECOND rule does not disturb the first
    /// one's proxied connection: each rule runs its own engine instance,
    /// so rule updates never restart unrelated rules.
    #[tokio::test]
    async fn adding_a_rule_does_not_restart_existing_ones() {
        let echo_addr = spawn_echo_server().await;
        let proxy_a = free_port_addr().await;
        let proxy_b = free_port_addr().await;

        let mut engine = test_engine();
        engine
            .add_adhoc_rule(tcp_spec("rule-a", proxy_a, echo_addr))
            .await
            .unwrap();

        // Connection A established through rule A.
        let mut a = None;
        for _ in 0..100 {
            match TcpStream::connect(proxy_a).await {
                Ok(s) => {
                    a = Some(s);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        let mut a = a.expect("proxy listener never came up");
        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf))
            .await
            .expect("echo timeout")
            .unwrap();
        assert_eq!(&buf[..n], b"hello");

        // Add rule B: rule A's connection must survive untouched.
        engine
            .add_adhoc_rule(tcp_spec("rule-b", proxy_b, echo_addr))
            .await
            .unwrap();
        a.write_all(b"still-here").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf))
            .await
            .expect("conn A died: rule B disturbed rule A's engine")
            .unwrap();
        assert_eq!(&buf[..n], b"still-here");

        // Removing rule B leaves rule A working.
        engine.remove_adhoc_rule("rule-b").await.unwrap();
        a.write_all(b"after-b-gone").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf))
            .await
            .expect("conn A died while removing rule B")
            .unwrap();
        assert_eq!(&buf[..n], b"after-b-gone");

        engine.shutdown().await;
    }

    /// Same-service enable idempotency at the engine level: registering a
    /// runtime for a service replaces (never duplicates) the previous one.
    #[tokio::test]
    async fn service_runtime_registration_replaces_previous() {
        let echo_addr = spawn_echo_server().await;
        let proxy = free_port_addr().await;
        let mut engine = test_engine();

        let handle = engine
            .spawn_engine_for_spec(&tcp_spec("r1", proxy, echo_addr))
            .await
            .unwrap();
        engine.register_service_runtime(
            42,
            RuleRuntime {
                spec: tcp_spec("r1", proxy, echo_addr),
                handle,
            },
        );
        assert_eq!(engine.active_rule_names(), vec!["r1".to_string()]);

        // A second registration must retire the old runtime first, or the
        // listen port bind of the replacement would fail.
        let old = engine
            .take_service_runtime(42)
            .expect("old runtime present");
        old.shutdown().await;
        let handle = engine
            .spawn_engine_for_spec(&tcp_spec("r1", proxy, echo_addr))
            .await
            .unwrap();
        engine.register_service_runtime(
            42,
            RuleRuntime {
                spec: tcp_spec("r1", proxy, echo_addr),
                handle,
            },
        );
        assert_eq!(engine.active_rule_names(), vec!["r1".to_string()]);
        assert!(engine.service_spec(42).is_some());
        assert!(engine.service_spec(43).is_none());

        // Teardown: after taking and stopping, the listen port frees up.
        let old = engine.take_service_runtime(42).unwrap();
        old.shutdown().await;
        let probe = tokio::time::timeout(Duration::from_secs(1), async {
            TcpListener::bind(proxy).await
        })
        .await
        .expect("probe timeout")
        .expect("listen port must be free after shutdown");
        drop(probe);
        engine.shutdown().await;
    }

    /// Pure swap-path logic: bookkeeping + live set updated, no engine
    /// spawned when there are no rules, filters survive across rule
    /// churn.
    #[tokio::test]
    async fn sync_filters_updates_bookkeeping_and_live_set() {
        let mut engine = test_engine();
        engine
            .sync_filters(vec![
                Filter {
                    kind: FilterKind::Substr("a".into()),
                    direction: Direction::Both,
                    action_on_match: FilterAction::Drop,
                },
                Filter {
                    kind: FilterKind::Substr("b".into()),
                    direction: Direction::Both,
                    action_on_match: FilterAction::Drop,
                },
            ])
            .await
            .unwrap();
        assert_eq!(engine.filter_count(), 2);
        assert_eq!(engine.shared_filters.len(), 2);
        assert!(engine.active_rule_names().is_empty());

        // Filters survive a rule being added (the new engine shares the set).
        let echo_addr = spawn_echo_server().await;
        let proxy = free_port_addr().await;
        engine
            .add_adhoc_rule(tcp_spec("r", proxy, echo_addr))
            .await
            .unwrap();
        assert_eq!(engine.shared_filters.len(), 2);

        // Swap to empty: live set follows atomically.
        engine.sync_filters(vec![]).await.unwrap();
        assert_eq!(engine.filter_count(), 0);
        assert!(engine.shared_filters.is_empty());
        engine.shutdown().await;
    }

    /// Filters that fail to compile (invalid regex) are skipped with a
    /// warning, like the engine's own static filter path.
    #[tokio::test]
    async fn sync_filters_skips_invalid_regex() {
        let mut engine = test_engine();
        engine
            .sync_filters(vec![
                Filter {
                    kind: FilterKind::Regex("(unclosed".into()),
                    direction: Direction::Both,
                    action_on_match: FilterAction::Drop,
                },
                Filter {
                    kind: FilterKind::Substr("ok".into()),
                    direction: Direction::Both,
                    action_on_match: FilterAction::Drop,
                },
            ])
            .await
            .unwrap();
        // Bookkeeping keeps what the server sent; the live set only holds
        // compiled filters.
        assert_eq!(engine.filter_count(), 2);
        assert_eq!(engine.shared_filters.len(), 1);
    }
}
