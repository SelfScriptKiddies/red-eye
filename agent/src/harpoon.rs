use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use log::{debug, info, warn};

use harpoon_core::capture::CaptureManager;
use harpoon_core::engine::filter::{CompiledFilter, SharedFilterSet};
use harpoon_core::engine::run_with_capture;
use harpoon_core::types::endpoint::{Endpoint, Protocol};
use harpoon_core::types::filter::{Direction, Filter, FilterAction, FilterKind};
use harpoon_core::types::rule::{DuplicateTarget, Rule, TlsConfig, TlsMode, UdpSourceMode};
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
    /// If set, duplicate all traffic to this endpoint (e.g. another team)
    pub duplicate_to: Option<SocketAddr>,
    /// Enable HTTP/2 proxy mode
    pub http2: bool,
}

impl ProxyRuleSpec {
    /// Convert to a harpoon-core Rule.
    ///
    /// The rule carries NO filters of its own: the engine is started with a
    /// [`SharedFilterSet`] (see [`HarpoonEngine`]) that all rules evaluate
    /// traffic against, so filters can be hot-swapped without restarting
    /// the engine.
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
}

pub struct HarpoonEngine {
    ports_cache: PortsCache,
    restart_tx: RestartNotifier,

    handle: Option<EngineHandle>,
    capture_mgr: Arc<CaptureManager>,
    rules: HashMap<String, ProxyRuleSpec>,
    /// Last synced filters (parsed form; kept for `filter_count`/status).
    filters: Vec<Filter>,
    /// Live filter set shared with all running proxy pipelines.
    ///
    /// `sync_filters` atomically swaps it (`store`); running proxies pick up
    /// the new set for traffic evaluated after the swap — no engine restart,
    /// no listener churn, no connection drops. Survives `restart_engine`
    /// (rule add/remove) because the engine is (re)started with a handle to
    /// the same set.
    shared_filters: SharedFilterSet,
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
            filters: Vec::new(),
            shared_filters: SharedFilterSet::empty(),
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

    /// Sync global filters (applied to all proxy rules) by hot-swapping the
    /// shared filter set.
    ///
    /// Does NOT restart the engine: running proxies keep serving, listeners
    /// stay bound, established connections keep flowing. Traffic evaluated
    /// after the swap is checked against the new filter set — per chunk, so
    /// even pre-existing connections are filtered going forward (a
    /// connection sending newly-blocked content is dropped/reset). There is
    /// never a window where no filters apply: the swap is atomic, and a
    /// chunk mid-evaluation during the swap completes under the old set.
    ///
    /// Filters that fail to compile (e.g. invalid regex) are skipped with a
    /// warning — same behavior as the engine's own static filter path.
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

    /// (Re)start the engine for rule add/remove/update paths. Rules are
    /// rebuilt WITHOUT owned filters: all filtering flows through the shared
    /// filter set, which carries the currently synced filters across the
    /// restart.
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
            shared_filter_set: Some(self.shared_filters.clone()),
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

/// Parse a filter definition from JSON fields into a harpoon-core Filter.
pub fn parse_filter(
    kind: &str,
    pattern: &str,
    direction: &str,
    action: &str,
) -> Result<Filter> {
    let filter_kind = match kind {
        "substr" => FilterKind::Substr(pattern.to_string()),
        "bsubstr" => {
            let bytes = hex::decode(pattern)
                .or_else(|_| Ok::<Vec<u8>, anyhow::Error>(pattern.as_bytes().to_vec()))?;
            FilterKind::BinarySubstr(bytes)
        }
        "regex" => FilterKind::Regex(pattern.to_string()),
        other => anyhow::bail!("Unknown filter kind: {}", other),
    };

    let dir = match direction {
        "c2s" | "inbound" | "in" => Direction::ClientToServer,
        "s2c" | "outbound" | "out" => Direction::ServerToClient,
        "both" | "" => Direction::Both,
        other => anyhow::bail!("Unknown direction: {}", other),
    };

    let action = match action {
        "drop" => FilterAction::Drop,
        "drop_connection" => FilterAction::DropConnection,
        "alert" | "tap" => FilterAction::TapOnly,
        "pass" | "" => FilterAction::Pass,
        other => anyhow::bail!("Unknown action: {}", other),
    };

    Ok(Filter {
        kind: filter_kind,
        direction: dir,
        action_on_match: action,
    })
}

/// Spawn a background task that forwards captured packets from harpoon to NATS.
pub fn spawn_capture_forwarder(
    capture_mgr: Arc<CaptureManager>,
    nats: crate::nats::NatsClient,
    machine_ip: std::net::IpAddr,
) {
    tokio::spawn(async move {
        let mut rx = capture_mgr.subscribe();
        info!("Harpoon capture forwarder started");

        loop {
            match rx.recv().await {
                Ok(packet) => {
                    if let Some(encoded) = packet_synthesis::encode_for_nats(&packet, machine_ip) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn test_engine() -> (HarpoonEngine, tokio::sync::watch::Receiver<()>) {
        let (restart_tx, restart_rx) = tokio::sync::watch::channel(());
        let ports_cache: PortsCache =
            Arc::new(std::sync::RwLock::new(std::collections::HashSet::new()));
        (HarpoonEngine::new(ports_cache, restart_tx, None), restart_rx)
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

    /// Live proof that `sync_filters` hot-swaps the filter set WITHOUT
    /// restarting the engine: the pre-existing proxied connection must
    /// survive the sync (a restart kills every proxied connection), traffic
    /// evaluated after the swap uses the new filters, and pre-existing
    /// connections are filtered going forward (per-chunk granularity).
    #[tokio::test]
    async fn sync_filters_hot_swaps_without_engine_restart() {
        let echo_addr = spawn_echo_server().await;
        let proxy_addr = free_port_addr().await;

        let (mut engine, _rx) = test_engine();
        engine
            .add_rule(ProxyRuleSpec {
                name: "test-rule".into(),
                listen: proxy_addr,
                target: echo_addr,
                protocol: Protocol::Tcp,
                tls_mode: None,
                nft_redirect_port: None,
                duplicate_to: None,
                http2: false,
            })
            .await
            .unwrap();
        assert!(engine.handle.is_some(), "engine should be running");

        // Connection A: established and echoing under the (empty) set.
        // add_rule returns before the spawned listener task binds, so retry
        // the connect until the proxy comes up.
        let mut a = None;
        for _ in 0..100 {
            match TcpStream::connect(proxy_addr).await {
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

        // Sync a drop-connection filter for "BLOCK".
        engine
            .sync_filters(vec![Filter {
                kind: FilterKind::Substr("BLOCK".into()),
                direction: Direction::Both,
                action_on_match: FilterAction::DropConnection,
            }])
            .await
            .unwrap();
        assert_eq!(engine.filter_count(), 1);

        // THE regression assertion: the engine was not restarted —
        // connection A still echoes through the same engine handle.
        a.write_all(b"still-here").await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf))
            .await
            .expect("conn A died: engine was restarted by sync_filters")
            .unwrap();
        assert_eq!(&buf[..n], b"still-here");

        // New connection B sending blocked content is cut by the new set.
        let mut b = TcpStream::connect(proxy_addr).await.unwrap();
        b.write_all(b"BLOCK").await.unwrap();
        let res = tokio::time::timeout(Duration::from_secs(2), b.read(&mut buf)).await;
        assert!(
            matches!(res, Ok(Ok(0)) | Ok(Err(_))),
            "blocked connection not closed"
        );

        // Per-chunk granularity: pre-existing connection A is filtered too.
        a.write_all(b"BLOCK").await.unwrap();
        let res = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf)).await;
        assert!(
            matches!(res, Ok(Ok(0)) | Ok(Err(_))),
            "pre-existing connection not filtered after swap"
        );

        engine.shutdown().await;
    }

    /// Pure swap-path logic: bookkeeping + live set updated, no engine
    /// spawned when there are no rules, filters survive across restarts.
    #[tokio::test]
    async fn sync_filters_updates_bookkeeping_and_live_set() {
        let (mut engine, _rx) = test_engine();
        assert!(engine.handle.is_none());

        engine
            .sync_filters(vec![
                Filter {
                    kind: FilterKind::Substr("a".into()),
                    direction: Direction::Both,
                    action_on_match: FilterAction::Drop,
                },
                Filter {
                    kind: FilterKind::Substr("b".into()),
                    direction: Direction::ClientToServer,
                    action_on_match: FilterAction::TapOnly,
                },
            ])
            .await
            .unwrap();
        assert_eq!(engine.filter_count(), 2);
        assert_eq!(engine.shared_filters.len(), 2);

        // Still no engine: syncing with no rules must not start one.
        assert!(engine.handle.is_none());

        // Swap to empty: live set follows atomically.
        engine.sync_filters(vec![]).await.unwrap();
        assert_eq!(engine.filter_count(), 0);
        assert!(engine.shared_filters.is_empty());
    }

    /// Filters that fail to compile (invalid regex) are skipped with a
    /// warning, like the engine's own static filter path.
    #[tokio::test]
    async fn sync_filters_skips_invalid_regex() {
        let (mut engine, _rx) = test_engine();
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
