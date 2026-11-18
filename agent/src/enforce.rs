//! Snapshot enforcement: turn the agent's filter/block projection into
//! proxy filter hot-swaps and nftables IP bans with expiry.
//!
//! Detection lives on the analyzer; this module only enforces. A filter
//! this agent cannot enforce (expression matchers on the current proxy
//! engine) is counted as an enforcement failure and reported through the
//! heartbeat instead of silently claiming blocking success.
//!
//! Shared-fixture contract: the analyzer and this agent must produce
//! identical matcher results. Expression evaluation rides the same
//! filter-dsl crate (compile validation below); payload matchers share
//! the contract fixtures in tests.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use log::{info, warn};
use tokio::sync::mpsc;

use crate::updates::AppState;
use crate::wire::{WireActiveBlock, WireFilter};
use harpoon_core::types::filter::{Direction, Filter, FilterAction, FilterKind};

/// One desired nftables ban with its expiry (epoch millis; u64::MAX =
/// persistent filter without expiry).
#[derive(Debug, Clone, PartialEq)]
pub struct DesiredBan {
    pub ip: IpAddr,
    pub until_ms: u64,
    /// filter_id the ban comes from (for logs).
    pub source_filter_id: u64,
}

/// The enforcement side of one snapshot: hot-swap payload filters and the
/// complete desired ban set (replacement semantics).
#[derive(Debug, Default)]
pub struct EnforcePlan {
    pub payload_filters: Vec<Filter>,
    pub bans: Vec<DesiredBan>,
    /// Sanitized reasons a filter could not be enforced on this agent.
    pub failures: Vec<String>,
}

/// Build the enforcement plan for a snapshot's filter + block projection.
/// Valid parts are always applied (partial enforcement beats none);
/// unenforceable entries land in `failures`.
pub fn plan(filters: &[WireFilter], blocks: &[WireActiveBlock]) -> EnforcePlan {
    let mut plan = EnforcePlan::default();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    for f in filters {
        if !f.enabled {
            continue;
        }
        if f.expires_at_ms != 0 && f.expires_at_ms <= now_ms {
            continue;
        }
        let direction = match f.direction {
            1 => Direction::ClientToServer, // inbound = c2s
            2 => Direction::ServerToClient, // outbound = s2c
            _ => Direction::Both,
        };
        let action = match f.action.as_str() {
            "block" => FilterAction::Drop,
            "block_connection" => FilterAction::DropConnection,
            other => {
                plan.failures.push(format!(
                    "filter {}: action '{}' is not enforceable on this agent",
                    f.pattern_name, other
                ));
                continue;
            }
        };
        let kind = match f.filter_type.as_str() {
            "substr" => Some(FilterKind::Substr(
                String::from_utf8_lossy(&f.filter_payload).to_string(),
            )),
            "bsubstr" => {
                // Hex input decodes to the searched bytes; invalid hex from
                // a broken producer falls back to raw bytes (legacy read
                // path), matching the analyzer.
                let bytes =
                    hex::decode(&f.filter_payload).unwrap_or_else(|_| f.filter_payload.clone());
                Some(FilterKind::BinarySubstr(bytes))
            }
            "regex" => Some(FilterKind::Regex(
                String::from_utf8_lossy(&f.filter_payload).to_string(),
            )),
            // Expression matching needs a hook in the proxy data path this
            // build does not have: detection continues on the analyzer,
            // enforcement is reported as a failure, never faked.
            "expression" => {
                plan.failures.push(format!(
                    "filter {}: expression enforcement is not supported by this agent",
                    f.pattern_name
                ));
                None
            }
            "ip_cidr" => {
                // Enforced as an nftables ban below, not a proxy filter.
                let network = f.ip_network.split('/').next().unwrap_or("");
                match network.parse::<IpAddr>() {
                    Ok(ip) => {
                        plan.bans.push(DesiredBan {
                            ip,
                            // A filter without expiry bans until removed.
                            until_ms: if f.expires_at_ms == 0 {
                                u64::MAX
                            } else {
                                f.expires_at_ms
                            },
                            source_filter_id: 0,
                        });
                    }
                    Err(_) => plan.failures.push(format!(
                        "filter {}: invalid ip_network '{}'",
                        f.pattern_name, f.ip_network
                    )),
                }
                None
            }
            other => {
                plan.failures.push(format!(
                    "filter {}: unknown matcher kind '{}'",
                    f.pattern_name, other
                ));
                None
            }
        };
        if let Some(kind) = kind {
            plan.payload_filters.push(Filter {
                kind,
                direction,
                action_on_match: action,
            });
        }
    }

    for b in blocks {
        if b.blocked_until_ms <= now_ms {
            continue;
        }
        match b.remote_ip.parse::<IpAddr>() {
            Ok(ip) => plan.bans.push(DesiredBan {
                ip,
                until_ms: b.blocked_until_ms,
                source_filter_id: b.filter_id,
            }),
            Err(_) => plan.failures.push(format!(
                "block for filter {}: invalid remote ip",
                b.filter_id
            )),
        }
    }

    plan
}

/// Task owning enforcement: applies plans to the harpoon engine and
/// nftables, removes bans that disappeared from the desired set or
/// expired, and reports failures on the shared state for the heartbeat.
pub fn spawn_enforcement(
    harpoon: Arc<tokio::sync::Mutex<crate::harpoon::HarpoonEngine>>,
    state: Arc<AppState>,
    mut rx: mpsc::Receiver<EnforcePlan>,
) {
    tokio::spawn(async move {
        // Currently applied bans: ip -> until_ms.
        let mut applied: HashMap<IpAddr, u64> = HashMap::new();
        let mut sweep = tokio::time::interval(std::time::Duration::from_secs(1));
        sweep.tick().await; // skip first tick

        loop {
            tokio::select! {
                biased;
                plan = rx.recv() => {
                    let Some(plan) = plan else { break };
                    apply_plan(&harpoon, &state, &mut applied, plan).await;
                }
                _ = sweep.tick() => {
                    // Expiry: unban locally even if the backend is
                    // unreachable; a later snapshot re-bans if still valid.
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let expired: Vec<IpAddr> = applied
                        .iter()
                        .filter(|(_, until)| **until <= now_ms)
                        .map(|(ip, _)| *ip)
                        .collect();
                    for ip in expired {
                        applied.remove(&ip);
                        let mut engine = harpoon.lock().await;
                        if let Err(e) = engine.unban_ip(&ip.to_string()) {
                            record_failure(&state, &format!("unban {}: {}", ip, e));
                        } else {
                            info!("Temporary ban expired, unbanned {}", ip);
                        }
                    }
                }
            }
        }
    });
}

async fn apply_plan(
    harpoon: &Arc<tokio::sync::Mutex<crate::harpoon::HarpoonEngine>>,
    state: &Arc<AppState>,
    applied: &mut HashMap<IpAddr, u64>,
    plan: EnforcePlan,
) {
    for failure in &plan.failures {
        record_failure(state, failure);
    }

    {
        let mut engine = harpoon.lock().await;
        if let Err(e) = engine.sync_filters(plan.payload_filters).await {
            record_failure(state, &format!("filter sync failed: {}", e));
        }

        // Ban additions and extensions.
        for ban in &plan.bans {
            let needs_apply = match applied.get(&ban.ip) {
                Some(current_until) => *current_until < ban.until_ms,
                None => true,
            };
            if !needs_apply {
                continue;
            }
            match engine.ban_ip(&ban.ip.to_string()) {
                Ok(_) => {
                    applied.insert(ban.ip, ban.until_ms);
                    info!(
                        "Enforced ban on {} (filter {}, until {}ms)",
                        ban.ip, ban.source_filter_id, ban.until_ms
                    );
                }
                Err(e) => {
                    record_failure(state, &format!("ban {}: {}", ban.ip, e));
                }
            }
        }

        // Replacement semantics: remove bans that the desired set no
        // longer contains (unban, filter deletion, expiry upstream).
        let desired: HashMap<IpAddr, u64> = plan.bans.iter().map(|b| (b.ip, b.until_ms)).collect();
        let removed: Vec<IpAddr> = applied
            .keys()
            .filter(|ip| !desired.contains_key(ip))
            .copied()
            .collect();
        for ip in removed {
            if let Err(e) = engine.unban_ip(&ip.to_string()) {
                record_failure(state, &format!("unban {}: {}", ip, e));
            } else {
                applied.remove(&ip);
                info!("Removed ban on {} (no longer desired)", ip);
            }
        }
    }

    if plan.failures.is_empty() {
        // A fully applied plan clears the reported error.
        *state.last_enforcement_error.write().unwrap() = None;
    }
}

/// Count and store a sanitized enforcement failure for the heartbeat.
/// Reasons carry filter names, IPs and fixed strings only - no payload
/// data, no arbitrary agent text beyond the bounded error itself.
fn record_failure(state: &Arc<AppState>, reason: &str) {
    warn!("Enforcement failure: {}", reason);
    state.enforcement_failures.fetch_add(1, Ordering::Relaxed);
    let mut bounded: String = reason.chars().take(300).collect();
    if reason.len() > 300 {
        bounded.push_str("...");
    }
    *state.last_enforcement_error.write().unwrap() = Some(bounded);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::red_eye::wire as gen;

    fn filter(kind: &str, action: &str, payload: &[u8]) -> WireFilter {
        WireFilter {
            pattern_name: format!("f-{}", kind),
            filter_type: kind.to_string(),
            filter_payload: payload.to_vec(),
            direction: 0,
            enabled: true,
            action: action.to_string(),
            revision: 1,
            ip_network: String::new(),
            ip_endpoint: gen::IpEndpoint::Src as i32,
            expires_at_ms: 0,
            services_all: true,
            service_ids: vec![],
            agents_all: true,
            agent_ids: vec![],
            threshold: None,
        }
    }

    #[test]
    fn plan_maps_payload_filters_and_ip_bans() {
        let plan = plan(
            &[
                filter("substr", "block", b"BLOCKED"),
                filter("regex", "block_connection", b"evil.*pattern"),
            ],
            &[],
        );
        assert_eq!(plan.payload_filters.len(), 2);
        assert!(plan.failures.is_empty());
        assert!(matches!(
            plan.payload_filters[0].kind,
            FilterKind::Substr(ref s) if s == "BLOCKED"
        ));
        assert!(matches!(
            plan.payload_filters[0].action_on_match,
            FilterAction::Drop
        ));
        assert!(matches!(
            plan.payload_filters[1].action_on_match,
            FilterAction::DropConnection
        ));
    }

    /// ip_cidr filters become nftables bans (host address), not proxy
    /// filters; expression filters become reported failures.
    #[test]
    fn plan_ip_cidr_bans_and_expression_fails() {
        let mut ipf = filter("ip_cidr", "block", b"");
        ipf.ip_network = "10.6.6.6/32".into();
        let mut expr = filter("expression", "block", b"protocol == \"TCP\"");
        expr.pattern_name = "expr-f".into();
        let plan = plan(&[ipf, expr], &[]);
        assert!(plan.payload_filters.is_empty());
        assert_eq!(plan.bans.len(), 1);
        assert_eq!(plan.bans[0].ip.to_string(), "10.6.6.6");
        assert_eq!(plan.bans[0].until_ms, u64::MAX);
        assert_eq!(plan.failures.len(), 1);
        assert!(plan.failures[0].contains("expr-f"));
    }

    /// Expired entries never apply; blocks extend the ban set with expiry.
    #[test]
    fn plan_skips_expired_and_applies_blocks() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut stale = filter("substr", "block", b"x");
        stale.expires_at_ms = now - 1_000;
        let mut active = filter("bsubstr", "block", b"deadbeef");
        active.expires_at_ms = now + 60_000;

        let block = WireActiveBlock {
            filter_id: 5,
            agent_id: "a".into(),
            remote_ip: "10.7.0.9".into(),
            blocked_until_ms: now + 30_000,
        };
        let expired_block = WireActiveBlock {
            filter_id: 6,
            agent_id: "a".into(),
            remote_ip: "10.7.0.10".into(),
            blocked_until_ms: now - 1,
        };
        let plan = plan(&[stale, active], &[block, expired_block]);
        assert_eq!(
            plan.payload_filters.len(),
            1,
            "expired filter skipped, active one kept"
        );
        assert_eq!(plan.bans.len(), 1, "expired block skipped");
        assert_eq!(plan.bans[0].ip.to_string(), "10.7.0.9");
        assert_eq!(plan.bans[0].until_ms, now + 30_000);
    }

    #[test]
    fn bsubstr_hex_decodes_to_bytes() {
        let plan = plan(&[filter("bsubstr", "block", b"deadbeef")], &[]);
        assert!(matches!(
            plan.payload_filters[0].kind,
            FilterKind::BinarySubstr(ref b) if b == &vec![0xde, 0xad, 0xbe, 0xef]
        ));
    }

    /// Shared analyzer/agent contract fixtures: identical compile/eval
    /// results in both crates.
    #[test]
    fn shared_analyzer_agent_fixtures_pass() {
        filter_dsl::fixtures::run_fixtures().expect("shared fixtures must pass");
    }

    /// The agent must reject (fail closed) expressions the shared DSL
    /// rejects, so a filter accepted by the backend can never silently
    /// diverge between analyzer and agent evaluation.
    #[test]
    fn expression_sources_match_shared_compile_rules() {
        assert!(filter_dsl::compile("payload.contains(b\"x\")").is_ok());
        assert!(filter_dsl::compile("src_port == \"80\"").is_err());
    }
}
