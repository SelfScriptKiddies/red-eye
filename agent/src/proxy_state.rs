//! Pure proxy capture-source model: which passive keys are excluded while
//! a proxy is effective, and which ordered steps enable, update or disable
//! a proxy rule. IO-free on purpose: the supervisor in `supervisor.rs` is
//! a thin shell that executes these plans against the harpoon engine,
//! nftables and the sniffer, so ordering and rollback stay unit-testable.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use harpoon_core::types::endpoint::Protocol;

use crate::updates::{AssignedService, TransportProto};

/// Desired proxy state for one service (from snapshots or commands).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDesired {
    pub service_id: u64,
    pub name: String,
    pub listen: SocketAddr,
    pub target: SocketAddr,
    pub protocol: TransportProto,
    /// Original service port: the nftables redirect source and the key
    /// excluded from passive capture while the proxy is effective.
    pub original_port: u16,
    pub tls_mode: Option<String>,
    pub http2: bool,
    pub revision: u64,
}

/// Effective proxy rule the agent is currently running for a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyEffective {
    pub service_id: u64,
    /// Desired revision this effective rule was built from.
    pub revision: u64,
    pub name: String,
    pub protocol: TransportProto,
    pub original_port: u16,
    pub listen_port: u16,
}

/// Heartbeat-reported phase for one service's proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyPhase {
    Effective,
    Transitioning,
    CaptureUnavailable,
    Failed,
}

impl ProxyPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProxyPhase::Effective => "effective",
            ProxyPhase::Transitioning => "transitioning",
            ProxyPhase::CaptureUnavailable => "capture_unavailable",
            ProxyPhase::Failed => "failed",
        }
    }
}

/// Diagnostic ledger entry for one service's proxy (heartbeat source).
#[derive(Debug, Clone)]
pub struct ProxyDiag {
    pub phase: ProxyPhase,
    pub desired_revision: u64,
    /// Desired revision actually applied (0 = none yet).
    pub effective_revision: u64,
    pub last_error: Option<String>,
    pub protocol: TransportProto,
    pub original_port: u16,
    pub listen_port: u16,
}

/// Which (protocol, port) keys the sniffer must NOT capture: services
/// whose key is withheld (effective or mid-transition). The subtraction is
/// protocol-aware - a TCP proxy never disables passive UDP on the same
/// numeric port.
pub fn excluded_keys(
    services: &HashMap<u64, AssignedService>,
    excluded: &HashSet<u64>,
) -> HashSet<(TransportProto, u16)> {
    services
        .iter()
        .filter(|(id, _)| excluded.contains(id))
        .map(|(_, s)| (s.protocol, s.port))
        .collect()
}

/// Effective passive capture keys: assigned services minus effectively
/// proxied (or transitioning) service keys.
pub fn passive_ports(
    services: &HashMap<u64, AssignedService>,
    excluded: &HashSet<u64>,
) -> HashSet<(TransportProto, u16)> {
    services
        .iter()
        .filter(|(id, _)| !excluded.contains(id))
        .map(|(_, s)| (s.protocol, s.port))
        .collect()
}

/// What the supervisor must do to converge one service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyKind {
    /// Desired and effective agree.
    None,
    /// Enable from scratch.
    Enable,
    /// Effective rule differs from desired (revision or spec): replace it.
    Update,
    /// Effective rule with no desired state: disable it.
    Disable,
}

pub fn desired_needs_apply(
    desired: Option<&ProxyDesired>,
    effective: Option<&ProxyEffective>,
) -> ApplyKind {
    match (desired, effective) {
        (None, None) => ApplyKind::None,
        (Some(_), None) => ApplyKind::Enable,
        (None, Some(_)) => ApplyKind::Disable,
        (Some(d), Some(e)) => {
            let spec_matches = e.name == d.name
                && e.protocol == d.protocol
                && e.original_port == d.original_port
                && e.listen_port == d.listen.port()
                && e.revision == d.revision;
            if spec_matches {
                ApplyKind::None
            } else {
                ApplyKind::Update
            }
        }
    }
}

/// Build the harpoon rule spec for a desired proxy: the nftables redirect
/// targets the ORIGINAL service port (protocol-aware), never the proxy
/// listen port.
pub fn spec_from_desired(desired: &ProxyDesired) -> crate::harpoon::ProxyRuleSpec {
    crate::harpoon::ProxyRuleSpec {
        name: desired.name.clone(),
        listen: desired.listen,
        target: desired.target,
        protocol: match desired.protocol {
            TransportProto::Tcp => Protocol::Tcp,
            TransportProto::Udp => Protocol::Udp,
        },
        tls_mode: desired.tls_mode.clone(),
        nft_redirect_port: Some(desired.original_port),
        duplicate_to: None,
        http2: desired.http2,
    }
}

/// Validation before any state change. The supervisor runs this first: a
/// failing rule must never touch the capture-source selection.
pub fn validate_desired(
    desired: &ProxyDesired,
    tls_available: bool,
    nft_available: bool,
) -> Result<(), String> {
    if desired.service_id == 0 {
        return Err("proxy rule carries service id 0".to_string());
    }
    if desired.name.is_empty() || desired.name.len() > 128 {
        return Err("proxy rule name is empty or too long".to_string());
    }
    if desired.listen.ip().is_unspecified() && desired.listen.port() == 0 {
        return Err("proxy listen port is 0".to_string());
    }
    if desired.target.port() == 0 {
        return Err("proxy target port is 0".to_string());
    }
    if desired.original_port == 0 {
        return Err("proxy original service port is 0".to_string());
    }
    match desired.tls_mode.as_deref() {
        None | Some("") => {}
        Some("mitm") if !tls_available => {
            return Err("TLS MITM requested but agent has no CA certificates".to_string())
        }
        Some("mitm") | Some("terminate") | Some("passthrough") => {}
        Some(other) => return Err(format!("unknown tls mode '{}'", other)),
    }
    if nft_available {
        Ok(())
    } else {
        Err("nftables binary is unavailable: redirect cannot be installed".to_string())
    }
}

/// Ordered steps of a fresh enable (spec order: prepare first so passive
/// capture stays authoritative while nothing is redirected yet).
pub const ENABLE_STEPS: &[EnableStep] = &[
    EnableStep::PrepareRule,
    EnableStep::ExcludePassive,
    EnableStep::ActivateCapture,
    EnableStep::InstallRedirect,
    EnableStep::MarkEffective,
];

/// Ordered steps of an update: teardown of the old rule fully completes
/// before the new engine binds the (possibly identical) listen port, so
/// there is never a window with two authoritative producers for the key.
pub const UPDATE_STEPS: &[EnableStep] = &[
    EnableStep::UninstallRedirect,
    EnableStep::RetireRule,
    EnableStep::PrepareRule,
    EnableStep::ExcludePassive,
    EnableStep::ActivateCapture,
    EnableStep::InstallRedirect,
    EnableStep::MarkEffective,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnableStep {
    PrepareRule,
    ExcludePassive,
    ActivateCapture,
    InstallRedirect,
    MarkEffective,
    /// Update-only: remove the old rule's nftables redirect.
    UninstallRedirect,
    /// Update-only: stop + drain the old rule's engine and session.
    RetireRule,
}

/// Ordered steps of a disable. The original key stays excluded until the
/// proxy provably cannot publish anymore (steps 3+4 below).
pub const DISABLE_STEPS: &[DisableStep] = &[
    DisableStep::MarkNotEffective,
    DisableStep::UninstallRedirect,
    DisableStep::StopPublication,
    DisableStep::RestorePassive,
    DisableStep::Ack,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisableStep {
    /// Reconciliation must not revive the rule from a stale snapshot.
    MarkNotEffective,
    UninstallRedirect,
    /// Stop capture publication + retire the rule, drain existing
    /// redirected sessions within a bounded interval.
    StopPublication,
    /// Return the original key to passive capture (if still assigned).
    RestorePassive,
    Ack,
}

/// Cleanup actions after a failed enable/update step. Derived from the
/// steps already completed, so rollback ordering is tested, not improvised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cleanup {
    StopRuleSession,
    StopRuleEngine,
    RemoveRedirect,
    /// Return the original key to passive capture.
    RestorePassive,
}

pub fn enable_rollback(steps_done: &[EnableStep]) -> Vec<Cleanup> {
    let mut plan = Vec::new();
    // Reverse order of side effects.
    if steps_done.contains(&EnableStep::InstallRedirect) {
        plan.push(Cleanup::RemoveRedirect);
    }
    if steps_done.contains(&EnableStep::ActivateCapture) {
        plan.push(Cleanup::StopRuleSession);
    }
    if steps_done.contains(&EnableStep::PrepareRule) {
        plan.push(Cleanup::StopRuleEngine);
    }
    if steps_done.contains(&EnableStep::ExcludePassive) {
        plan.push(Cleanup::RestorePassive);
    }
    plan
}

pub fn disable_rollback(steps_done: &[DisableStep]) -> Vec<Cleanup> {
    // A failed disable must keep the passive key excluded until the proxy
    // provably stopped: nothing is restored on failure. The supervisor
    // reports the failed phase and retries.
    let _ = steps_done;
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn services() -> HashMap<u64, AssignedService> {
        HashMap::from([
            (
                1,
                AssignedService {
                    protocol: TransportProto::Tcp,
                    port: 80,
                },
            ),
            (
                2,
                AssignedService {
                    protocol: TransportProto::Udp,
                    port: 53,
                },
            ),
            (
                3,
                AssignedService {
                    protocol: TransportProto::Tcp,
                    port: 8080,
                },
            ),
        ])
    }

    #[test]
    fn passive_subtraction_is_protocol_aware() {
        // TCP:80 proxied; UDP:53 proxied. TCP and UDP on the same numeric
        // port stay independent: add TCP:53 and it must survive.
        let mut svc = services();
        svc.insert(
            4,
            AssignedService {
                protocol: TransportProto::Tcp,
                port: 53,
            },
        );
        let excluded = HashSet::from([1u64, 2]);
        let passive = passive_ports(&svc, &excluded);
        assert_eq!(
            passive,
            HashSet::from([(TransportProto::Tcp, 8080u16), (TransportProto::Tcp, 53u16),])
        );
        let keys = excluded_keys(&svc, &excluded);
        assert_eq!(
            keys,
            HashSet::from([(TransportProto::Tcp, 80u16), (TransportProto::Udp, 53u16)])
        );
    }

    #[test]
    fn empty_service_set_passive_is_empty() {
        let passive = passive_ports(&HashMap::new(), &HashSet::from([7u64]));
        assert!(passive.is_empty());
    }

    fn desired(sid: u64, revision: u64, port: u16) -> ProxyDesired {
        ProxyDesired {
            service_id: sid,
            name: format!("protect-{sid}-x"),
            listen: format!("0.0.0.0:{}", port + 10000).parse().unwrap(),
            target: format!("127.0.0.1:{port}").parse().unwrap(),
            protocol: TransportProto::Tcp,
            original_port: port,
            tls_mode: None,
            http2: false,
            revision,
        }
    }

    fn effective(sid: u64, revision: u64, port: u16) -> ProxyEffective {
        ProxyEffective {
            service_id: sid,
            revision,
            name: format!("protect-{sid}-x"),
            protocol: TransportProto::Tcp,
            original_port: port,
            listen_port: port + 10000,
        }
    }

    #[test]
    fn apply_kind_decisions() {
        assert_eq!(desired_needs_apply(None, None), ApplyKind::None);
        assert_eq!(
            desired_needs_apply(Some(&desired(1, 3, 80)), None),
            ApplyKind::Enable
        );
        assert_eq!(
            desired_needs_apply(None, Some(&effective(1, 3, 80))),
            ApplyKind::Disable
        );
        // Same revision + spec: idempotent, no second rule/forwarder.
        assert_eq!(
            desired_needs_apply(Some(&desired(1, 3, 80)), Some(&effective(1, 3, 80))),
            ApplyKind::None
        );
        // Newer desired revision: update.
        assert_eq!(
            desired_needs_apply(Some(&desired(1, 4, 80)), Some(&effective(1, 3, 80))),
            ApplyKind::Update
        );
        // Changed listen port at the same revision is still an update.
        let mut d = desired(1, 3, 80);
        d.listen = "0.0.0.0:20081".parse().unwrap();
        assert_eq!(
            desired_needs_apply(Some(&d), Some(&effective(1, 3, 80))),
            ApplyKind::Update
        );
    }

    #[test]
    fn enable_order_matches_spec() {
        // Prepare (no redirect) -> exclude passive -> publisher -> redirect
        // -> effective. A short bounded capture gap is intentional; double
        // capture is not.
        let steps = ENABLE_STEPS;
        let pos = |s: EnableStep| steps.iter().position(|&x| x == s).unwrap();
        assert!(pos(EnableStep::PrepareRule) < pos(EnableStep::ExcludePassive));
        assert!(pos(EnableStep::ExcludePassive) < pos(EnableStep::ActivateCapture));
        assert!(pos(EnableStep::ActivateCapture) < pos(EnableStep::InstallRedirect));
        assert!(pos(EnableStep::InstallRedirect) < pos(EnableStep::MarkEffective));
    }

    #[test]
    fn update_retears_old_rule_before_new_engine_binds() {
        let pos = |s: EnableStep| UPDATE_STEPS.iter().position(|&x| x == s).unwrap();
        assert!(pos(EnableStep::UninstallRedirect) < pos(EnableStep::RetireRule));
        assert!(pos(EnableStep::RetireRule) < pos(EnableStep::PrepareRule));
        assert!(pos(EnableStep::PrepareRule) < pos(EnableStep::InstallRedirect));
    }

    #[test]
    fn disable_restores_passive_only_after_publication_stops() {
        let pos = |s: DisableStep| DISABLE_STEPS.iter().position(|&x| x == s).unwrap();
        assert!(pos(DisableStep::MarkNotEffective) < pos(DisableStep::UninstallRedirect));
        assert!(pos(DisableStep::UninstallRedirect) < pos(DisableStep::StopPublication));
        assert!(pos(DisableStep::StopPublication) < pos(DisableStep::RestorePassive));
    }

    #[test]
    fn rollback_covers_every_acquired_resource() {
        // Failure before exclusion: old state stays, nothing restored.
        assert_eq!(
            enable_rollback(&[EnableStep::PrepareRule]),
            vec![Cleanup::StopRuleEngine]
        );

        // Failure after exclusion: passive must come back.
        assert_eq!(
            enable_rollback(&[EnableStep::PrepareRule, EnableStep::ExcludePassive]),
            vec![Cleanup::StopRuleEngine, Cleanup::RestorePassive]
        );

        // Failure after redirect: everything the transition acquired is
        // undone in reverse order.
        assert_eq!(
            enable_rollback(&[
                EnableStep::PrepareRule,
                EnableStep::ExcludePassive,
                EnableStep::ActivateCapture,
                EnableStep::InstallRedirect,
            ]),
            vec![
                Cleanup::RemoveRedirect,
                Cleanup::StopRuleSession,
                Cleanup::StopRuleEngine,
                Cleanup::RestorePassive,
            ]
        );
    }

    #[test]
    fn failed_disable_restores_nothing() {
        // Passive stays excluded until the proxy provably stopped.
        assert!(disable_rollback(&[DisableStep::StopPublication]).is_empty());
    }

    #[test]
    fn validation_rejects_incomplete_rules() {
        let mut d = desired(1, 3, 80);
        assert_eq!(validate_desired(&d, true, true), Ok(()));

        d.service_id = 0;
        assert!(validate_desired(&d, true, true).is_err());
        d.service_id = 1;

        d.tls_mode = Some("bogus".into());
        assert!(validate_desired(&d, true, true).is_err());
        d.tls_mode = Some("mitm".into());
        assert!(validate_desired(&d, false, true).is_err());
        d.tls_mode = None;

        d.original_port = 0;
        assert!(validate_desired(&d, true, true).is_err());
        d.original_port = 80;

        assert!(validate_desired(&d, true, false).is_err());
    }

    #[test]
    fn phases_render_as_heartbeat_states() {
        assert_eq!(ProxyPhase::Effective.as_str(), "effective");
        assert_eq!(ProxyPhase::Transitioning.as_str(), "transitioning");
        assert_eq!(
            ProxyPhase::CaptureUnavailable.as_str(),
            "capture_unavailable"
        );
        assert_eq!(ProxyPhase::Failed.as_str(), "failed");
    }
}
