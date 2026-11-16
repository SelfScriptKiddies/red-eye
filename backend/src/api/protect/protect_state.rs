/// Pure proxy-protection state logic: capture-source classification,
/// port allocation and desired-state decision tables. Kept IO-free so the
/// ordering guarantees the API relies on are unit-testable.

/// Capture source of one (agent, service) pair as shown in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureSource {
    Passive,
    Proxy,
    Transitioning,
    /// Proxy desired but the agent reports no working proxy capture
    /// (failed transition or lost Harpoon capture).
    ProxyUnavailable,
}

impl CaptureSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            CaptureSource::Passive => "passive",
            CaptureSource::Proxy => "proxy",
            CaptureSource::Transitioning => "transitioning",
            CaptureSource::ProxyUnavailable => "proxy_unavailable",
        }
    }
}

/// Agent-reported capture phase (heartbeat diagnostic, persisted in
/// proxy_protection.reported_state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportedState {
    Effective,
    Transitioning,
    CaptureUnavailable,
    Failed,
}

impl ReportedState {
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "effective" => Some(ReportedState::Effective),
            "transitioning" => Some(ReportedState::Transitioning),
            "capture_unavailable" => Some(ReportedState::CaptureUnavailable),
            "failed" => Some(ReportedState::Failed),
            _ => None,
        }
    }
}

/// Classify the capture source from desired state + agent diagnostics.
/// A backend write alone can never produce Proxy: the agent must report
/// the effective state first.
pub fn capture_source(
    desired_enabled: bool,
    reported: Option<ReportedState>,
    last_error: Option<&str>,
) -> CaptureSource {
    match reported {
        Some(ReportedState::Effective) if desired_enabled => CaptureSource::Proxy,
        Some(ReportedState::Transitioning) => CaptureSource::Transitioning,
        Some(ReportedState::CaptureUnavailable) | Some(ReportedState::Failed) => {
            CaptureSource::ProxyUnavailable
        }
        // Effective-but-desired-disabled means the disable transition is
        // still running on the agent (reconciliation cannot revive the
        // proxy, but the key is not passive yet).
        Some(ReportedState::Effective) => CaptureSource::Transitioning,
        None if desired_enabled && last_error.is_some() => CaptureSource::ProxyUnavailable,
        None if desired_enabled => CaptureSource::Transitioning,
        None => CaptureSource::Passive,
    }
}

/// Ports already bound by this agent's proxy rules (persisted desired
/// rules and live heartbeat redirects), per transport protocol.
pub fn allocate_proxy_port(
    service_port: i32,
    used_ports: &[u16],
    requested: Option<u16>,
) -> Result<u16, String> {
    if let Some(port) = requested {
        if port == 0 {
            return Err("proxy port 0 is not usable".to_string());
        }
        if used_ports.contains(&port) {
            return Err(format!(
                "proxy port {port} is already used by another proxy rule on this agent"
            ));
        }
        return Ok(port);
    }

    // Default window: service_port + 10000, clamped into the dynamic
    // range; scan forward while staying inside u16.
    let base = (service_port + 10000).clamp(1024, 65535) as u16;
    for offset in 0..100u16 {
        let candidate = base.saturating_add(offset);
        if candidate != 0 && !used_ports.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(format!(
        "no free proxy port in the {base}-{} range for this agent",
        base.saturating_add(99)
    ))
}

/// Decide the desired-state write for a toggle request against an
/// existing row (if any). Returns the command the agent must receive and
/// whether the row changes at all (idempotent repeats still re-publish so
/// a restarted agent converges, but the revision bump is skipped when the
/// desired state is identical).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredAction {
    /// Enable or re-enable: persist + publish ProxyAdd.
    Enable,
    /// Disable: persist + publish ProxyRemove.
    Disable,
    /// Requested state already stored: re-publish only, no revision bump.
    Republish,
}

pub fn desired_action(existing_enabled: Option<bool>, request_enabled: bool) -> DesiredAction {
    match existing_enabled {
        Some(true) if request_enabled => DesiredAction::Republish,
        Some(false) if !request_enabled => DesiredAction::Republish,
        Some(_) if request_enabled => DesiredAction::Enable,
        Some(_) => DesiredAction::Disable,
        None if request_enabled => DesiredAction::Enable,
        None => DesiredAction::Disable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_reported_state_is_required_for_proxy() {
        // A backend write alone must never claim Proxy.
        assert_eq!(
            capture_source(true, None, None),
            CaptureSource::Transitioning
        );
        assert_eq!(
            capture_source(true, Some(ReportedState::Effective), None),
            CaptureSource::Proxy
        );
    }

    #[test]
    fn enabled_without_report_splits_on_error() {
        assert_eq!(
            capture_source(true, None, Some("bind failed")),
            CaptureSource::ProxyUnavailable
        );
        assert_eq!(
            capture_source(true, None, None),
            CaptureSource::Transitioning
        );
    }

    #[test]
    fn reported_transitions_and_losses_map_explicitly() {
        assert_eq!(
            capture_source(true, Some(ReportedState::Transitioning), None),
            CaptureSource::Transitioning
        );
        assert_eq!(
            capture_source(true, Some(ReportedState::CaptureUnavailable), None),
            CaptureSource::ProxyUnavailable
        );
        assert_eq!(
            capture_source(true, Some(ReportedState::Failed), Some("nft missing")),
            CaptureSource::ProxyUnavailable
        );
    }

    #[test]
    fn disable_in_progress_is_transitioning_then_passive() {
        assert_eq!(
            capture_source(false, Some(ReportedState::Effective), None),
            CaptureSource::Transitioning
        );
        assert_eq!(
            capture_source(false, Some(ReportedState::Transitioning), None),
            CaptureSource::Transitioning
        );
        assert_eq!(capture_source(false, None, None), CaptureSource::Passive);
    }

    #[test]
    fn agent_reported_states_decode_and_unknown_is_none() {
        assert_eq!(
            ReportedState::from_wire("effective"),
            Some(ReportedState::Effective)
        );
        assert_eq!(ReportedState::from_wire("junk"), None);
    }

    #[test]
    fn explicit_port_rejects_collisions() {
        assert_eq!(
            allocate_proxy_port(80, &[10080, 10081], Some(10080)).is_err(),
            true
        );
        assert_eq!(allocate_proxy_port(80, &[10080], Some(10090)), Ok(10090));
        assert!(allocate_proxy_port(80, &[], Some(0)).is_err());
    }

    #[test]
    fn auto_allocation_scans_past_used_ports_and_fails_when_full() {
        assert_eq!(allocate_proxy_port(80, &[10080], None), Ok(10081));
        assert_eq!(allocate_proxy_port(80, &[], None), Ok(10080));
        let full: Vec<u16> = (10080..10180).collect();
        assert!(allocate_proxy_port(80, &full, None).is_err());
    }

    #[test]
    fn high_service_ports_clamp_into_valid_range() {
        let port = allocate_proxy_port(65535, &[], None).unwrap();
        assert!(port >= 1024);
    }

    #[test]
    fn desired_actions_are_idempotent() {
        assert_eq!(desired_action(None, true), DesiredAction::Enable);
        assert_eq!(desired_action(Some(false), true), DesiredAction::Enable);
        assert_eq!(desired_action(Some(true), false), DesiredAction::Disable);
        assert_eq!(desired_action(None, false), DesiredAction::Disable);
        assert_eq!(desired_action(Some(true), true), DesiredAction::Republish);
        assert_eq!(desired_action(Some(false), false), DesiredAction::Republish);
    }
}
