//! Supervised proxy transitions: a thin IO shell executing the ordered
//! plans from [`crate::proxy_state`] against the harpoon engine,
//! nftables, the sniffer's port set and the revision acker. One
//! transition runs at a time; desired state converges through retries
//! with bounded backoff. No transition ever restarts a data-plane
//! process: each rule owns its engine instance.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::{debug, info, warn};

use crate::harpoon::{HarpoonEngine, RuleRuntime};
use crate::nftables;
use crate::proxy_state::{
    self, ApplyKind, Cleanup, DisableStep, EnableStep, ProxyDesired, ProxyDiag, ProxyEffective,
    ProxyPhase, DISABLE_STEPS, ENABLE_STEPS, UPDATE_STEPS,
};
use crate::updates::{AppState, TransportProto};

/// How long a transition waits for the sniffer to confirm a revised
/// passive filter before the step is declared failed.
const EXCLUSION_CONFIRM: Duration = Duration::from_secs(3);
/// Passive-restore confirmation budget during disable (failure there is
/// tolerated: the key is already returned, the sniffer health shows the
/// capture gap).
const RESTORE_CONFIRM: Duration = Duration::from_secs(2);
/// Backoff before retrying a failed transition for the same service.
const RETRY_BACKOFF: Duration = Duration::from_secs(10);
/// A freshly effective rule is exempt from liveness probes while its
/// engine finishes binding.
const PROBE_GRACE: Duration = Duration::from_secs(10);
/// Repeated transition errors are logged at most this often per service.
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Rule identity data a transition works on, for diag bookkeeping.
struct TransitionSpec {
    desired_revision: u64,
    effective_revision: u64,
    protocol: TransportProto,
    original_port: u16,
    listen_port: u16,
}

/// Bookkeeping for desired/effective proxy state and transitions. Guarded
/// by a std mutex and never held across awaits.
#[derive(Default)]
struct ProxyLedger {
    desired: HashMap<u64, ProxyDesired>,
    effective: HashMap<u64, ProxyEffective>,
    /// Services whose original key is withheld from passive capture
    /// (effective rules plus rules mid-transition).
    excluded: HashSet<u64>,
    diag: HashMap<u64, ProxyDiag>,
    /// Services with a transition in progress.
    busy: HashSet<u64>,
    next_retry: HashMap<u64, Instant>,
    effective_since: HashMap<u64, Instant>,
}

pub struct ProxySupervisor {
    ledger: Mutex<ProxyLedger>,
    engine: Arc<tokio::sync::Mutex<HarpoonEngine>>,
    state: Arc<AppState>,
    tls_available: bool,
    /// False only in unit tests without nftables on the host.
    redirects_enabled: bool,
    wake_tx: tokio::sync::mpsc::UnboundedSender<()>,
    snapshot_revision: AtomicU64,
    last_error_log: Mutex<HashMap<u64, Instant>>,
}

/// A single service's proxy status for the heartbeat.
#[derive(Debug, Clone)]
pub struct ProxyStatusReport {
    pub service_id: u64,
    pub rule_name: String,
    pub desired_revision: u64,
    pub effective_revision: Option<u64>,
    /// "effective" | "transitioning" | "capture_unavailable" | "failed"
    pub state: &'static str,
    /// "tcp" | "udp"
    pub protocol: &'static str,
    pub original_port: u16,
    pub listen_port: u16,
    pub last_error: Option<String>,
}

fn spec_for_desired(desired: &ProxyDesired) -> TransitionSpec {
    TransitionSpec {
        desired_revision: desired.revision,
        effective_revision: 0,
        protocol: desired.protocol,
        original_port: desired.original_port,
        listen_port: desired.listen.port(),
    }
}

fn spec_for_effective(effective: &ProxyEffective) -> TransitionSpec {
    TransitionSpec {
        desired_revision: effective.revision,
        effective_revision: effective.revision,
        protocol: effective.protocol,
        original_port: effective.original_port,
        listen_port: effective.listen_port,
    }
}

impl ProxySupervisor {
    pub fn new(
        engine: Arc<tokio::sync::Mutex<HarpoonEngine>>,
        state: Arc<AppState>,
        tls_available: bool,
    ) -> (Arc<Self>, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (wake_tx, wake_rx) = tokio::sync::mpsc::unbounded_channel();
        let supervisor = Arc::new(Self {
            ledger: Mutex::new(ProxyLedger::default()),
            engine,
            state,
            tls_available,
            redirects_enabled: true,
            wake_tx,
            snapshot_revision: AtomicU64::new(0),
            last_error_log: Mutex::new(HashMap::new()),
        });
        (supervisor, wake_rx)
    }

    /// Test construction: no nftables required, no redirects installed.
    #[cfg(test)]
    pub fn for_tests() -> Arc<Self> {
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let state = crate::updates::test_state();
        Self::with_parts(engine, state, true, false)
    }

    /// Test-support: build with explicit parts.
    #[cfg(test)]
    pub fn with_parts(
        engine: Arc<tokio::sync::Mutex<HarpoonEngine>>,
        state: Arc<AppState>,
        tls_available: bool,
        redirects_enabled: bool,
    ) -> Arc<Self> {
        let (wake_tx, _wake_rx) = tokio::sync::mpsc::unbounded_channel();
        Arc::new(Self {
            ledger: Mutex::new(ProxyLedger::default()),
            engine,
            state,
            tls_available,
            redirects_enabled,
            wake_tx,
            snapshot_revision: AtomicU64::new(0),
            last_error_log: Mutex::new(HashMap::new()),
        })
    }

    /// Test-support: pretend a rule became effective long enough ago that
    /// the recovery sweep will probe it.
    #[cfg(test)]
    pub fn test_backdate_effective(&self, sid: u64) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger
            .effective_since
            .insert(sid, Instant::now() - PROBE_GRACE * 2);
    }

    pub fn wake(&self) {
        let _ = self.wake_tx.send(());
    }

    /// Publish a new desired map (complete replacement semantics from a
    /// snapshot) and report how many transitions are needed to converge.
    /// Desired proxies for unassigned services are dropped: a proxy
    /// without its service key cannot be enforced or attributed.
    pub fn set_desired(&self, desired: HashMap<u64, ProxyDesired>, revision: u64) -> usize {
        let mut ledger = self.ledger.lock().unwrap();
        let services = self.state.services.read().unwrap();
        let filtered: HashMap<u64, ProxyDesired> = desired
            .into_iter()
            .filter(|(sid, d)| {
                if services.contains_key(sid) {
                    true
                } else {
                    debug!(
                        "Dropping desired proxy '{}' for unassigned service {}",
                        d.name, sid
                    );
                    false
                }
            })
            .collect();
        drop(services);

        ledger.desired = filtered;
        let desired_keys: HashSet<u64> = ledger.desired.keys().copied().collect();
        let effective_keys: HashSet<u64> = ledger.effective.keys().copied().collect();

        // Freshly desired services are excluded from passive capture
        // IMMEDIATELY, before any transition runs: a fresh start or a
        // ConfigSync must never briefly expose a proxied original to the
        // passive sniffer. An enable that previously FAILED keeps passive
        // capture (spec: restore passive when appropriate) - its retry
        // re-excludes at the ExcludePassive step.
        for sid in &desired_keys {
            let failed = ledger
                .diag
                .get(sid)
                .map(|d| d.phase == ProxyPhase::Failed)
                .unwrap_or(false);
            if !effective_keys.contains(sid) && !failed {
                ledger.excluded.insert(*sid);
            }
        }

        ledger
            .diag
            .retain(|sid, _| desired_keys.contains(sid) || effective_keys.contains(sid));
        ledger
            .next_retry
            .retain(|sid, _| desired_keys.contains(sid));
        ledger
            .effective_since
            .retain(|sid, _| effective_keys.contains(sid));

        self.snapshot_revision.store(revision, Ordering::SeqCst);

        let pending: HashSet<u64> = ledger
            .desired
            .keys()
            .chain(ledger.effective.keys())
            .copied()
            .collect();
        pending
            .iter()
            .filter(|sid| {
                proxy_state::desired_needs_apply(
                    ledger.desired.get(*sid),
                    ledger.effective.get(*sid),
                ) != ApplyKind::None
            })
            .count()
    }

    /// Command-path desired updates (ProxyAdd/ProxyRemove with a service
    /// id): same ledger, no snapshot revision. Freshly desired services
    /// are excluded from passive capture immediately (same boot
    /// guarantee as set_desired).
    pub fn command_set_desired(&self, desired: ProxyDesired) {
        {
            let mut ledger = self.ledger.lock().unwrap();
            let failed = ledger
                .diag
                .get(&desired.service_id)
                .map(|d| d.phase == ProxyPhase::Failed)
                .unwrap_or(false);
            if !ledger.effective.contains_key(&desired.service_id) && !failed {
                ledger.excluded.insert(desired.service_id);
            }
            ledger.desired.insert(desired.service_id, desired);
        }
        self.wake();
    }

    pub fn command_remove_desired(&self, service_id: u64) {
        {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.desired.remove(&service_id);
        }
        self.wake();
    }

    /// Current exclusion set (services whose passive key is withheld).
    pub fn excluded_snapshot(&self) -> HashSet<u64> {
        self.ledger.lock().unwrap().excluded.clone()
    }

    /// Per-service proxy status for the heartbeat.
    pub fn status_reports(&self) -> Vec<ProxyStatusReport> {
        let ledger = self.ledger.lock().unwrap();
        let mut ids: Vec<u64> = ledger
            .desired
            .keys()
            .chain(ledger.effective.keys())
            .copied()
            .collect();
        ids.sort_unstable();
        ids.dedup();

        ids.into_iter()
            .filter_map(|sid| {
                let diag = ledger.diag.get(&sid)?;
                let name = ledger
                    .desired
                    .get(&sid)
                    .map(|d| d.name.clone())
                    .or_else(|| ledger.effective.get(&sid).map(|e| e.name.clone()))?;
                Some(ProxyStatusReport {
                    service_id: sid,
                    rule_name: name,
                    desired_revision: diag.desired_revision,
                    effective_revision: if diag.phase == ProxyPhase::Effective {
                        Some(diag.effective_revision)
                    } else {
                        None
                    },
                    state: diag.phase.as_str(),
                    protocol: diag.protocol.as_str(),
                    original_port: diag.original_port,
                    listen_port: diag.listen_port,
                    last_error: diag.last_error.clone(),
                })
            })
            .collect()
    }

    fn set_diag_transitioning(&self, sid: u64, spec: &TransitionSpec) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.diag.insert(
            sid,
            ProxyDiag {
                phase: ProxyPhase::Transitioning,
                desired_revision: spec.desired_revision,
                effective_revision: spec.effective_revision,
                last_error: None,
                protocol: spec.protocol,
                original_port: spec.original_port,
                listen_port: spec.listen_port,
            },
        );
    }

    fn log_error_rate_limited(&self, sid: u64, context: &str, error: &str) {
        let mut logs = self.last_error_log.lock().unwrap();
        let now = Instant::now();
        if logs
            .get(&sid)
            .map(|t| now.duration_since(*t) < ERROR_LOG_INTERVAL)
            .unwrap_or(false)
        {
            return;
        }
        logs.insert(sid, now);
        warn!(
            "Proxy transition failed for service {} ({}): {}",
            sid, context, error
        );
    }

    /// Pick and lock the next transition to run. Returns None when
    /// everything converged (or is in backoff).
    fn next_apply(&self) -> Option<(u64, ApplyKind)> {
        let now = Instant::now();
        let mut ledger = self.ledger.lock().unwrap();
        let mut ids: Vec<u64> = ledger
            .desired
            .keys()
            .chain(ledger.effective.keys())
            .copied()
            .collect();
        ids.sort_unstable();
        ids.dedup();

        for sid in ids {
            if ledger.busy.contains(&sid) {
                continue;
            }
            if ledger
                .next_retry
                .get(&sid)
                .map(|t| *t > now)
                .unwrap_or(false)
            {
                continue;
            }
            let kind = proxy_state::desired_needs_apply(
                ledger.desired.get(&sid),
                ledger.effective.get(&sid),
            );
            if kind == ApplyKind::None {
                continue;
            }
            ledger.busy.insert(sid);
            return Some((sid, kind));
        }
        None
    }

    fn release(&self, sid: u64) {
        self.ledger.lock().unwrap().busy.remove(&sid);
    }

    /// Run one supervisor pass: recovery sweep, then every ready
    /// transition in service-id order. Public for tests; the spawned task
    /// loops it.
    pub async fn tick_once(&self) {
        self.recovery_sweep().await;
        while let Some((sid, kind)) = self.next_apply() {
            match kind {
                ApplyKind::None => self.release(sid),
                ApplyKind::Enable | ApplyKind::Update => self.run_enable(sid, kind).await,
                ApplyKind::Disable => self.run_disable(sid).await,
            }
        }
    }

    /// Enable or update. Step order and rollback come from the pure plan
    /// in proxy_state.
    async fn run_enable(&self, sid: u64, kind: ApplyKind) {
        let (desired, old_effective) = {
            let ledger = self.ledger.lock().unwrap();
            (
                ledger.desired.get(&sid).cloned(),
                ledger.effective.get(&sid).cloned(),
            )
        };
        let Some(desired) = desired else {
            self.release(sid);
            return;
        };
        let steps = if kind == ApplyKind::Update {
            UPDATE_STEPS
        } else {
            ENABLE_STEPS
        };

        self.set_diag_transitioning(sid, &spec_for_desired(&desired));

        let nft_ok = !self.redirects_enabled || nftables::nft_binary_available();
        if let Err(err) = proxy_state::validate_desired(&desired, self.tls_available, nft_ok) {
            self.enable_failed(sid, &desired, kind, &[], err, &mut None)
                .await;
            return;
        }

        let mut done: Vec<EnableStep> = Vec::new();
        let mut local_runtime: Option<RuleRuntime> = None;
        for &step in steps {
            match self
                .enable_step(
                    step,
                    sid,
                    &desired,
                    old_effective.as_ref(),
                    &mut local_runtime,
                )
                .await
            {
                Ok(()) => done.push(step),
                Err(err) => {
                    self.log_error_rate_limited(sid, "enable", &err);
                    self.enable_failed(sid, &desired, kind, &done, err, &mut local_runtime)
                        .await;
                    return;
                }
            }
        }

        let revision = desired.revision;
        {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.effective_since.insert(sid, Instant::now());
            if let Some(diag) = ledger.diag.get_mut(&sid) {
                diag.phase = ProxyPhase::Effective;
                diag.effective_revision = revision;
                diag.desired_revision = revision;
                diag.last_error = None;
            }
        }
        self.state.acker.proxy_done(revision);
        info!(
            "Proxy for service {} is effective at revision {} (rule '{}')",
            sid, revision, desired.name
        );
        self.release(sid);
    }

    async fn enable_step(
        &self,
        step: EnableStep,
        sid: u64,
        desired: &ProxyDesired,
        old_effective: Option<&ProxyEffective>,
        local_runtime: &mut Option<RuleRuntime>,
    ) -> Result<(), String> {
        match step {
            EnableStep::PrepareRule => {
                let spec = proxy_state::spec_from_desired(desired);
                let handle = self
                    .engine
                    .lock()
                    .await
                    .spawn_engine_for_spec(&spec)
                    .await
                    .map_err(|e| format!("prepare rule '{}': {}", desired.name, e))?;
                *local_runtime = Some(RuleRuntime { spec, handle });
                Ok(())
            }
            EnableStep::ExcludePassive => {
                {
                    let mut ledger = self.ledger.lock().unwrap();
                    ledger.excluded.insert(sid);
                }
                let excluded = self.excluded_snapshot();
                let (passive, generation) = recompute_passive_ports(&self.state, &excluded);
                let _ = passive;
                wait_sniffer_applied(&self.state, generation, EXCLUSION_CONFIRM).await
            }
            EnableStep::ActivateCapture => self
                .engine
                .lock()
                .await
                .start_capture_session(&desired.name)
                .map_err(|e| format!("start capture '{}': {}", desired.name, e)),
            EnableStep::InstallRedirect => {
                if !self.redirects_enabled {
                    return Ok(());
                }
                let spec = proxy_state::spec_from_desired(desired);
                self.engine
                    .lock()
                    .await
                    .install_redirect(&spec)
                    .map_err(|e| {
                        format!(
                            "install redirect for {} {}: {}",
                            desired.protocol.as_str(),
                            desired.original_port,
                            e
                        )
                    })
            }
            EnableStep::MarkEffective => {
                let Some(runtime) = local_runtime.take() else {
                    return Err("transition lost its prepared rule runtime".to_string());
                };
                let effective = ProxyEffective {
                    service_id: sid,
                    revision: desired.revision,
                    name: desired.name.clone(),
                    protocol: desired.protocol,
                    original_port: desired.original_port,
                    listen_port: desired.listen.port(),
                };
                {
                    let mut ledger = self.ledger.lock().unwrap();
                    ledger.effective.insert(sid, effective);
                    ledger.excluded.insert(sid);
                    ledger.next_retry.remove(&sid);
                }
                self.engine
                    .lock()
                    .await
                    .register_service_runtime(sid, runtime);
                Ok(())
            }
            // Update-only steps.
            EnableStep::UninstallRedirect => {
                if !self.redirects_enabled {
                    return Ok(());
                }
                if let Some(old) = old_effective {
                    let transport = old.protocol;
                    let port = old.original_port;
                    self.engine
                        .lock()
                        .await
                        .remove_redirect(transport, port)
                        .map_err(|e| {
                            format!(
                                "remove old redirect for {} {}: {}",
                                transport.as_str(),
                                port,
                                e
                            )
                        })?;
                }
                Ok(())
            }
            EnableStep::RetireRule => {
                let retired = self.engine.lock().await.take_service_runtime(sid);
                if let Some(old) = retired {
                    let name = old.spec.name.clone();
                    self.engine.lock().await.stop_capture_session(&name);
                    old.shutdown().await;
                }
                Ok(())
            }
        }
    }

    /// Roll back a failed enable/update and report the failed revision.
    /// An update keeps the original key excluded: the old rule is already
    /// retired, so passive capture must not resume while the desired state
    /// still demands a proxy (the supervisor retries with backoff).
    async fn enable_failed(
        &self,
        sid: u64,
        desired: &ProxyDesired,
        kind: ApplyKind,
        done: &[EnableStep],
        error: String,
        local_runtime: &mut Option<RuleRuntime>,
    ) {
        for cleanup in proxy_state::enable_rollback(done) {
            if cleanup == Cleanup::RestorePassive && kind == ApplyKind::Update {
                continue;
            }
            self.cleanup(cleanup, desired, local_runtime).await;
        }
        // A fresh enable that failed before reaching the exclusion step
        // must undo the pre-inserted boot exclusion: the service stays
        // passively captured until the retry succeeds (spec: restore
        // passive capture when appropriate).
        if kind == ApplyKind::Enable && !done.contains(&EnableStep::ExcludePassive) {
            {
                let mut ledger = self.ledger.lock().unwrap();
                ledger.excluded.remove(&sid);
            }
            let excluded = self.excluded_snapshot();
            recompute_passive_ports(&self.state, &excluded);
        }
        {
            let mut ledger = self.ledger.lock().unwrap();
            if let Some(diag) = ledger.diag.get_mut(&sid) {
                diag.phase = ProxyPhase::Failed;
                diag.last_error = Some(error.clone());
            }
            ledger
                .next_retry
                .insert(sid, Instant::now() + RETRY_BACKOFF);
        }
        self.state.acker.proxy_failed(desired.revision, &error);
        self.release(sid);
    }

    async fn cleanup(
        &self,
        action: Cleanup,
        desired: &ProxyDesired,
        local_runtime: &mut Option<RuleRuntime>,
    ) {
        match action {
            Cleanup::RemoveRedirect => {
                if self.redirects_enabled {
                    let mut engine = self.engine.lock().await;
                    let _ = engine.remove_redirect(desired.protocol, desired.original_port);
                }
            }
            Cleanup::StopRuleSession => {
                self.engine.lock().await.stop_capture_session(&desired.name);
            }
            Cleanup::StopRuleEngine => {
                let runtime = match local_runtime.take() {
                    Some(runtime) => Some(runtime),
                    None => self
                        .engine
                        .lock()
                        .await
                        .take_service_runtime(desired.service_id),
                };
                if let Some(runtime) = runtime {
                    runtime.shutdown().await;
                }
            }
            Cleanup::RestorePassive => {
                {
                    let mut ledger = self.ledger.lock().unwrap();
                    ledger.excluded.remove(&desired.service_id);
                }
                let excluded = self.excluded_snapshot();
                recompute_passive_ports(&self.state, &excluded);
            }
        }
    }

    /// Disable per the spec order: not-effective first (reconciliation
    /// cannot revive), redirect off, publication drained and stopped, only
    /// then the original key returns to passive capture.
    async fn run_disable(&self, sid: u64) {
        let effective = {
            let ledger = self.ledger.lock().unwrap();
            ledger.effective.get(&sid).cloned()
        };
        let Some(effective) = effective else {
            self.release(sid);
            return;
        };
        self.set_diag_transitioning(sid, &spec_for_effective(&effective));

        let mut done: Vec<DisableStep> = Vec::new();
        for &step in DISABLE_STEPS {
            if let Err(err) = self.disable_step(step, sid, &effective).await {
                self.log_error_rate_limited(sid, "disable", &err);
                // Passive stays excluded until the agent can guarantee the
                // proxy stopped publishing; the rollback plan is empty by
                // design, so nothing is undone on failure.
                for cleanup in proxy_state::disable_rollback(&done) {
                    let _ = cleanup;
                }
                {
                    let mut ledger = self.ledger.lock().unwrap();
                    if let Some(diag) = ledger.diag.get_mut(&sid) {
                        diag.phase = ProxyPhase::Failed;
                        diag.last_error = Some(err.clone());
                    }
                    ledger
                        .next_retry
                        .insert(sid, Instant::now() + RETRY_BACKOFF);
                }
                self.state.acker.proxy_failed(0, &err);
                self.release(sid);
                return;
            }
            done.push(step);
        }
        self.release(sid);
        info!(
            "Proxy for service {} disabled; passive capture resumed for the original key",
            sid
        );
    }

    async fn disable_step(
        &self,
        step: DisableStep,
        sid: u64,
        effective: &ProxyEffective,
    ) -> Result<(), String> {
        match step {
            DisableStep::MarkNotEffective => {
                let mut ledger = self.ledger.lock().unwrap();
                ledger.effective.remove(&sid);
                ledger.effective_since.remove(&sid);
                if let Some(diag) = ledger.diag.get_mut(&sid) {
                    diag.phase = ProxyPhase::Transitioning;
                    diag.effective_revision = 0;
                }
                Ok(())
            }
            DisableStep::UninstallRedirect => {
                if !self.redirects_enabled {
                    return Ok(());
                }
                // Must succeed before publication stops: a surviving
                // redirect into a dead proxy port would blackhole service
                // traffic.
                let transport = effective.protocol;
                let port = effective.original_port;
                self.engine
                    .lock()
                    .await
                    .remove_redirect(transport, port)
                    .map_err(|e| {
                        format!("remove redirect for {} {}: {}", transport.as_str(), port, e)
                    })
            }
            DisableStep::StopPublication => {
                let runtime = self
                    .engine
                    .lock()
                    .await
                    .take_service_runtime(sid)
                    .ok_or_else(|| format!("rule runtime for service {} already gone", sid))?;
                let name = runtime.spec.name.clone();
                self.engine.lock().await.stop_capture_session(&name);
                // Bounded drain of existing redirected sessions inside
                // shutdown; after this no session for the rule can publish.
                runtime.shutdown().await;
                Ok(())
            }
            DisableStep::RestorePassive => {
                {
                    let mut ledger = self.ledger.lock().unwrap();
                    ledger.excluded.remove(&sid);
                    ledger.diag.remove(&sid);
                    ledger.next_retry.remove(&sid);
                }
                let excluded = self.excluded_snapshot();
                let (passive, generation) = recompute_passive_ports(&self.state, &excluded);
                let _ = passive;
                // The key is already returned; a confirmation timeout only
                // means the sniffer is unhealthy (visible via health), not
                // that the proxy is still publishing.
                if wait_sniffer_applied(&self.state, generation, RESTORE_CONFIRM)
                    .await
                    .is_err()
                {
                    warn!(
                        "Passive restore for service {} not confirmed by the sniffer in time",
                        sid
                    );
                }
                Ok(())
            }
            DisableStep::Ack => {
                self.state
                    .acker
                    .proxy_done(self.snapshot_revision.load(Ordering::SeqCst));
                Ok(())
            }
        }
    }

    /// Liveness sweep over effective rules: a dead engine is restarted in
    /// place. Passive capture is NEVER re-enabled here - the proxy remains
    /// the authoritative capture source while it is desired, and a lost
    /// capture source is reported instead.
    async fn recovery_sweep(&self) {
        let now = Instant::now();
        let candidates: Vec<(u64, ProxyEffective, ProxyDesired)> = {
            let ledger = self.ledger.lock().unwrap();
            ledger
                .effective
                .iter()
                .filter(|(sid, _)| {
                    !ledger.busy.contains(sid)
                        && ledger
                            .effective_since
                            .get(sid)
                            .map(|t| now.duration_since(*t) > PROBE_GRACE)
                            .unwrap_or(false)
                })
                .filter_map(|(sid, e)| {
                    ledger
                        .desired
                        .get(sid)
                        .cloned()
                        .map(|d| (*sid, e.clone(), d))
                })
                .collect()
        };

        for (sid, effective, desired) in candidates {
            if listener_alive(desired.protocol, desired.listen) {
                continue;
            }
            self.log_error_rate_limited(
                sid,
                "recovery",
                "harpoon rule engine died while proxy is desired",
            );
            {
                let mut ledger = self.ledger.lock().unwrap();
                if let Some(diag) = ledger.diag.get_mut(&sid) {
                    diag.phase = ProxyPhase::CaptureUnavailable;
                    diag.last_error = Some("proxy capture source lost, recovering".to_string());
                }
            }

            // Rebuild the engine for this rule only; unrelated rules and
            // the exclusion set stay untouched.
            let old = self.engine.lock().await.take_service_runtime(sid);
            if let Some(old) = old {
                let name = old.spec.name.clone();
                self.engine.lock().await.stop_capture_session(&name);
                old.shutdown().await;
            }

            let spec = proxy_state::spec_from_desired(&desired);
            let spawned = {
                let mut engine = self.engine.lock().await;
                match engine.spawn_engine_for_spec(&spec).await {
                    Ok(handle) => {
                        // A crashed engine leaves its capture session
                        // behind; reset it before the replacement starts
                        // publishing (no traffic flows in between).
                        engine.stop_capture_session(&desired.name);
                        if let Err(e) = engine.start_capture_session(&desired.name) {
                            Err(format!("restart capture '{}': {}", desired.name, e))
                        } else {
                            engine.register_service_runtime(sid, RuleRuntime { spec, handle });
                            Ok(())
                        }
                    }
                    Err(e) => Err(format!("restart engine '{}': {}", desired.name, e)),
                }
            };
            match spawned {
                Ok(()) => {
                    info!("Recovered proxy rule for service {} after engine loss", sid);
                    let mut ledger = self.ledger.lock().unwrap();
                    ledger.effective_since.insert(sid, Instant::now());
                    if let Some(diag) = ledger.diag.get_mut(&sid) {
                        diag.phase = ProxyPhase::Effective;
                        diag.effective_revision = effective.revision;
                        diag.last_error = None;
                    }
                }
                Err(e) => {
                    warn!(
                        "Proxy recovery for service {} failed (will retry): {}",
                        sid, e
                    );
                }
            }
        }
    }
}

/// True when a listener of the given protocol is bound on `addr` (probe
/// bind fails with the port in use). A successful probe bind means the
/// engine's listener is gone; the probe socket is dropped immediately.
fn listener_alive(protocol: TransportProto, addr: SocketAddr) -> bool {
    match protocol {
        TransportProto::Tcp => std::net::TcpListener::bind(addr).is_err(),
        TransportProto::Udp => std::net::UdpSocket::bind(addr).is_err(),
    }
}

/// Recompute passive capture keys from assigned services minus the
/// excluded (proxied/transitioning) originals, swap them in atomically and
/// notify the sniffer. Returns the new passive set plus the new
/// generation. This is the ONLY writer of the passive port set: ConfigSync,
/// proxy transitions and rollback all funnel through it, so an excluded
/// original can never be re-added by a stale writer.
pub fn recompute_passive_ports(
    state: &AppState,
    excluded: &HashSet<u64>,
) -> (HashSet<(TransportProto, u16)>, u64) {
    let services = state.services.read().unwrap().clone();
    let passive = proxy_state::passive_ports(&services, excluded);
    let excluded_keys = proxy_state::excluded_keys(&services, excluded);
    {
        let mut ports = state.ports.write().unwrap();
        *ports = passive.clone();
        *state.excluded_ports.write().unwrap() = excluded_keys;
    }
    let generation = state.ports_generation.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = state.restart_tx.send(());
    (passive, generation)
}

/// Wait until the sniffer reports having applied the port set of
/// `generation` (or passive capture is intentionally off). Bounded: a
/// transition must not wait forever on an unhealthy sniffer.
async fn wait_sniffer_applied(
    state: &AppState,
    generation: u64,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        if state.health.applied_generation() >= generation {
            return Ok(());
        }
        if state.health.idle_no_services() && state.ports.read().unwrap().is_empty() {
            // Passive capture intentionally off: trivially applied.
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "sniffer did not confirm passive filter generation {} in time",
                generation
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Spawn the supervisor loop: recovery sweep + converge transitions,
/// woken by desired-state changes and a 1s tick (retries, sweeps).
pub fn spawn_proxy_supervisor(
    engine: Arc<tokio::sync::Mutex<HarpoonEngine>>,
    state: Arc<AppState>,
    tls_available: bool,
) -> Arc<ProxySupervisor> {
    let (supervisor, mut wake_rx) = ProxySupervisor::new(engine, state, tls_available);
    let loop_supervisor = supervisor.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = wake_rx.recv() => {}
                _ = tick.tick() => {}
            }
            loop_supervisor.tick_once().await;
        }
    });
    supervisor
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harpoon::ProxyRuleSpec;
    use crate::updates::{test_state, TransportProto};
    use crate::wire::{
        WireAgentConfigSnapshot, WireAgentNetworkSettings, WireProxyRule, WireService,
    };
    use harpoon_core::types::endpoint::Protocol;
    use std::collections::HashSet;

    fn tcp_desired(sid: u64, listen_port: u16, original_port: u16, revision: u64) -> ProxyDesired {
        ProxyDesired {
            service_id: sid,
            name: format!("protect-{sid}-svc"),
            listen: format!("127.0.0.1:{listen_port}").parse().unwrap(),
            target: format!("127.0.0.1:{original_port}").parse().unwrap(),
            protocol: TransportProto::Tcp,
            original_port,
            tls_mode: None,
            http2: false,
            revision,
        }
    }

    fn spec_of(desired: &ProxyDesired) -> ProxyRuleSpec {
        proxy_state::spec_from_desired(desired)
    }

    async fn free_port() -> u16 {
        let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        tmp.local_addr().unwrap().port()
    }

    fn assigned(
        services: &[(u64, TransportProto, u16)],
    ) -> HashMap<u64, crate::updates::AssignedService> {
        services
            .iter()
            .map(|(id, proto, port)| {
                (
                    *id,
                    crate::updates::AssignedService {
                        protocol: *proto,
                        port: *port,
                    },
                )
            })
            .collect()
    }

    fn passive(state: &AppState) -> HashSet<(TransportProto, u16)> {
        state.ports.read().unwrap().clone()
    }

    /// Stand-in for the real capture thread: applies port-set generations
    /// so transition confirmations succeed without an AF_PACKET device.
    fn spawn_sniffer_stub(state: Arc<AppState>) {
        let mut restart_rx = state.restart_tx.subscribe();
        let health = state.health.clone();
        let generation = state.ports_generation.clone();
        let ports = state.ports.clone();
        tokio::spawn(async move {
            loop {
                if restart_rx.changed().await.is_err() {
                    break;
                }
                // Mimic the bounded refresh delay of the real thread.
                tokio::time::sleep(Duration::from_millis(5)).await;
                if ports.read().unwrap().is_empty() {
                    health.set_idle_no_services(true);
                } else {
                    health.set_idle_no_services(false);
                }
                health.set_applied_generation_test_only(generation.load(Ordering::SeqCst));
            }
        });
    }

    /// Publish an initial passive set for the assigned services (what the
    /// first snapshot would produce).
    fn seed_passive(state: &AppState, supervisor: &ProxySupervisor) {
        let excluded = supervisor.excluded_snapshot();
        recompute_passive_ports(state, &excluded);
    }

    /// Enable: the harpoon runtime comes up, the original key leaves the
    /// passive set, unrelated keys keep capturing, and the diag reports
    /// effective with the desired revision.
    #[tokio::test]
    async fn enable_excludes_original_key_and_reports_effective() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[
            (1, TransportProto::Tcp, 80),
            (3, TransportProto::Tcp, 8080),
            (4, TransportProto::Udp, 80),
        ]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());

        let listen = free_port().await;
        let desired = tcp_desired(1, listen, 80, 5);
        assert_eq!(
            supervisor.set_desired(HashMap::from([(1u64, desired)]), 5),
            1
        );

        // Boot-order guarantee: the desired proxy is excluded from passive
        // capture before the first recompute. Ports were empty; they now
        // fill with the passive keys only, never with the proxied original.
        let excluded = supervisor.excluded_snapshot();
        let (ports, _gen) = recompute_passive_ports(&state, &excluded);
        assert_eq!(ports.len(), 2, "tcp/8080 and udp/80 stay passive");
        assert!(ports.contains(&(TransportProto::Tcp, 8080)));
        assert!(ports.contains(&(TransportProto::Udp, 80)));
        assert!(!ports.contains(&(TransportProto::Tcp, 80)));

        supervisor.tick_once().await;

        assert_eq!(passive(&state).len(), 2);
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert!(engine.lock().await.service_spec(1).is_some());
        assert_eq!(engine.lock().await.active_rule_names().len(), 1);
        let reports = supervisor.status_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, "effective");
        assert_eq!(reports[0].effective_revision, Some(5));
        assert_eq!(reports[0].original_port, 80);

        engine.lock().await.shutdown().await;
    }

    /// Same-service re-apply is idempotent: no second rule, no second
    /// capture session, no second forwarder (the forwarder is global).
    #[tokio::test]
    async fn repeated_enable_is_idempotent() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        let desired = tcp_desired(1, listen, 80, 5);
        supervisor.set_desired(HashMap::from([(1u64, desired.clone())]), 5);
        supervisor.tick_once().await;
        let sessions_after_first = engine.lock().await.capture_manager().list_sessions().len();

        // Redelivered desired state (same revision) converges to nothing.
        supervisor.set_desired(HashMap::from([(1u64, desired)]), 5);
        supervisor.tick_once().await;
        supervisor.tick_once().await;

        assert_eq!(engine.lock().await.active_rule_names().len(), 1);
        assert_eq!(
            engine.lock().await.capture_manager().list_sessions().len(),
            sessions_after_first
        );
        assert_eq!(supervisor.status_reports()[0].state, "effective");

        engine.lock().await.shutdown().await;
    }

    /// Disable: publication stops first, then the original key returns to
    /// passive capture; diag is cleared so the service reports passive.
    #[tokio::test]
    async fn disable_restores_passive_after_publication_stops() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        supervisor.set_desired(HashMap::from([(1u64, tcp_desired(1, listen, 80, 5))]), 5);
        supervisor.tick_once().await;
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));

        supervisor.command_remove_desired(1);
        supervisor.tick_once().await;

        assert!(passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert!(engine.lock().await.service_spec(1).is_none());
        assert!(engine.lock().await.active_rule_names().is_empty());
        assert!(supervisor.status_reports().is_empty());

        engine.lock().await.shutdown().await;
    }

    /// TCP and UDP on one numeric port are independent capture sources.
    #[tokio::test]
    async fn tcp_and_udp_same_port_are_independent() {
        let state = test_state();
        *state.services.write().unwrap() =
            assigned(&[(1, TransportProto::Tcp, 53), (2, TransportProto::Udp, 53)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        let mut desired = tcp_desired(1, listen, 53, 3);
        desired.name = "protect-1-dns-tcp".into();
        supervisor.set_desired(HashMap::from([(1u64, desired)]), 3);
        supervisor.tick_once().await;

        let passive = passive(&state);
        assert!(!passive.contains(&(TransportProto::Tcp, 53)));
        assert!(passive.contains(&(TransportProto::Udp, 53)));

        engine.lock().await.shutdown().await;
    }

    /// Full snapshot path: a ConfigSync recompute cannot re-add an
    /// excluded original while the proxy is effective, and a snapshot
    /// without the proxy disables it.
    #[tokio::test]
    async fn configsync_cannot_readd_excluded_original() {
        let state = test_state();
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());

        let listen = free_port().await;
        let snap = WireAgentConfigSnapshot {
            revision: 5,
            agent_id: "agent-a".into(),
            network: Some(WireAgentNetworkSettings {
                interface: "eth0".into(),
                machine_ip: "0.0.0.0".into(),
            }),
            services: vec![
                WireService {
                    name: "web".into(),
                    port: 80,
                    id: 1,
                    ..Default::default()
                },
                WireService {
                    name: "web-alt".into(),
                    port: 8080,
                    id: 2,
                    ..Default::default()
                },
            ],
            desired_proxies: vec![WireProxyRule {
                service_id: 1,
                name: "protect-1-web".into(),
                listen_address: "127.0.0.1".into(),
                listen_port: listen as u32,
                target_address: "127.0.0.1".into(),
                target_port: 80,
                protocol: harpoon_core::types::endpoint::Protocol::Tcp as i32,
                tls_mode: String::new(),
                http2_enabled: false,
                revision: 5,
            }],
            filters: vec![],
            blocks: vec![],
        };

        crate::updates::apply_snapshot(&snap, &state, &supervisor);
        supervisor.tick_once().await;
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));

        // Re-deliver the same services WITH the proxy: exclusion must hold.
        crate::updates::apply_snapshot(&snap, &state, &supervisor);
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));

        // A snapshot revoking the proxy disables it and passive resumes.
        let mut revoke = snap.clone();
        revoke.revision = 6;
        revoke.desired_proxies = vec![];
        crate::updates::apply_snapshot(&revoke, &state, &supervisor);
        supervisor.tick_once().await;

        assert!(passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert!(engine.lock().await.service_spec(1).is_none());

        engine.lock().await.shutdown().await;
    }

    /// A stale snapshot cannot revive a removed proxy.
    #[tokio::test]
    async fn stale_snapshot_cannot_revive_removed_proxy() {
        let state = test_state();
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());

        let listen = free_port().await;
        let snap = WireAgentConfigSnapshot {
            revision: 5,
            agent_id: "agent-a".into(),
            network: None,
            services: vec![WireService {
                name: "web".into(),
                port: 80,
                id: 1,
                ..Default::default()
            }],
            desired_proxies: vec![WireProxyRule {
                service_id: 1,
                name: "protect-1-web".into(),
                listen_address: "127.0.0.1".into(),
                listen_port: listen as u32,
                target_address: "127.0.0.1".into(),
                target_port: 80,
                protocol: Protocol::Tcp as i32,
                tls_mode: String::new(),
                http2_enabled: false,
                revision: 5,
            }],
            filters: vec![],
            blocks: vec![],
        };
        crate::updates::apply_snapshot(&snap, &state, &supervisor);
        supervisor.tick_once().await;
        assert!(engine.lock().await.service_spec(1).is_some());

        let mut revoked = snap.clone();
        revoked.revision = 6;
        revoked.desired_proxies = vec![];
        crate::updates::apply_snapshot(&revoked, &state, &supervisor);
        supervisor.tick_once().await;
        assert!(engine.lock().await.service_spec(1).is_none());

        // Replay the OLD snapshot (with the proxy): stale, ignored.
        crate::updates::apply_snapshot(&snap, &state, &supervisor);
        supervisor.tick_once().await;
        assert!(engine.lock().await.service_spec(1).is_none());
        assert!(passive(&state).contains(&(TransportProto::Tcp, 80)));

        engine.lock().await.shutdown().await;
    }

    /// A failed validate/prepare leaves the old state authoritative:
    /// passive keys unchanged, no effective rule, failure reported, and
    /// the boot exclusion is undone so passive capture actually runs.
    #[tokio::test]
    async fn prepare_failure_keeps_old_state() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        // TLS MITM requested but the agent has no CA certs: validation
        // fails before any state change.
        let mut desired = tcp_desired(1, free_port().await, 80, 5);
        desired.tls_mode = Some("mitm".into());
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), false, false);
        supervisor.set_desired(HashMap::from([(1u64, desired)]), 5);
        supervisor.tick_once().await;

        // Failure happened BEFORE exclusion: passive still covers tcp/80.
        assert!(passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert_eq!(supervisor.excluded_snapshot().len(), 0);
        assert!(engine.lock().await.service_spec(1).is_none());
        let reports = supervisor.status_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].state, "failed");
        assert!(reports[0].last_error.is_some());

        engine.lock().await.shutdown().await;
    }

    /// A failure AFTER exclusion rolls back to passive capture: never two
    /// producers for one key.
    #[tokio::test]
    async fn post_exclusion_failure_restores_passive() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        let desired = tcp_desired(1, listen, 80, 5);
        // Sabotage the capture session so ActivateCapture fails.
        engine
            .lock()
            .await
            .start_capture_session(&desired.name)
            .unwrap();

        supervisor.set_desired(HashMap::from([(1u64, desired)]), 5);
        supervisor.tick_once().await;

        assert!(passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert!(engine.lock().await.service_spec(1).is_none());
        // Only the session the test planted remains; the transition
        // registered nothing.
        assert_eq!(
            engine.lock().await.capture_manager().list_sessions().len(),
            1
        );
        assert_eq!(supervisor.status_reports()[0].state, "failed");

        engine.lock().await.shutdown().await;
    }

    /// Update: newer desired revision replaces the runtime, keeps the
    /// exclusion and never leaves two authoritative producers.
    #[tokio::test]
    async fn update_replaces_rule_keeps_exclusion() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        supervisor.set_desired(HashMap::from([(1u64, tcp_desired(1, listen, 80, 5))]), 5);
        supervisor.tick_once().await;

        let mut updated = tcp_desired(1, listen, 80, 6);
        updated.http2 = true;
        supervisor.set_desired(HashMap::from([(1u64, updated)]), 6);
        supervisor.tick_once().await;

        assert_eq!(supervisor.status_reports()[0].effective_revision, Some(6));
        assert_eq!(engine.lock().await.active_rule_names().len(), 1);
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));

        engine.lock().await.shutdown().await;
    }

    /// A harpoon engine that dies while the proxy is desired is detected
    /// and rebuilt in place; passive capture is NOT re-enabled and the
    /// loss is reported first.
    #[tokio::test]
    async fn dead_engine_is_recovered_without_passive() {
        let state = test_state();
        *state.services.write().unwrap() = assigned(&[(1, TransportProto::Tcp, 80)]);
        let engine = Arc::new(tokio::sync::Mutex::new(HarpoonEngine::new(None)));
        let supervisor = ProxySupervisor::with_parts(engine.clone(), state.clone(), true, false);
        spawn_sniffer_stub(state.clone());
        seed_passive(&state, &supervisor);

        let listen = free_port().await;
        supervisor.set_desired(HashMap::from([(1u64, tcp_desired(1, listen, 80, 5))]), 5);
        supervisor.tick_once().await;
        assert!(engine.lock().await.service_spec(1).is_some());
        supervisor.test_backdate_effective(1);

        // Simulate the engine dying (crash): the listener disappears.
        let spec = spec_of(&tcp_desired(1, listen, 80, 5));
        let old = engine.lock().await.take_service_runtime(1).unwrap();
        old.shutdown().await;
        // Port free now: the probe must report dead.
        assert!(tokio::net::TcpListener::bind(spec.listen).await.is_ok());

        supervisor.tick_once().await;

        // Recovery rebuilt the rule; passive stays excluded.
        assert!(engine.lock().await.service_spec(1).is_some());
        assert!(!passive(&state).contains(&(TransportProto::Tcp, 80)));
        assert_eq!(supervisor.status_reports()[0].state, "effective");

        engine.lock().await.shutdown().await;
    }
}
