use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info, warn};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::RwLock;

use crate::wire::{WireAgentConfigSnapshot, WireAgentNetworkSettings, WireProtocol, WireService};

/// Minimum spacing between unsolicited snapshots to one agent, so a
/// heartbeat-driven resend cannot flood the stream while an agent is
/// briefly stale (heartbeats arrive every 5s; one snapshot per 30s caps
/// convergence latency well below the 5-minute reconciliation).
const SNAPSHOT_MIN_INTERVAL_MS: u64 = 30_000;

/// Control-plane owner: single source of the revision counter, per-agent
/// configuration state and snapshot publication. Every state mutation is
/// committed to PostgreSQL first; NATS publication afterwards is an
/// optimization for prompt delivery, never the source of truth.
pub struct ControlPlane {
    pool: PgPool,
    updater: Arc<crate::api::updater::UpdateAgent>,
    /// Last unsolicited snapshot time per agent (in-memory rate limit;
    /// a backend restart may produce one extra snapshot, which consumers
    /// treat as harmless idempotent state).
    last_snapshot_ms: Arc<RwLock<HashMap<String, u64>>>,
}

/// Persistent per-agent control-plane row (agent_config).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AgentConfigState {
    pub agent_id: String,
    pub interface_override: Option<String>,
    pub machine_ip_override: Option<String>,
    pub reported_interface: String,
    pub reported_machine_ip: String,
    pub last_boot_id: String,
    pub last_acked_revision: i64,
    pub last_sync_error: Option<String>,
}

/// Why a snapshot should be (re)published to an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReason {
    NewBoot,
    StaleRevision,
    Uncertain,
    /// Direct operator action (protection toggle): bypasses the
    /// unsolicited-snapshot rate limit so the transition starts now.
    Operator,
    NotNeeded,
}

/// Operator-facing view of one agent's network settings (GET/PUT/DELETE
/// /api/agent/{agent_id}/network-settings).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct AgentNetworkSettingsView {
    pub agent_id: String,
    /// Effective capture interface (override if present, else bootstrap)
    pub interface: Option<String>,
    /// "override" | "bootstrap" | "unknown"
    pub interface_source: String,
    /// Effective machine IP (concrete address or wildcard 0.0.0.0 / ::)
    pub machine_ip: Option<String>,
    /// "override" | "bootstrap" | "unknown"
    pub machine_ip_source: String,
    /// Last revision the agent reported as applied
    pub last_acked_revision: i64,
    /// Current control-plane revision
    pub current_revision: i64,
    /// "ok" | "pending" (acked revision behind current) | "error"
    pub sync_state: String,
    /// Sanitized snapshot application error reported by the agent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync_error: Option<String>,
}

/// Request for PUT /api/agent/{agent_id}/network-settings. A null (or
/// absent) field clears that override; a string sets it. The agent-side
/// validation runs again on the agent host.
#[derive(Debug, Clone, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct NetworkSettingsUpdate {
    #[serde(default)]
    pub interface: Option<String>,
    #[serde(default)]
    pub machine_ip: Option<String>,
}

/// Source of an effective network setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingSource {
    Override,
    Bootstrap,
    Unknown,
}

impl SettingSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            SettingSource::Override => "override",
            SettingSource::Bootstrap => "bootstrap",
            SettingSource::Unknown => "unknown",
        }
    }
}

/// Effective network settings for one agent, resolved per the precedence
/// rules: valid server override, else reported bootstrap value.
#[derive(Debug, Clone)]
pub struct EffectiveNetwork {
    pub interface: Option<String>,
    pub interface_source: SettingSource,
    pub machine_ip: Option<String>,
    pub machine_ip_source: SettingSource,
}

/// Validate a machine IP: a concrete IPv4/IPv6 address or the wildcard
/// 0.0.0.0 / :: (both parse as IpAddr, the analyzer treats them as
/// wildcard matchers).
pub fn valid_machine_ip(value: &str) -> bool {
    value.parse::<std::net::IpAddr>().is_ok()
}

/// Validate an interface name: non-empty, bounded, no whitespace or
/// control characters. The agent re-validates against its own interfaces;
/// the server cannot see the remote host.
pub fn valid_interface(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.chars().all(|c| !c.is_whitespace() && !c.is_control())
}

impl ControlPlane {
    pub fn new(pool: PgPool, updater: Arc<crate::api::updater::UpdateAgent>) -> Self {
        Self {
            pool,
            updater,
            last_snapshot_ms: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn updater(&self) -> &Arc<crate::api::updater::UpdateAgent> {
        &self.updater
    }

    /// Advance the revision counter inside the caller's transaction: the
    /// state change and its revision move atomically, so consumers can
    /// gate snapshots on a single monotonic number.
    pub async fn bump_revision(tx: &mut Transaction<'_, Postgres>) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("UPDATE control_plane_revision SET revision = revision + 1 WHERE id = 1 RETURNING revision")
            .fetch_one(tx.as_mut())
            .await
    }

    pub async fn current_revision(&self) -> Option<i64> {
        match sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM control_plane_revision WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await
        {
            Ok(rev) => Some(rev),
            Err(e) => {
                error!("Failed to read control-plane revision: {}", e);
                None
            }
        }
    }

    pub async fn agent_state(&self, agent_id: &str) -> Option<AgentConfigState> {
        sqlx::query_as::<_, AgentConfigState>(
            r#"
            SELECT agent_id, interface_override, machine_ip_override,
                   reported_interface, reported_machine_ip, last_boot_id,
                   last_acked_revision, last_sync_error
            FROM agent_config
            WHERE agent_id = $1
            "#,
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
    }

    /// Persist heartbeat-learned state and decide whether the agent needs a
    /// fresh snapshot: new boot id, acknowledged revision behind the
    /// current one, or an uncertain outcome (DB failure this round).
    pub async fn record_heartbeat(
        &self,
        agent_id: &str,
        boot_id: Option<&str>,
        interface: Option<&str>,
        machine_ip: Option<&str>,
        applied_revision: Option<i64>,
        sync_error: Option<&str>,
    ) -> SnapshotReason {
        let current = match self.current_revision().await {
            Some(rev) => rev,
            None => return SnapshotReason::Uncertain,
        };

        let result = sqlx::query_as::<_, (String, i64)>(
            r#"
            INSERT INTO agent_config
                (agent_id, reported_interface, reported_machine_ip,
                 last_boot_id, last_acked_revision, last_sync_error, updated_at)
            VALUES ($1,
                    COALESCE($2, ''),
                    COALESCE($3, ''),
                    COALESCE($4, ''),
                    COALESCE($5, 0),
                    $6, now())
            ON CONFLICT (agent_id) DO UPDATE SET
                reported_interface = COALESCE(EXCLUDED.reported_interface, agent_config.reported_interface),
                reported_machine_ip = COALESCE(EXCLUDED.reported_machine_ip, agent_config.reported_machine_ip),
                last_boot_id = COALESCE(EXCLUDED.last_boot_id, agent_config.last_boot_id),
                last_acked_revision = CASE
                    WHEN EXCLUDED.last_acked_revision IS NOT NULL
                        THEN EXCLUDED.last_acked_revision
                    ELSE agent_config.last_acked_revision END,
                last_sync_error = EXCLUDED.last_sync_error,
                updated_at = now()
            RETURNING last_boot_id, last_acked_revision
            "#,
        )
        .bind(agent_id)
        .bind(interface)
        .bind(machine_ip)
        .bind(boot_id)
        .bind(applied_revision)
        .bind(sync_error)
        .fetch_one(&self.pool)
        .await;

        match result {
            Ok((stored_boot_id, stored_revision)) => {
                if let Some(boot) = boot_id {
                    if boot != stored_boot_id {
                        // A new boot must see current state: update the
                        // stored id and force a snapshot.
                        let _ = sqlx::query(
                            "UPDATE agent_config SET last_boot_id = $2 WHERE agent_id = $1",
                        )
                        .bind(agent_id)
                        .bind(boot)
                        .execute(&self.pool)
                        .await;
                        return SnapshotReason::NewBoot;
                    }
                }
                if stored_revision < current {
                    SnapshotReason::StaleRevision
                } else {
                    SnapshotReason::NotNeeded
                }
            }
            Err(e) => {
                // The agent may be unknown-but-alive; deliver a snapshot so
                // it converges instead of silently lagging forever.
                error!(
                    "Failed to persist heartbeat state for agent '{}': {}",
                    agent_id, e
                );
                SnapshotReason::Uncertain
            }
        }
    }

    /// Resolve the effective network settings for an agent (override wins
    /// over bootstrap; both may be unknown). An invalid override falls
    /// back to the bootstrap value rather than disabling the agent.
    pub fn effective_network(state: &AgentConfigState) -> EffectiveNetwork {
        let (interface, interface_source) = match &state.interface_override {
            Some(v) if valid_interface(v) => (Some(v.clone()), SettingSource::Override),
            Some(_) => {
                warn!(
                    "Agent '{}' has invalid interface override, falling back to bootstrap",
                    state.agent_id
                );
                (
                    none_if_empty(&state.reported_interface),
                    SettingSource::Bootstrap,
                )
            }
            None => (
                none_if_empty(&state.reported_interface),
                SettingSource::Bootstrap,
            ),
        };
        let (machine_ip, machine_ip_source) = match &state.machine_ip_override {
            Some(v) if valid_machine_ip(v) => (Some(v.clone()), SettingSource::Override),
            Some(_) => {
                warn!(
                    "Agent '{}' has invalid machine_ip override, falling back to bootstrap",
                    state.agent_id
                );
                (
                    none_if_empty(&state.reported_machine_ip),
                    SettingSource::Bootstrap,
                )
            }
            None => (
                none_if_empty(&state.reported_machine_ip),
                SettingSource::Bootstrap,
            ),
        };
        EffectiveNetwork {
            interface,
            interface_source,
            machine_ip,
            machine_ip_source,
        }
    }

    /// Build the complete snapshot for one agent: its effective network
    /// settings plus its service set (global services plus explicit
    /// agent_service assignments). None when the DB read fails - callers
    /// treat that as pending sync, never as an empty assignment.
    pub async fn snapshot_for(&self, agent_id: &str) -> Option<WireAgentConfigSnapshot> {
        let revision = match self.current_revision().await {
            Some(rev) => rev,
            None => return None,
        };
        let state = match self.agent_state(agent_id).await {
            Some(state) => state,
            None => AgentConfigState {
                agent_id: agent_id.to_string(),
                interface_override: None,
                machine_ip_override: None,
                reported_interface: String::new(),
                reported_machine_ip: String::new(),
                last_boot_id: String::new(),
                last_acked_revision: 0,
                last_sync_error: None,
            },
        };
        let network = Self::effective_network(&state);

        let rows = match sqlx::query_as::<_, (i64, String, i32, String, bool)>(
            r#"
            SELECT id, name, port, protocol, is_global
            FROM service
            WHERE is_global
               OR id IN (SELECT service_id FROM agent_service WHERE agent_id = $1)
            ORDER BY id
            "#,
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                error!(
                    "Failed to read service set for agent '{}': {} - snapshot withheld",
                    agent_id, e
                );
                return None;
            }
        };

        let mut services = Vec::with_capacity(rows.len());
        for (id, name, port, protocol, is_global) in rows {
            let Ok(port_u16) = u16::try_from(port) else {
                warn!("Service id {} has out-of-range port {}, skipped", id, port);
                continue;
            };
            let wire_protocol = match protocol.as_str() {
                "TCP" => WireProtocol::Tcp,
                "UDP" => WireProtocol::Udp,
                other => {
                    warn!(
                        "Service id {} has unknown protocol '{}', skipped",
                        id, other
                    );
                    continue;
                }
            };
            services.push(WireService {
                name,
                port: port_u16 as u32,
                chunked_transfer_encoding: false,
                decrypt_tls: false,
                inflate_websockets: false,
                merge_adjacent_packets: false,
                urldecode: false,
                http_parse: false,
                id: u64::try_from(id).unwrap_or_default(),
                protocol: wire_protocol as i32,
                is_global,
            });
        }

        // Desired proxy state rides the snapshot: a same-ID restarted
        // agent reconstructs its proxy rules and passive exclusions from
        // it without another operator command.
        let protect =
            crate::api::protect::protect_repository::ProtectRepository::new(self.pool.clone());
        let desired_proxies = match protect.enabled_desired(agent_id).await {
            Ok(rows) => {
                let mut rules = Vec::with_capacity(rows.len());
                for row in rows {
                    let wire_protocol = match row.protocol.as_str() {
                        "TCP" => WireProtocol::Tcp,
                        "UDP" => WireProtocol::Udp,
                        other => {
                            warn!(
                                "Proxy rule for service id {} has unknown protocol '{}', skipped",
                                row.service_id, other
                            );
                            continue;
                        }
                    };
                    let Some(listen_port) = row.listen_port.and_then(|p| u16::try_from(p).ok())
                    else {
                        warn!(
                            "Proxy rule for service id {} has no usable listen port, skipped",
                            row.service_id
                        );
                        continue;
                    };
                    let Some(target_port) = row.target_port.and_then(|p| u16::try_from(p).ok())
                    else {
                        warn!(
                            "Proxy rule for service id {} has no usable target port, skipped",
                            row.service_id
                        );
                        continue;
                    };
                    rules.push(crate::wire::WireProxyRule {
                        service_id: u64::try_from(row.service_id).unwrap_or_default(),
                        name: crate::api::protect::proxy_rule_name(
                            row.service_id,
                            &row.service_name,
                        ),
                        listen_address: row
                            .listen_address
                            .clone()
                            .unwrap_or_else(|| "0.0.0.0".into()),
                        listen_port: listen_port as u32,
                        target_address: row
                            .target_address
                            .clone()
                            .unwrap_or_else(|| "127.0.0.1".into()),
                        target_port: target_port as u32,
                        protocol: wire_protocol as i32,
                        tls_mode: row.tls_mode.clone().unwrap_or_default(),
                        http2_enabled: row.http2_enabled,
                        revision: u64::try_from(row.revision).unwrap_or_default(),
                    });
                }
                rules
            }
            Err(e) => {
                error!(
                    "Failed to read desired proxies for agent '{}': {} - snapshot withheld",
                    agent_id, e
                );
                return None;
            }
        };

        Some(WireAgentConfigSnapshot {
            revision: u64::try_from(revision).unwrap_or_default(),
            agent_id: agent_id.to_string(),
            network: Some(WireAgentNetworkSettings {
                interface: network.interface.unwrap_or_default(),
                machine_ip: network.machine_ip.unwrap_or_default(),
            }),
            services,
            desired_proxies,
        })
    }

    /// Set (or clear) the server-side network overrides inside one
    /// transaction with the revision bump. `None` clears that override and
    /// returns the agent to its reported bootstrap value.
    pub async fn set_network_overrides(
        &self,
        agent_id: &str,
        interface: Option<&str>,
        machine_ip: Option<&str>,
    ) -> Result<i64, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO agent_config
                (agent_id, interface_override, machine_ip_override, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (agent_id) DO UPDATE SET
                interface_override = $2,
                machine_ip_override = $3,
                updated_at = now()
            "#,
        )
        .bind(agent_id)
        .bind(interface)
        .bind(machine_ip)
        .execute(tx.as_mut())
        .await?;
        let revision = Self::bump_revision(&mut tx).await?;
        tx.commit().await?;
        Ok(revision)
    }

    /// Operator-facing view of one agent's network settings: effective
    /// values, their source, acknowledged revision and sync state.
    pub async fn network_settings_view(&self, agent_id: &str) -> Option<AgentNetworkSettingsView> {
        let state = self.agent_state(agent_id).await?;
        let network = Self::effective_network(&state);
        let current = self.current_revision().await.unwrap_or(0);
        let sync_state = if state.last_sync_error.is_some() {
            "error"
        } else if state.last_acked_revision < current {
            "pending"
        } else {
            "ok"
        };
        Some(AgentNetworkSettingsView {
            agent_id: agent_id.to_string(),
            interface: network.interface,
            interface_source: network.interface_source.as_str().to_string(),
            machine_ip: network.machine_ip,
            machine_ip_source: network.machine_ip_source.as_str().to_string(),
            last_acked_revision: state.last_acked_revision,
            current_revision: current,
            sync_state: sync_state.to_string(),
            last_sync_error: state.last_sync_error,
        })
    }

    /// Build and publish the snapshot for one agent. Rate-limited unless
    /// `force` (boot detection and operator actions bypass the limiter).
    /// Returns false when publication failed (change stays pending sync).
    pub async fn send_snapshot(&self, agent_id: &str, reason: SnapshotReason) -> bool {
        if reason == SnapshotReason::NotNeeded {
            return true;
        }

        if reason != SnapshotReason::NewBoot
            && reason != SnapshotReason::Uncertain
            && reason != SnapshotReason::Operator
        {
            let now = now_ms();
            let mut last = self.last_snapshot_ms.write().await;
            let last_sent = last.get(agent_id).copied().unwrap_or(0);
            if now.saturating_sub(last_sent) < SNAPSHOT_MIN_INTERVAL_MS {
                return true;
            }
            last.insert(agent_id.to_string(), now);
        }

        let Some(snapshot) = self.snapshot_for(agent_id).await else {
            warn!(
                "Snapshot for agent '{}' withheld (state unavailable), stays pending sync",
                agent_id
            );
            return false;
        };

        let sent = self.updater.publish_agent_snapshot(snapshot).await;
        if sent {
            info!("Published snapshot to agent '{}' ({:?})", agent_id, reason);
        } else {
            warn!(
                "Snapshot publication to agent '{}' failed, stays pending sync",
                agent_id
            );
        }
        sent
    }

    /// Re-send snapshots to agents whose acknowledged revision lags the
    /// current one (periodic reconciliation path). Online agents only:
    /// offline agents get their snapshot from the boot/heartbeat path.
    pub async fn reconcile_stale_agents(
        &self,
        agent_service: &Arc<crate::api::agent::agent_service::AgentService>,
    ) {
        let Some(current) = self.current_revision().await else {
            return;
        };
        let online: Vec<String> = agent_service
            .list_agents()
            .await
            .into_iter()
            .filter(|a| a.online)
            .map(|a| a.agent_id)
            .collect();

        for agent_id in online {
            let acked = match self.agent_state(&agent_id).await {
                Some(state) => state.last_acked_revision,
                None => 0,
            };
            if acked < current {
                if !self
                    .send_snapshot(&agent_id, SnapshotReason::StaleRevision)
                    .await
                {
                    warn!(
                        "Reconciliation snapshot to agent '{}' not delivered",
                        agent_id
                    );
                }
            }
        }
    }
}

fn none_if_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(
        interface_override: Option<&str>,
        machine_ip_override: Option<&str>,
        reported_interface: &str,
        reported_machine_ip: &str,
    ) -> AgentConfigState {
        AgentConfigState {
            agent_id: "a1".into(),
            interface_override: interface_override.map(String::from),
            machine_ip_override: machine_ip_override.map(String::from),
            reported_interface: reported_interface.into(),
            reported_machine_ip: reported_machine_ip.into(),
            last_boot_id: String::new(),
            last_acked_revision: 0,
            last_sync_error: None,
        }
    }

    #[test]
    fn override_wins_over_bootstrap_when_valid() {
        let net = ControlPlane::effective_network(&state(
            Some("eth1"),
            Some("10.0.0.9"),
            "eth0",
            "10.0.0.5",
        ));
        assert_eq!(net.interface.as_deref(), Some("eth1"));
        assert_eq!(net.interface_source, SettingSource::Override);
        assert_eq!(net.machine_ip.as_deref(), Some("10.0.0.9"));
        assert_eq!(net.machine_ip_source, SettingSource::Override);
    }

    #[test]
    fn bootstrap_is_used_without_overrides() {
        let net = ControlPlane::effective_network(&state(None, None, "eth0", "10.0.0.5"));
        assert_eq!(net.interface.as_deref(), Some("eth0"));
        assert_eq!(net.interface_source, SettingSource::Bootstrap);
        assert_eq!(net.machine_ip.as_deref(), Some("10.0.0.5"));
        assert_eq!(net.machine_ip_source, SettingSource::Bootstrap);
    }

    #[test]
    fn invalid_override_falls_back_to_bootstrap() {
        let net = ControlPlane::effective_network(&state(
            Some("not an interface name with spaces"),
            Some("999.1.2.3"),
            "eth0",
            "10.0.0.5",
        ));
        assert_eq!(net.interface.as_deref(), Some("eth0"));
        assert_eq!(net.machine_ip.as_deref(), Some("10.0.0.5"));
        assert_eq!(net.interface_source, SettingSource::Bootstrap);
    }

    #[test]
    fn machine_ip_accepts_wildcards_and_concrete_addrs() {
        assert!(valid_machine_ip("0.0.0.0"));
        assert!(valid_machine_ip("::"));
        assert!(valid_machine_ip("10.0.0.5"));
        assert!(valid_machine_ip("fd00::5"));
        assert!(!valid_machine_ip("banana"));
        assert!(!valid_machine_ip(""));
    }

    #[test]
    fn interface_names_must_be_plain_tokens() {
        assert!(valid_interface("eth0"));
        assert!(valid_interface("enp3s0.100"));
        assert!(!valid_interface(""));
        assert!(!valid_interface("two words"));
        assert!(!valid_interface("a\nb"));
    }
}
