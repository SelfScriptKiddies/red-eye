use ipnetwork::IpNetwork;
use log::{error, info};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_config::ControlPlane;
use crate::wire::{WireActiveBlock, WireFilter};

/// Unified filter row for the agent projection.
#[derive(sqlx::FromRow)]
struct AgentFilterRow {
    id: i64,
    name: String,
    template_type: String,
    template: String,
    direction: String,
    action: String,
    enabled: bool,
    ip_network: Option<IpNetwork>,
    ip_endpoint: Option<String>,
    revision: i64,
    expires_at_ms: Option<i64>,
    services_all: bool,
    agents_all: bool,
}

/// Filter projection for one agent: only enabled block-family filters the
/// agent can enforce in its current protection mode (payload matchers and
/// IP/CIDR drops; expression matching is detection-only on the analyzer).
/// Global agent targets apply; explicit targets use the agent's stable id.
pub async fn agent_filter_projection(
    pool: &PgPool,
    agent_id: &str,
) -> Result<Vec<WireFilter>, sqlx::Error> {
    let rows = sqlx::query_as::<_, AgentFilterRow>(
        "SELECT id, name, template_type, template, direction, action, enabled, \
         ip_network, ip_endpoint, revision, \
         CASE WHEN expires_at IS NULL THEN NULL \
              ELSE (EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT END AS expires_at_ms, \
         service_targets_global AS services_all, agent_targets_global AS agents_all \
         FROM tag_name \
         WHERE enabled \
           AND action IN ('block', 'block_connection') \
           AND template_type IN ('regex', 'substr', 'bsubstr', 'ip_cidr') \
           AND (agent_targets_global \
                OR id IN (SELECT filter_id FROM filter_agent_target WHERE agent_id = $1)) \
           AND (expires_at IS NULL OR expires_at > now()) \
         ORDER BY id",
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await?;

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let mut service_targets: std::collections::HashMap<i64, Vec<i64>> =
        std::collections::HashMap::new();
    if !ids.is_empty() {
        for (filter_id, service_id) in sqlx::query_as::<_, (i64, i64)>(
            "SELECT filter_id, service_id FROM filter_service_target WHERE filter_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(pool)
        .await?
        {
            service_targets
                .entry(filter_id)
                .or_default()
                .push(service_id);
        }
    }

    Ok(rows
        .into_iter()
        .map(|row| {
            let service_ids = service_targets.remove(&row.id).unwrap_or_default();
            crate::api::tag::tag_dto::TagName {
                id: row.id,
                name: row.name,
                color: 0,
                template_type: row.template_type,
                template: row.template,
                direction: row.direction,
                special_type: None,
                action: row.action,
                enabled: row.enabled,
                ip_network: row.ip_network.map(|n| n.to_string()),
                ip_endpoint: row.ip_endpoint,
                expression_source: None,
                service_targets_global: row.services_all,
                agent_targets_global: row.agents_all,
                service_ids,
                agent_ids: Vec::new(),
                threshold_count: None,
                threshold_window_secs: None,
                threshold_group_by: None,
                threshold_cooldown_secs: None,
                threshold_block_secs: None,
                // Threshold state lives with the analyzer: the agent
                // enforces blocks delivered through active_block rows.
                expires_at: None,
                revision: row.revision,
            }
            .into()
        })
        .collect())
}

/// Unexpired active blocks for one agent (complete replacement set).
pub async fn agent_block_projection(
    pool: &PgPool,
    agent_id: &str,
) -> Result<Vec<WireActiveBlock>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, String, String, i64)>(
        "SELECT filter_id, agent_id, host(remote_ip), \
                (EXTRACT(EPOCH FROM blocked_until) * 1000)::BIGINT \
         FROM active_block \
         WHERE agent_id = $1 AND blocked_until > now() \
         ORDER BY id",
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(filter_id, agent_id, remote_ip, until_ms)| WireActiveBlock {
                filter_id: u64::try_from(filter_id).unwrap_or_default(),
                agent_id,
                remote_ip,
                blocked_until_ms: until_ms.max(0) as u64,
            },
        )
        .collect())
}

/// Remove expired blocks. Expiry is honored even if the analyzer that
/// created the block is offline: agents unban locally at expiry too, so a
/// missed revision bump cannot extend a block.
pub async fn delete_expired_blocks(pool: &PgPool) -> u64 {
    match sqlx::query("DELETE FROM active_block WHERE blocked_until < now()")
        .execute(pool)
        .await
    {
        Ok(result) => result.rows_affected(),
        Err(e) => {
            error!(
                "Active block cleanup failed (retrying next interval): {}",
                e
            );
            0
        }
    }
}

/// Periodic cleanup task. Pure expiry deletions do not bump the revision:
/// consumers expire blocks locally, so a bounded best-effort cleanup
/// cannot wedge the control plane.
pub fn spawn_block_cleanup(pool: PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await; // skip first tick
        loop {
            interval.tick().await;
            let removed = delete_expired_blocks(&pool).await;
            if removed > 0 {
                info!("Block cleanup: removed {} expired blocks", removed);
            }
        }
    });
}

/// Manually unban one active block: remove the row and advance the
/// revision so the agent drops the nftables rule promptly. Returns the
/// affected agent id.
pub async fn unban_block(
    control: &ControlPlane,
    block_id: i64,
) -> Result<Option<(String, i64)>, sqlx::Error> {
    let pool = control.pool();
    let target = sqlx::query_as::<_, (String,)>("SELECT agent_id FROM active_block WHERE id = $1")
        .bind(block_id)
        .fetch_optional(pool)
        .await?;
    let Some((agent_id,)) = target else {
        return Ok(None);
    };
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM active_block WHERE id = $1")
        .bind(block_id)
        .execute(tx.as_mut())
        .await?;
    let revision = ControlPlane::bump_revision(&mut tx).await?;
    tx.commit().await?;
    Ok(Some((agent_id, revision)))
}

/// List active blocks newest first, bounded.
pub async fn list_blocks(pool: &PgPool, limit: i64) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            i64,
            i64,
        ),
    >(
        "SELECT b.id, b.filter_id, b.agent_id, host(b.remote_ip), tn.name, tn.template_type, \
                (EXTRACT(EPOCH FROM b.created_at)::BIGINT), \
                (EXTRACT(EPOCH FROM b.blocked_until)::BIGINT), \
                (EXTRACT(EPOCH FROM now())::BIGINT) \
         FROM active_block b \
         JOIN tag_name tn ON tn.id = b.filter_id \
         WHERE b.blocked_until > now() \
         ORDER BY b.id DESC \
         LIMIT $1",
    )
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, filter_id, agent_id, remote_ip, filter_name, matcher, created_at, until, now)| {
                serde_json::json!({
                    "id": id,
                    "filter_id": filter_id,
                    "filter_name": filter_name,
                    "matcher": matcher,
                    "agent_id": agent_id,
                    "remote_ip": remote_ip,
                    "created_at": created_at,
                    "blocked_until": until,
                    "remaining_secs": (until - now).max(0),
                })
            },
        )
        .collect())
}

/// Bounded recent filter events for reconnect and audit.
pub async fn list_filter_events(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let rows = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            String,
            Option<i64>,
            Option<i64>,
            Option<String>,
            String,
            Option<i32>,
            Option<i32>,
            i64,
        ),
    >(
        "SELECT id, filter_id, agent_id, service_id, stream_id, host(remote_ip), kind, \
                window_secs, match_count, (EXTRACT(EPOCH FROM created_at)::BIGINT) \
         FROM filter_event \
         ORDER BY id DESC \
         LIMIT $1",
    )
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                filter_id,
                agent_id,
                service_id,
                stream_id,
                remote_ip,
                kind,
                window,
                count,
                at,
            )| {
                serde_json::json!({
                    "id": id,
                    "filter_id": filter_id,
                    "agent_id": agent_id,
                    "service_id": service_id,
                    "stream_id": stream_id,
                    "remote_ip": remote_ip,
                    "kind": kind,
                    "window_secs": window,
                    "match_count": count,
                    "created_at": at,
                })
            },
        )
        .collect())
}

/// Compatibility republish for MCP filter mutations: push the full
/// analyzer view (ConfigSync) and refresh agents whose revision lags, so
/// a changed filter reaches every consumer immediately. Replaces the
/// legacy FilterSync broadcast from the pre-unified model; disabled
/// filters drop out of the projections on the next snapshot.
pub async fn sync_block_filters(
    pool: &PgPool,
    tag_service: &crate::api::tag::tag_service::TagService,
    control: &Arc<ControlPlane>,
    agent_service: &Arc<crate::api::agent::agent_service::AgentService>,
) {
    let services = match crate::api::service::service_service::ServiceService::new_from_pgpool(
        pool.clone(),
        control.clone(),
    )
    .get_all_services()
    .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(
                "Config sync after filter mutation: services unavailable: {}",
                e
            );
            return;
        }
    };
    let tags = match tag_service.get_all_tag_names().await {
        Ok(t) => t,
        Err(e) => {
            error!(
                "Config sync after filter mutation: filters unavailable: {}",
                e
            );
            return;
        }
    };
    let services_data: Vec<(u64, crate::wire::WireService)> = services
        .into_iter()
        .map(|s| (u64::try_from(s.id).unwrap_or_default(), s.into()))
        .collect();
    let filters_data: Vec<(u64, WireFilter)> = tags
        .into_iter()
        .map(|t| (u64::try_from(t.id).unwrap_or_default(), t.into()))
        .collect();
    if let Err(e) = control
        .updater()
        .sync_config(services_data, filters_data)
        .await
    {
        error!("Config sync after filter mutation failed: {}", e);
    }
    control.reconcile_stale_agents(agent_service).await;
}
