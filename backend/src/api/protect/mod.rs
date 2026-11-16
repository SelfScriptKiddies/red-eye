pub mod protect_dto;
pub mod protect_repository;
pub mod protect_state;

use std::sync::Arc;

use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, delete, get, post, web};
use log::warn;

use crate::api::agent::agent_config::{ControlPlane, SnapshotReason};
use crate::api::agent::agent_service::AgentService;
use crate::api::protect::protect_dto::{
    IpBanRequest, ProtectRequest, ProtectResponse, ProtectionStateRow,
};
use crate::api::protect::protect_repository::{ProtectRepository, ProtectionRow};
use crate::api::protect::protect_state::{DesiredAction, ReportedState, capture_source};

/// The UI sends stream dst/src IPs verbatim, and those come CIDR-styled from
/// the API ("10.0.0.1/32"). The agent's nftables validator expects a bare
/// address, so strip the prefix (and whitespace) before dispatching.
fn normalize_ip(ip: &str) -> String {
    ip.trim().split('/').next().unwrap_or(ip.trim()).to_string()
}

/// Display name of the harpoon rule. The desired-state identity is
/// (agent_id, service_id); the service id in the name keeps it unique even
/// when two services share a name across protocols. Shared with the
/// snapshot builder so commands and snapshots always agree on the name.
pub(crate) fn proxy_rule_name(service_id: i64, service_name: &str) -> String {
    let slug: String = service_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    format!("protect-{service_id}-{slug}")
}

fn wire_protocol(protocol: &str) -> &'static str {
    if protocol.eq_ignore_ascii_case("udp") {
        "UDP"
    } else {
        "TCP"
    }
}

fn row_to_state(row: ProtectionRow, agent_id: &str) -> ProtectionStateRow {
    let reported = row
        .reported_state
        .as_deref()
        .and_then(ReportedState::from_wire);
    let source = capture_source(row.enabled, reported, row.last_error.as_deref());
    ProtectionStateRow {
        agent_id: agent_id.to_string(),
        service_id: row.service_id,
        service_name: row.service_name,
        service_port: row.service_port,
        protocol: row.protocol.clone(),
        enabled: row.enabled,
        listen_port: row.listen_port,
        target_port: row.target_port,
        tls_mode: row.tls_mode,
        http2_enabled: row.http2_enabled,
        revision: row.revision,
        effective: row.effective,
        effective_revision: row.effective_revision,
        capture_source: source.as_str().to_string(),
        last_error: row.last_error,
    }
}

#[utoipa::path(post, path = "/api/protect", tag = "protect",
    responses((status = 200, description = "Desired protection state saved")))]
#[post("/protect")]
async fn toggle_protection(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    service_service: web::Data<crate::api::service::service_service::ServiceService>,
    control: web::Data<Arc<ControlPlane>>,
    body: web::Json<ProtectRequest>,
) -> impl Responder {
    let body = body.into_inner();
    let repo = ProtectRepository::new(control.pool().clone());

    let service = match service_service.get_service_by_id(body.service_id).await {
        Ok(s) => s,
        Err(e) if crate::api::is_pool_timeout(&e) => return crate::api::sqlx_error_response(&e),
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "Service not found"}));
        }
    };
    let protocol = wire_protocol(&service.protocol);
    let rule_name = proxy_rule_name(service.id, &service.name);
    let http2 = service.http2_parse && protocol == "TCP";

    let existing = match repo.get_row(&body.agent_id, body.service_id).await {
        Ok(row) => row,
        Err(e) => return crate::api::sqlx_error_response(&e),
    };
    let action = protect_state::desired_action(existing.as_ref().map(|r| r.enabled), body.enable);

    // Listen port: only the enable path needs one. Allocation considers
    // persisted desired rules first (they survive backend restarts), then
    // live heartbeat redirects, and refuses collisions explicitly.
    let mut proxy_port: Option<u16> = None;
    if body.enable {
        let mut used: Vec<u16> = repo
            .enabled_listen_ports(&body.agent_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|p| u16::try_from(p).ok())
            .collect();
        if let Some(info) = agent_service.get_agent(&body.agent_id).await {
            for redirect in &info.nft_redirects {
                used.push(redirect.to_port);
            }
        }
        match protect_state::allocate_proxy_port(service.port, &used, body.proxy_port) {
            Ok(port) => proxy_port = Some(port),
            Err(msg) => {
                return HttpResponse::Conflict().json(serde_json::json!({"error": msg}));
            }
        }
    }

    let mut revision = control.current_revision().await.unwrap_or(0);
    if action != DesiredAction::Republish {
        let result = if body.enable {
            let mut tx = match repo.pool().begin().await {
                Ok(tx) => tx,
                Err(e) => return crate::api::sqlx_error_response(&e),
            };
            let persisted = ProtectRepository::upsert_enabled(
                &mut tx,
                &body.agent_id,
                body.service_id,
                "0.0.0.0",
                proxy_port.unwrap_or_default() as i32,
                "127.0.0.1",
                service.port,
                protocol,
                if body.tls_mitm { Some("mitm") } else { None },
                http2,
            )
            .await;
            if let Err(e) = persisted {
                return crate::api::sqlx_error_response(&e);
            }
            commit_with_revision(tx).await
        } else {
            let mut tx = match repo.pool().begin().await {
                Ok(tx) => tx,
                Err(e) => return crate::api::sqlx_error_response(&e),
            };
            let persisted =
                ProtectRepository::set_enabled(&mut tx, &body.agent_id, body.service_id, false)
                    .await;
            if let Err(e) = persisted {
                return crate::api::sqlx_error_response(&e);
            }
            commit_with_revision(tx).await
        };
        match result {
            Ok(rev) => revision = rev,
            Err(e) => return crate::api::sqlx_error_response(&e),
        }
    }

    // Publication happens after commit and is an optimization: a failure
    // leaves the change pending sync (heartbeats + reconciliation
    // re-deliver the snapshot), never rolls the desired state back.
    let sync = publish_desired(
        &agent_service,
        &control,
        &body,
        &rule_name,
        proxy_port,
        service.port,
        protocol,
        http2,
    )
    .await;

    let response = ProtectResponse {
        desired_saved: true,
        enabled: body.enable,
        service_id: body.service_id,
        agent_id: body.agent_id.clone(),
        rule_name,
        revision,
        sync,
        proxy_port,
    };
    HttpResponse::Ok().json(response)
}

async fn commit_with_revision(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<i64, sqlx::Error> {
    let revision = ControlPlane::bump_revision(&mut tx).await?;
    tx.commit().await?;
    Ok(revision)
}

#[allow(clippy::too_many_arguments)]
async fn publish_desired(
    agent_service: &Arc<AgentService>,
    control: &Arc<ControlPlane>,
    body: &ProtectRequest,
    rule_name: &str,
    proxy_port: Option<u16>,
    service_port: i32,
    protocol: &str,
    http2: bool,
) -> String {
    let mut delivered = true;

    let command = if body.enable {
        let mut command = serde_json::json!({
            "type": "ProxyAdd",
            "service_id": body.service_id,
            "name": rule_name,
            "listen": format!("0.0.0.0:{}", proxy_port.unwrap_or_default()),
            "target": format!("127.0.0.1:{}", service_port),
            "protocol": protocol.to_lowercase(),
            "nft_redirect_port": service_port,
            "http2": http2
        });
        if body.tls_mitm {
            command["tls_mode"] = serde_json::json!("mitm");
        }
        command
    } else {
        serde_json::json!({
            "type": "ProxyRemove",
            "service_id": body.service_id,
            "name": rule_name
        })
    };

    if let Err(e) = agent_service.send_command(&body.agent_id, &command).await {
        warn!(
            "Proxy desired state for agent '{}' service {} committed but command failed (pending sync): {}",
            body.agent_id, body.service_id, e
        );
        delivered = false;
    }

    // The snapshot carries the same desired state (service_id-keyed) so a
    // restarted agent converges without a second operator command.
    if !control
        .send_snapshot(&body.agent_id, SnapshotReason::Operator)
        .await
    {
        delivered = false;
    }

    if delivered {
        "published".to_string()
    } else {
        "pending".to_string()
    }
}

/// Protection state for one agent: desired rows plus the agent-reported
/// capture source (passive | proxy | transitioning | proxy_unavailable).
#[utoipa::path(get, path = "/api/protect/{agent_id}/state", tag = "protect",
    responses((status = 200, description = "Protection state rows")))]
#[get("/protect/{agent_id}/state")]
async fn protection_state(
    _identity: Identity,
    control: web::Data<Arc<ControlPlane>>,
    agent_id: web::Path<String>,
) -> impl Responder {
    let agent_id = agent_id.into_inner();
    let repo = ProtectRepository::new(control.pool().clone());
    match repo.rows_for_agent(&agent_id).await {
        Ok(rows) => {
            let state: Vec<ProtectionStateRow> = rows
                .into_iter()
                .map(|row| row_to_state(row, &agent_id))
                .collect();
            HttpResponse::Ok().json(state)
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Delete the desired protection row entirely (implies disable: the agent
/// runs the disable transition when the proxy disappears from its
/// snapshot, and a deleted service's port is not returned to passive
/// capture).
#[utoipa::path(delete, path = "/api/protect/{agent_id}/{service_id}", tag = "protect",
    responses((status = 200, description = "Protection desired state deleted")))]
#[delete("/protect/{agent_id}/{service_id}")]
async fn delete_protection(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    control: web::Data<Arc<ControlPlane>>,
    path: web::Path<(String, i64)>,
) -> impl Responder {
    let (agent_id, service_id) = path.into_inner();
    let repo = ProtectRepository::new(control.pool().clone());

    let deleted = match repo.delete_row(&agent_id, service_id).await {
        Ok(n) => n,
        Err(e) => return crate::api::sqlx_error_response(&e),
    };
    let mut revision = control.current_revision().await.unwrap_or(0);
    if deleted > 0 {
        let tx = match repo.pool().begin().await {
            Ok(tx) => tx,
            Err(e) => return crate::api::sqlx_error_response(&e),
        };
        match commit_with_revision(tx).await {
            Ok(rev) => revision = rev,
            Err(e) => return crate::api::sqlx_error_response(&e),
        }
    }

    let command = serde_json::json!({
        "type": "ProxyRemove",
        "service_id": service_id
    });
    let mut delivered = true;
    if let Err(e) = agent_service.send_command(&agent_id, &command).await {
        delivered = false;
        warn!(
            "Protection delete for agent '{}' service {} committed but command failed: {}",
            agent_id, service_id, e
        );
    }
    if !control
        .send_snapshot(&agent_id, SnapshotReason::Operator)
        .await
    {
        delivered = false;
    }

    HttpResponse::Ok().json(serde_json::json!({
        "desired_deleted": deleted > 0,
        "revision": revision,
        "sync": if delivered { "published" } else { "pending" }
    }))
}

#[utoipa::path(post, path = "/api/protect/ip-ban", tag = "protect",
    responses((status = 200, description = "IP ban toggled")))]
#[post("/protect/ip-ban")]
async fn toggle_ip_ban(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    body: web::Json<IpBanRequest>,
) -> impl Responder {
    let ip = normalize_ip(&body.ip);
    if ip.parse::<std::net::IpAddr>().is_err() {
        return HttpResponse::BadRequest()
            .json(serde_json::json!({"error": format!("Invalid IP address: {}", body.ip)}));
    }

    let command = if body.ban {
        serde_json::json!({"type": "IpBan", "ip": ip})
    } else {
        serde_json::json!({"type": "IpUnban", "ip": ip})
    };

    let action = if body.ban { "banned" } else { "unbanned" };

    if body.agent_id == "_all" {
        let results = agent_service.broadcast_command(&[], &command).await;
        let success = results.iter().filter(|(_, r)| r.is_ok()).count();
        HttpResponse::Ok().json(serde_json::json!({"status": action, "ip": ip, "agents": success}))
    } else {
        match agent_service.send_command(&body.agent_id, &command).await {
            Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": action, "ip": ip})),
            Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
        }
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(toggle_protection)
        .service(protection_state)
        .service(delete_protection)
        .service(toggle_ip_ban);
}
