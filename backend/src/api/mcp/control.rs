use actix_web::{HttpRequest, HttpResponse, Responder, delete, post, put, web};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_config::{self, ControlPlane};
use crate::api::agent::agent_service::AgentService;
use crate::api::service::service_dto::ServiceTransfer;
use crate::api::service::service_service::{ServiceService, is_unique_service_violation};
use crate::api::soc::bearer;
use crate::api::soc::settings;
use crate::api::tag::tag_dto::TagNameCreate;
use crate::api::tag::tag_service::TagService;

// Full-control mutation endpoints for red-eye-mcp. The service credential
// must carry capability=full_control here INDEPENDENTLY of any MCP-layer
// token: a read_only deployment (the default compose) cannot mutate even
// if its MCP process is compromised or misconfigured. Every call needs
// expected_revision (optimistic concurrency against the control-plane
// revision), confirm=true and a non-empty reason for the audit record.
// A failed validation leaves the target unchanged.

/// Scope + confirm + reason + revision checks shared by every mutation.
/// The current revision is read by the caller so both the ControlPlane
/// path and the direct-SQL path (filters, bans) share one guard.
async fn guard_mutation(
    pool: &PgPool,
    expected_revision: i64,
    confirm: bool,
    reason: &str,
) -> Result<String, HttpResponse> {
    let capability = crate::api::settings::get_setting(pool, settings::MCP_SERVICE_CAPABILITY)
        .await
        .unwrap_or_else(|| "read_only".to_string());
    if capability != "full_control" {
        return Err(HttpResponse::Forbidden().json(json!({
            "error": "service credential lacks full_control scope",
        })));
    }
    if !confirm {
        return Err(HttpResponse::BadRequest()
            .json(json!({"error": "confirm=true is required for mutations"})));
    }
    let reason = reason.trim().to_string();
    if reason.is_empty() || reason.chars().count() > 500 {
        return Err(
            HttpResponse::BadRequest().json(json!({"error": "reason must be 1-500 characters"}))
        );
    }
    let current: Option<i64> =
        sqlx::query_scalar("SELECT revision FROM control_plane_revision WHERE id = 1")
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    match current {
        Some(current) if current == expected_revision => {}
        Some(current) => {
            return Err(HttpResponse::Conflict().json(json!({
                "error": "revision conflict: refresh the resource and retry",
                "expected_revision": expected_revision,
                "current_revision": current,
            })));
        }
        None => {
            return Err(HttpResponse::InternalServerError()
                .json(json!({"error": "control-plane revision unavailable"})));
        }
    }
    Ok(reason)
}

fn audit(tool: &str, session: &str, target: &str, expected: i64, result: &str, detail: &str) {
    // Structured audit record: ids and statuses only. Payloads, bearer
    // tokens and keys never enter audit output.
    log::info!(
        target: "mcp_audit",
        "MCP_AUDIT tool={} session={} target={} expected_revision={} result={} detail={}",
        tool, session, target, expected, result, detail
    );
}

fn session_hint(req: &HttpRequest) -> String {
    req.headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
                .take(64)
                .collect()
        })
        .unwrap_or_else(|| "none".to_string())
}

async fn require_full(pool: &PgPool, req: &HttpRequest) -> Result<(), HttpResponse> {
    bearer::require_service_token(
        pool,
        req,
        settings::MCP_SERVICE_TOKEN,
        Some("mcp_service_last_seen"),
    )
    .await
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ControlBase {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ServiceCreateBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub name: String,
    pub port: i32,
    pub protocol: String,
    #[serde(default)]
    pub is_global: bool,
    #[serde(default)]
    pub agent_ids: Vec<String>,
    #[serde(default)]
    pub chunked_transfer_encoding: bool,
    #[serde(default)]
    pub urldecode: bool,
    #[serde(default)]
    pub merge_adjacent_packets: bool,
    #[serde(default)]
    pub inflate_websockets: bool,
    #[serde(default)]
    pub decrypt_tls: bool,
    #[serde(default)]
    pub http_parse: bool,
    #[serde(default)]
    pub http2_parse: bool,
    #[serde(default)]
    pub alert_flag_leak: bool,
}

/// POST /api/mcp/control/service: create_service; agent assignment rides
/// on agent_ids + is_global (set_service_agents is the same mutation with
/// a changed agent set).
#[post("/mcp/control/service")]
async fn create_service(
    pool: web::Data<PgPool>,
    service_service: web::Data<ServiceService>,
    body: web::Json<ServiceCreateBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let session = session_hint(&req);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "create_service",
            &session,
            &body.name,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    let bad = |m: &str| HttpResponse::BadRequest().json(json!({"error": m}));
    if body.name.trim().is_empty() || body.name.chars().count() > 255 {
        audit(
            "create_service",
            &session,
            "-",
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return bad("name must be 1-255 characters");
    }
    if !(1..=65535).contains(&body.port) {
        audit(
            "create_service",
            &session,
            &body.name,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return bad("port must be 1-65535");
    }
    let protocol = body.protocol.trim().to_uppercase();
    if protocol != "TCP" && protocol != "UDP" {
        audit(
            "create_service",
            &session,
            &body.name,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return bad("protocol must be TCP or UDP");
    }
    let transfer = ServiceTransfer {
        name: body.name.trim().to_string(),
        port: body.port,
        chunked_transfer_encoding: Some(body.chunked_transfer_encoding),
        urldecode: Some(body.urldecode),
        merge_adjacent_packets: Some(body.merge_adjacent_packets),
        inflate_websockets: Some(body.inflate_websockets),
        decrypt_tls: Some(body.decrypt_tls),
        http_parse: Some(body.http_parse),
        http2_parse: Some(body.http2_parse),
        alert_flag_leak: Some(body.alert_flag_leak),
        protocol: Some(protocol),
        is_global: Some(body.is_global),
        agent_ids: Some(body.agent_ids.clone()),
    };
    let validated = crate::api::service::service_dto::Service::from(transfer);
    match service_service
        .create_service(&validated, &body.agent_ids)
        .await
    {
        Ok(result) => {
            audit(
                "create_service",
                &session,
                &body.name,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Created().json(json!({
                "service": result.service,
                "revision": result.revision,
                "sync": result.sync,
            }))
        }
        Err(e) if is_unique_service_violation(&e) => {
            audit(
                "create_service",
                &session,
                &body.name,
                body.expected_revision,
                "rejected",
                "duplicate",
            );
            HttpResponse::Conflict()
                .json(json!({"error": "service with this name, port and protocol exists"}))
        }
        Err(_) => {
            audit(
                "create_service",
                &session,
                &body.name,
                body.expected_revision,
                "error",
                "db",
            );
            HttpResponse::InternalServerError().json(json!({"error": "create failed"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ServiceUpdateBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub name: String,
    pub port: i32,
    pub protocol: Option<String>,
    pub is_global: Option<bool>,
    pub agent_ids: Option<Vec<String>>,
    #[serde(default)]
    pub chunked_transfer_encoding: Option<bool>,
    #[serde(default)]
    pub urldecode: Option<bool>,
    #[serde(default)]
    pub merge_adjacent_packets: Option<bool>,
    #[serde(default)]
    pub inflate_websockets: Option<bool>,
    #[serde(default)]
    pub decrypt_tls: Option<bool>,
    #[serde(default)]
    pub http_parse: Option<bool>,
    #[serde(default)]
    pub http2_parse: Option<bool>,
    #[serde(default)]
    pub alert_flag_leak: Option<bool>,
}

/// PUT /api/mcp/control/service/{id}: update_service and
/// set_service_agents (agent_ids present). Omitted fields keep the row's
/// current values, mirroring the operator PUT.
#[put("/mcp/control/service/{id}")]
async fn update_service(
    pool: web::Data<PgPool>,
    service_service: web::Data<ServiceService>,
    path: web::Path<i64>,
    body: web::Json<ServiceUpdateBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("service:{}", id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "update_service",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    let existing = match service_service.get_service_by_id(id).await {
        Ok(s) => s,
        Err(_) => {
            audit(
                "update_service",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "missing",
            );
            return HttpResponse::NotFound().json(json!({"error": "service not found"}));
        }
    };
    if body.name.trim().is_empty() || !(1..=65535).contains(&body.port) {
        audit(
            "update_service",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return HttpResponse::BadRequest().json(json!({"error": "invalid name or port"}));
    }
    let protocol = body
        .protocol
        .clone()
        .map(|p| p.trim().to_uppercase())
        .filter(|p| p == "TCP" || p == "UDP");
    if body.protocol.is_some() && protocol.is_none() {
        audit(
            "update_service",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return HttpResponse::BadRequest().json(json!({"error": "protocol must be TCP or UDP"}));
    }
    let transfer = ServiceTransfer {
        name: body.name.trim().to_string(),
        port: body.port,
        chunked_transfer_encoding: body.chunked_transfer_encoding,
        urldecode: body.urldecode,
        merge_adjacent_packets: body.merge_adjacent_packets,
        inflate_websockets: body.inflate_websockets,
        decrypt_tls: body.decrypt_tls,
        http_parse: body.http_parse,
        http2_parse: body.http2_parse,
        alert_flag_leak: body.alert_flag_leak,
        protocol: protocol.clone(),
        is_global: body.is_global,
        agent_ids: body.agent_ids.clone(),
    };
    let mut validated = crate::api::service::service_dto::Service::from(transfer);
    validated.id = id;
    if protocol.is_none() {
        validated.protocol = existing.protocol;
    }
    if body.is_global.is_none() {
        validated.is_global = existing.is_global;
    }
    match service_service
        .update_service(&validated, body.agent_ids.clone())
        .await
    {
        Ok(result) => {
            audit(
                "update_service",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({
                "service": result.service,
                "revision": result.revision,
                "sync": result.sync,
            }))
        }
        Err(e) if is_unique_service_violation(&e) => {
            audit(
                "update_service",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "duplicate",
            );
            HttpResponse::Conflict()
                .json(json!({"error": "service with this name, port and protocol exists"}))
        }
        Err(_) => {
            audit(
                "update_service",
                &session,
                &target,
                body.expected_revision,
                "error",
                "db",
            );
            HttpResponse::InternalServerError().json(json!({"error": "update failed"}))
        }
    }
}

/// DELETE /api/mcp/control/service/{id}
#[delete("/mcp/control/service/{id}")]
async fn delete_service(
    pool: web::Data<PgPool>,
    service_service: web::Data<ServiceService>,
    path: web::Path<i64>,
    body: web::Json<ControlBase>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("service:{}", id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "delete_service",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    match service_service.delete_service(id).await {
        Ok(result) => {
            audit(
                "delete_service",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"revision": result.revision, "sync": result.sync}))
        }
        Err(_) => {
            audit(
                "delete_service",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "missing",
            );
            HttpResponse::NotFound().json(json!({"error": "service not found"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct NetworkSettingsBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub interface: Option<String>,
    pub machine_ip: Option<String>,
}

/// PUT /api/mcp/control/agent/{agent_id}/network-settings
/// (set_agent_network_settings).
#[put("/mcp/control/agent/{agent_id}/network-settings")]
async fn set_agent_network_settings(
    pool: web::Data<PgPool>,
    control: web::Data<Arc<ControlPlane>>,
    path: web::Path<String>,
    body: web::Json<NetworkSettingsBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let agent_id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("agent:{}", agent_id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "set_agent_network_settings",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    if let Some(v) = &body.interface
        && !agent_config::valid_interface(v)
    {
        audit(
            "set_agent_network_settings",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid-interface",
        );
        return HttpResponse::BadRequest().json(json!({"error": "invalid interface name"}));
    }
    if let Some(v) = &body.machine_ip
        && !agent_config::valid_machine_ip(v)
    {
        audit(
            "set_agent_network_settings",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid-ip",
        );
        return HttpResponse::BadRequest().json(json!({"error": "invalid machine_ip"}));
    }
    match control
        .set_network_overrides(
            &agent_id,
            body.interface.as_deref(),
            body.machine_ip.as_deref(),
        )
        .await
    {
        Ok(revision) => {
            audit(
                "set_agent_network_settings",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"revision": revision}))
        }
        Err(_) => {
            audit(
                "set_agent_network_settings",
                &session,
                &target,
                body.expected_revision,
                "error",
                "db",
            );
            HttpResponse::InternalServerError().json(json!({"error": "update failed"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ProxyBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub agent_id: String,
    pub enable: bool,
    #[serde(default)]
    pub tls_mitm: bool,
    pub proxy_port: Option<i32>,
}

/// POST /api/mcp/control/service/{id}/proxy (set_proxy_protection).
#[post("/mcp/control/service/{id}/proxy")]
async fn set_proxy_protection(
    pool: web::Data<PgPool>,
    service_service: web::Data<ServiceService>,
    agent_service: web::Data<Arc<AgentService>>,
    path: web::Path<i64>,
    body: web::Json<ProxyBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("service:{}/agent:{}", id, body.agent_id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "set_proxy_protection",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    let Ok(service) = service_service.get_service_by_id(id).await else {
        audit(
            "set_proxy_protection",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "missing",
        );
        return HttpResponse::NotFound().json(json!({"error": "service not found"}));
    };
    let rule_name = format!("protect-{}", service.name.replace(' ', "-").to_lowercase());
    let command = if body.enable {
        let port = body
            .proxy_port
            .unwrap_or((service.port + 10000).clamp(1024, 65535));
        let mut cmd = json!({
            "type": "ProxyAdd",
            "name": rule_name,
            "listen": format!("0.0.0.0:{}", port),
            "target": format!("127.0.0.1:{}", service.port),
            "protocol": service.protocol.to_lowercase(),
            "nft_redirect_port": service.port,
            "http2": service.http2_parse && service.protocol.eq_ignore_ascii_case("tcp"),
        });
        if body.tls_mitm {
            cmd["tls_mode"] = json!("mitm");
        }
        cmd
    } else {
        json!({"type": "ProxyRemove", "name": rule_name})
    };
    match agent_service.send_command(&body.agent_id, &command).await {
        Ok(_) => {
            audit(
                "set_proxy_protection",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok()
                .json(json!({"status": if body.enable { "enabled" } else { "disabled" }}))
        }
        Err(e) => {
            let sanitized: String = e.chars().filter(|c| !c.is_control()).take(200).collect();
            audit(
                "set_proxy_protection",
                &session,
                &target,
                body.expected_revision,
                "error",
                "agent",
            );
            HttpResponse::BadRequest().json(json!({"error": sanitized}))
        }
    }
}

fn valid_filter(
    name: &str,
    template: &str,
    template_type: &str,
    direction: &str,
    special_type: Option<&str>,
) -> Result<(), HttpResponse> {
    let bad = |m: &str| HttpResponse::BadRequest().json(json!({"error": m}));
    if name.trim().is_empty() || name.chars().count() > 255 {
        return Err(bad("name must be 1-255 characters"));
    }
    if template.trim().is_empty() || template.chars().count() > 255 {
        return Err(bad("template must be 1-255 characters"));
    }
    if !["regex", "substr", "bsubstr"].contains(&template_type) {
        return Err(bad("template_type must be regex, substr or bsubstr"));
    }
    if !["in", "out", "both"].contains(&direction) {
        return Err(bad("direction must be in, out or both"));
    }
    if let Some(s) = special_type {
        let known = ["flag", "alert", "block", "block_conn", "favorite", ""];
        if !known.contains(&s) {
            return Err(bad("unknown special_type"));
        }
    }
    Ok(())
}

fn tag_name_create(
    name: String,
    color: i16,
    template_type: String,
    template: String,
    direction: String,
    special_type: Option<String>,
) -> TagNameCreate {
    TagNameCreate {
        name,
        color,
        template_type,
        template,
        direction,
        special_type: special_type.filter(|s| !s.is_empty()),
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FilterBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub name: String,
    pub color: i16,
    pub template_type: String,
    pub template: String,
    pub direction: String,
    pub special_type: Option<String>,
}

/// POST /api/mcp/control/filter (create_filter).
#[post("/mcp/control/filter")]
async fn create_filter(
    pool: web::Data<PgPool>,
    tag_service: web::Data<TagService>,
    agent_service: web::Data<Arc<AgentService>>,
    body: web::Json<FilterBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let session = session_hint(&req);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "create_filter",
            &session,
            &body.name,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    if let Err(r) = valid_filter(
        &body.name,
        &body.template,
        &body.template_type,
        &body.direction,
        body.special_type.as_deref(),
    ) {
        audit(
            "create_filter",
            &session,
            &body.name,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return r;
    }
    let create = tag_name_create(
        body.name.trim().to_string(),
        body.color,
        body.template_type.clone(),
        body.template.clone(),
        body.direction.clone(),
        body.special_type.clone(),
    );
    match tag_service.create_tag_name(create).await {
        Ok(tag) => {
            if matches!(
                tag.special_type.as_deref(),
                Some("block") | Some("block_conn")
            ) {
                crate::api::block_sync::sync_block_filters(pool.get_ref(), &agent_service).await;
            }
            audit(
                "create_filter",
                &session,
                &body.name,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Created().json(json!({"filter": tag}))
        }
        Err(_) => {
            audit(
                "create_filter",
                &session,
                &body.name,
                body.expected_revision,
                "error",
                "db",
            );
            HttpResponse::InternalServerError().json(json!({"error": "create failed"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FilterUpdateBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub name: String,
    pub color: i16,
    pub template_type: String,
    pub template: String,
    pub direction: String,
    pub special_type: Option<String>,
    pub enabled: Option<bool>,
}

/// PUT /api/mcp/control/filter/{id} (update_filter, optional enabled).
#[put("/mcp/control/filter/{id}")]
async fn update_filter(
    pool: web::Data<PgPool>,
    tag_service: web::Data<TagService>,
    agent_service: web::Data<Arc<AgentService>>,
    path: web::Path<i64>,
    body: web::Json<FilterUpdateBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("filter:{}", id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "update_filter",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    if let Err(r) = valid_filter(
        &body.name,
        &body.template,
        &body.template_type,
        &body.direction,
        body.special_type.as_deref(),
    ) {
        audit(
            "update_filter",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid",
        );
        return r;
    }
    let create = tag_name_create(
        body.name.trim().to_string(),
        body.color,
        body.template_type.clone(),
        body.template.clone(),
        body.direction.clone(),
        body.special_type.clone(),
    );
    match tag_service.update_tag_name(id, create).await {
        Ok(tag) => {
            if let Some(enabled) = body.enabled {
                let _ = sqlx::query("UPDATE tag_name SET enabled = $2 WHERE id = $1")
                    .bind(id)
                    .bind(enabled)
                    .execute(pool.get_ref())
                    .await;
            }
            if matches!(
                tag.special_type.as_deref(),
                Some("block") | Some("block_conn")
            ) {
                crate::api::block_sync::sync_block_filters(pool.get_ref(), &agent_service).await;
            }
            audit(
                "update_filter",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"filter": tag}))
        }
        Err(_) => {
            audit(
                "update_filter",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "missing",
            );
            HttpResponse::NotFound().json(json!({"error": "filter not found"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FilterEnabledBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub enabled: bool,
}

/// PUT /api/mcp/control/filter/{id}/enabled (set_filter_enabled). A
/// disabled filter stops being pushed to agents by the next sync; a
/// failed toggle leaves the previous state unchanged.
#[put("/mcp/control/filter/{id}/enabled")]
async fn set_filter_enabled(
    pool: web::Data<PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
    path: web::Path<i64>,
    body: web::Json<FilterEnabledBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("filter:{}", id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "set_filter_enabled",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    let n = sqlx::query("UPDATE tag_name SET enabled = $2 WHERE id = $1")
        .bind(id)
        .bind(body.enabled)
        .execute(pool.get_ref())
        .await;
    match n {
        Ok(r) if r.rows_affected() > 0 => {
            // Re-sync so the change reaches agents immediately instead of
            // waiting for the next unrelated block-filter update.
            crate::api::block_sync::sync_block_filters(pool.get_ref(), &agent_service).await;
            audit(
                "set_filter_enabled",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"enabled": body.enabled}))
        }
        Ok(_) => {
            audit(
                "set_filter_enabled",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "missing",
            );
            HttpResponse::NotFound().json(json!({"error": "filter not found"}))
        }
        Err(_) => {
            audit(
                "set_filter_enabled",
                &session,
                &target,
                body.expected_revision,
                "error",
                "db",
            );
            HttpResponse::InternalServerError().json(json!({"error": "update failed"}))
        }
    }
}

/// DELETE /api/mcp/control/filter/{id}
#[delete("/mcp/control/filter/{id}")]
async fn delete_filter(
    pool: web::Data<PgPool>,
    tag_service: web::Data<TagService>,
    agent_service: web::Data<Arc<AgentService>>,
    path: web::Path<i64>,
    body: web::Json<ControlBase>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let id = path.into_inner();
    let session = session_hint(&req);
    let target = format!("filter:{}", id);
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "delete_filter",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    match tag_service.delete_tag_name(id).await {
        Ok(_) => {
            crate::api::block_sync::sync_block_filters(pool.get_ref(), &agent_service).await;
            audit(
                "delete_filter",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"status": "deleted"}))
        }
        Err(_) => {
            audit(
                "delete_filter",
                &session,
                &target,
                body.expected_revision,
                "rejected",
                "missing",
            );
            HttpResponse::NotFound().json(json!({"error": "filter not found"}))
        }
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct BlockRemoveBody {
    pub expected_revision: i64,
    pub confirm: bool,
    pub reason: String,
    pub agent_id: String,
    pub ip: String,
}

/// POST /api/mcp/control/blocks/remove (remove_active_block).
#[post("/mcp/control/blocks/remove")]
async fn remove_active_block(
    pool: web::Data<PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
    body: web::Json<BlockRemoveBody>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(r) = require_full(pool.get_ref(), &req).await {
        return r;
    }
    let session = session_hint(&req);
    let target = format!("ip:{}", body.ip.trim());
    if let Err(r) = guard_mutation(
        pool.get_ref(),
        body.expected_revision,
        body.confirm,
        &body.reason,
    )
    .await
    {
        audit(
            "remove_active_block",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "guard",
        );
        return r;
    }
    let ip = body
        .ip
        .trim()
        .split('/')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if ip.parse::<std::net::IpAddr>().is_err() {
        audit(
            "remove_active_block",
            &session,
            &target,
            body.expected_revision,
            "rejected",
            "invalid-ip",
        );
        return HttpResponse::BadRequest().json(json!({"error": "invalid IP address"}));
    }
    let command = json!({"type": "IpUnban", "ip": ip});
    let outcome = if body.agent_id == "_all" {
        let results = agent_service.broadcast_command(&[], &command).await;
        Ok(results.iter().filter(|(_, r)| r.is_ok()).count())
    } else {
        agent_service
            .send_command(&body.agent_id, &command)
            .await
            .map(|_| 1)
    };
    match outcome {
        Ok(n) => {
            audit(
                "remove_active_block",
                &session,
                &target,
                body.expected_revision,
                "ok",
                "",
            );
            HttpResponse::Ok().json(json!({"status": "unbanned", "ip": ip, "agents": n}))
        }
        Err(e) => {
            let sanitized: String = e.chars().filter(|c| !c.is_control()).take(200).collect();
            audit(
                "remove_active_block",
                &session,
                &target,
                body.expected_revision,
                "error",
                "agent",
            );
            HttpResponse::BadRequest().json(json!({"error": sanitized}))
        }
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_service)
        .service(update_service)
        .service(delete_service)
        .service(set_agent_network_settings)
        .service(set_proxy_protection)
        .service(create_filter)
        .service(update_filter)
        .service(set_filter_enabled)
        .service(delete_filter)
        .service(remove_active_block);
}
