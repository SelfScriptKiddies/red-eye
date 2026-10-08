use actix_identity::Identity;
use actix_web::{post, web, HttpResponse, Responder};
use serde::Deserialize;
use std::sync::Arc;

use crate::api::agent::agent_service::AgentService;
use crate::api::service::service_service::ServiceService;

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ProtectRequest {
    pub service_id: i64,
    pub agent_id: String,
    pub enable: bool,
    #[serde(default)]
    pub tls_mitm: bool,
    /// Custom proxy port (if not set, defaults to service_port + 10000)
    pub proxy_port: Option<u16>,
}

/// Generate proxy rule name from service
fn proxy_name(service_name: &str) -> String {
    format!("protect-{}", service_name.replace(' ', "-").to_lowercase())
}

/// Default proxy port — tries port+10000, falls back to +10001..+10099 if taken.
fn default_proxy_port(service_port: i32, used_ports: &[u16]) -> u16 {
    let base = (service_port + 10000) as u16;
    for offset in 0..100 {
        let candidate = base + offset;
        if !used_ports.contains(&candidate) {
            return candidate;
        }
    }
    base
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct IpBanRequest {
    pub agent_id: String,
    pub ip: String,
    pub ban: bool,
}

#[utoipa::path(post, path = "/api/protect", tag = "protect",
    responses((status = 200, description = "Protection toggled")))]
#[post("/protect")]
async fn toggle_protection(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    service_service: web::Data<ServiceService>,
    body: web::Json<ProtectRequest>,
) -> impl Responder {
    let service = match service_service.get_service_by_id(body.service_id).await {
        Ok(s) => s,
        Err(e) if crate::api::is_pool_timeout(&e) => return crate::api::sqlx_error_response(&e),
        Err(_) => return HttpResponse::BadRequest().json(serde_json::json!({"error": "Service not found"})),
    };

    let rule_name = proxy_name(&service.name);
    let harpoon_port = match body.proxy_port {
        Some(p) => p,
        None => {
            let used_ports: Vec<u16> = agent_service
                .get_agent(&body.agent_id).await
                .map(|info| info.nft_redirects.iter().map(|r| r.to_port).collect())
                .unwrap_or_default();
            default_proxy_port(service.port, &used_ports)
        }
    };

    if body.enable {
        let mut command = serde_json::json!({
            "type": "ProxyAdd",
            "name": rule_name,
            "listen": format!("0.0.0.0:{}", harpoon_port),
            "target": format!("127.0.0.1:{}", service.port),
            "protocol": "tcp",
            "nft_redirect_port": service.port,
            "http2": service.http2_parse
        });

        if body.tls_mitm {
            command["tls_mode"] = serde_json::json!("mitm");
        }

        match agent_service.send_command(&body.agent_id, &command).await {
            Ok(_) => HttpResponse::Ok().json(serde_json::json!({
                "status": "enabled",
                "proxy_port": harpoon_port,
                "rule_name": rule_name,
                "tls_mitm": body.tls_mitm
            })),
            Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
        }
    } else {
        let command = serde_json::json!({
            "type": "ProxyRemove",
            "name": rule_name
        });

        match agent_service.send_command(&body.agent_id, &command).await {
            Ok(_) => HttpResponse::Ok().json(serde_json::json!({
                "status": "disabled",
                "rule_name": rule_name
            })),
            Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
        }
    }
}

#[utoipa::path(post, path = "/api/protect/ip-ban", tag = "protect",
    responses((status = 200, description = "IP ban toggled")))]
#[post("/protect/ip-ban")]
async fn toggle_ip_ban(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    body: web::Json<IpBanRequest>,
) -> impl Responder {
    let command = if body.ban {
        serde_json::json!({"type": "IpBan", "ip": body.ip})
    } else {
        serde_json::json!({"type": "IpUnban", "ip": body.ip})
    };

    let action = if body.ban { "banned" } else { "unbanned" };

    if body.agent_id == "_all" {
        let results = agent_service.broadcast_command(&[], &command).await;
        let success = results.iter().filter(|(_, r)| r.is_ok()).count();
        HttpResponse::Ok().json(serde_json::json!({"status": action, "ip": body.ip, "agents": success}))
    } else {
        match agent_service.send_command(&body.agent_id, &command).await {
            Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": action, "ip": body.ip})),
            Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
        }
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(toggle_protection)
        .service(toggle_ip_ban);
}
