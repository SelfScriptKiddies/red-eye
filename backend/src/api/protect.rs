use actix_identity::Identity;
use actix_web::{post, web, HttpResponse, Responder};
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_service::AgentService;
use crate::api::service::service_service::ServiceService;

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ProtectRequest {
    pub service_id: i32,
    pub agent_id: String,
    pub enable: bool,
    /// Enable TLS MITM (decrypt + re-encrypt)
    #[serde(default)]
    pub tls_mitm: bool,
}

/// Calculate harpoon proxy port from service port
fn proxy_port(service_port: i32) -> u16 {
    (service_port + 10000) as u16
}

/// Generate proxy rule name from service
fn proxy_name(service_name: &str) -> String {
    format!("protect-{}", service_name.replace(' ', "-").to_lowercase())
}

#[utoipa::path(
    post,
    path = "/api/protect",
    responses(
        (status = 200, description = "Protection toggled"),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "protect"
)]
#[post("/protect")]
async fn toggle_protection(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    service_service: web::Data<ServiceService>,
    body: web::Json<ProtectRequest>,
) -> impl Responder {
    // Get service info
    let service = match service_service.get_service_by_id(body.service_id).await {
        Ok(s) => s,
        Err(_) => return HttpResponse::BadRequest().json(serde_json::json!({"error": "Service not found"})),
    };

    let rule_name = proxy_name(&service.name);
    let harpoon_port = proxy_port(service.port);

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

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(toggle_protection);
}
