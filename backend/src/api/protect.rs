use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, post, web};
use serde::Deserialize;
use std::sync::Arc;

use crate::api::agent::agent_service::AgentService;
use crate::api::service::service_service::ServiceService;
use crate::api::tag::tag_service::TagService;
use crate::api::updater::UpdateAgent;

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
    // Keep the +10000 offset inside valid port range instead of wrapping
    // (a wrapped port would collide with real service ports).
    let base = (service_port + 10000).clamp(1024, 65535) as u16;
    for offset in 0..100 {
        let candidate = base.saturating_add(offset);
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

/// The UI sends stream dst/src IPs verbatim, and those come CIDR-styled from
/// the API ("10.0.0.1/32"). The unified filter model expects a bare
/// address, so strip the prefix (and whitespace) before matching.
fn normalize_ip(ip: &str) -> String {
    ip.trim().split('/').next().unwrap_or(ip.trim()).to_string()
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
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": "Service not found"}));
        }
    };

    let rule_name = proxy_name(&service.name);
    let harpoon_port = match body.proxy_port {
        Some(p) => p,
        None => {
            let used_ports: Vec<u16> = agent_service
                .get_agent(&body.agent_id)
                .await
                .map(|info| info.nft_redirects.iter().map(|r| r.to_port).collect())
                .unwrap_or_default();
            default_proxy_port(service.port, &used_ports)
        }
    };

    if body.enable {
        // Protocol comes from the service row: a UDP service is proxied
        // and redirected with nftables over udp, never hardcoded to tcp.
        let mut command = serde_json::json!({
            "type": "ProxyAdd",
            "name": rule_name,
            "listen": format!("0.0.0.0:{}", harpoon_port),
            "target": format!("127.0.0.1:{}", service.port),
            "protocol": service.protocol.to_lowercase(),
            "nft_redirect_port": service.port,
            "http2": service.http2_parse && service.protocol.eq_ignore_ascii_case("tcp")
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

/// Compatibility caller of the unified filter model: the memory-only
/// IpBan/IpUnban agent commands became persistent ip_cidr filters with
/// block action (marked for removal once the UI uses the filter editor).
/// For "_all" the filter targets every agent globally; otherwise it is
/// bound to the named agent.
#[utoipa::path(post, path = "/api/protect/ip-ban", tag = "protect",
    responses((status = 200, description = "IP ban toggled through a unified ip_cidr filter")))]
#[post("/protect/ip-ban")]
async fn toggle_ip_ban(
    _identity: Identity,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>,
    body: web::Json<IpBanRequest>,
) -> impl Responder {
    use crate::api::tag::tag_dto::TagNameCreate;

    let addr = match normalize_ip(&body.ip).parse::<std::net::IpAddr>() {
        Ok(a) => a,
        Err(_) => {
            return HttpResponse::BadRequest()
                .json(serde_json::json!({"error": format!("Invalid IP address: {}", body.ip)}));
        }
    };
    // A single address is stored as its host prefix.
    let cidr = if addr.is_ipv4() {
        format!("{}/32", addr)
    } else {
        format!("{}/128", addr)
    };

    if body.ban {
        let request = TagNameCreate {
            name: format!("ban-{}", addr),
            color: 0,
            template_type: "ip_cidr".to_string(),
            template: String::new(),
            direction: "both".to_string(),
            special_type: None,
            action: Some("block".to_string()),
            enabled: true,
            ip_network: Some(cidr),
            ip_endpoint: Some("src".to_string()),
            expression_source: None,
            service_targets_global: true,
            agent_targets_global: body.agent_id == "_all",
            service_ids: Vec::new(),
            agent_ids: if body.agent_id == "_all" {
                Vec::new()
            } else {
                vec![body.agent_id.clone()]
            },
            threshold_count: None,
            threshold_window_secs: None,
            threshold_group_by: None,
            threshold_cooldown_secs: None,
            threshold_block_secs: None,
            expires_at: None,
        };
        match tag_service.create_tag_name(request).await {
            Ok(created) => {
                let id = u64::try_from(created.id).unwrap_or_default();
                if let Err(e) = updater.add_filter(created.into(), id).await {
                    log::warn!("Ban filter published as pending sync: {}", e);
                }
                HttpResponse::Ok().json(serde_json::json!({"status": "banned", "ip": addr}))
            }
            Err(crate::api::tag::tag_service::TagServiceError::Validation(e)) => {
                HttpResponse::BadRequest().json(serde_json::json!({"error": e.to_string()}))
            }
            Err(crate::api::tag::tag_service::TagServiceError::Sqlx(e)) => {
                crate::api::sqlx_error_response(&e)
            }
        }
    } else {
        // Unban removes the matching persistent ban filters; several may
        // exist when individual agents were banned one by one.
        let all = match tag_service.get_all_tag_names().await {
            Ok(all) => all,
            Err(e) => return crate::api::sqlx_error_response(&e),
        };
        let mut removed = 0usize;
        for filter in all {
            let agent_match = body.agent_id == "_all"
                || filter.agent_targets_global
                || filter.agent_ids.iter().any(|a| *a == body.agent_id);
            if filter.name == format!("ban-{}", addr)
                && filter.action == "block"
                && filter.template_type == "ip_cidr"
                && agent_match
                && tag_service.delete_tag_name(filter.id).await.is_ok()
            {
                removed += 1;
                if let Err(e) = updater
                    .remove_filter(u64::try_from(filter.id).unwrap_or_default())
                    .await
                {
                    log::warn!("Ban filter removal published as pending sync: {}", e);
                }
            }
        }
        if removed == 0 {
            return HttpResponse::NotFound().json(serde_json::json!(
                {"error": "No persistent ban filter for this IP and agent"}
            ));
        }
        HttpResponse::Ok()
            .json(serde_json::json!({"status": "unbanned", "ip": addr, "filters": removed}))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(toggle_protection).service(toggle_ip_ban);
}
