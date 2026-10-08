pub mod agent_dto;
pub mod agent_service;

use std::sync::Arc;

use actix_identity::Identity;
use actix_web::{get, post, web, HttpResponse, Responder};

use serde::Deserialize;

use self::agent_dto::{AgentCommandPayload, BroadcastCommandPayload};
use self::agent_service::AgentService;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_all_agents)
        .service(get_agent_by_id)
        .service(send_agent_command)
        .service(broadcast_agent_command)
        .service(get_agent_services)
        .service(bind_agent_service)
        .service(unbind_agent_service);
}

#[utoipa::path(
    get,
    path = "/api/agent",
    responses(
        (status = 200, description = "List of all known agents"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[get("/agent")]
async fn get_all_agents(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
) -> impl Responder {
    let agents = agent_service.list_agents().await;
    HttpResponse::Ok().json(agents)
}

#[utoipa::path(
    get,
    path = "/api/agent/{id}",
    responses(
        (status = 200, description = "Agent details"),
        (status = 404, description = "Agent not found"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[get("/agent/{id}")]
async fn get_agent_by_id(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    id: web::Path<String>,
) -> impl Responder {
    match agent_service.get_agent(&id).await {
        Some(agent) => HttpResponse::Ok().json(agent),
        None => HttpResponse::NotFound().json(serde_json::json!({"error": "Agent not found"})),
    }
}

#[utoipa::path(
    post,
    path = "/api/agent/{id}/command",
    responses(
        (status = 200, description = "Command sent successfully"),
        (status = 400, description = "Invalid command"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[post("/agent/{id}/command")]
async fn send_agent_command(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    id: web::Path<String>,
    command: web::Json<AgentCommandPayload>,
) -> impl Responder {
    match agent_service.send_command(&id, &command.inner).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": "sent"})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
    }
}

#[utoipa::path(
    post,
    path = "/api/agent/broadcast",
    responses(
        (status = 200, description = "Command broadcast results"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[post("/agent/broadcast")]
async fn broadcast_agent_command(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    payload: web::Json<BroadcastCommandPayload>,
) -> impl Responder {
    let results = agent_service
        .broadcast_command(&payload.agent_ids, &payload.command)
        .await;

    let response: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(id, result)| {
            serde_json::json!({
                "agent_id": id,
                "status": if result.is_ok() { "sent" } else { "error" },
                "error": result.err()
            })
        })
        .collect();

    HttpResponse::Ok().json(response)
}

// ── Agent-Service bindings ──

#[get("/agent/{id}/services")]
async fn get_agent_services(
    _identity: Identity,
    id: web::Path<String>,
    pool: web::Data<sqlx::PgPool>,
) -> impl Responder {
    let agent_id = id.into_inner();
    let services = sqlx::query_scalar::<_, i64>(
        "SELECT service_id FROM agent_service WHERE agent_id = $1"
    )
    .bind(&agent_id)
    .fetch_all(pool.get_ref())
    .await;

    match services {
        Ok(ids) => HttpResponse::Ok().json(ids),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[derive(Deserialize)]
struct BindRequest {
    service_id: i64,
}

#[post("/agent/{id}/services/bind")]
async fn bind_agent_service(
    _identity: Identity,
    id: web::Path<String>,
    body: web::Json<BindRequest>,
    pool: web::Data<sqlx::PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
) -> impl Responder {
    let agent_id = id.into_inner();
    let result = sqlx::query(
        "INSERT INTO agent_service (agent_id, service_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    )
    .bind(&agent_id)
    .bind(body.service_id)
    .execute(pool.get_ref())
    .await;

    match result {
        Ok(_) => {
            // Push updated ports to agent
            sync_agent_ports(&agent_id, pool.get_ref(), &agent_service).await;
            HttpResponse::Ok().json(serde_json::json!({"status": "bound"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[post("/agent/{id}/services/unbind")]
async fn unbind_agent_service(
    _identity: Identity,
    id: web::Path<String>,
    body: web::Json<BindRequest>,
    pool: web::Data<sqlx::PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
) -> impl Responder {
    let agent_id = id.into_inner();
    let result = sqlx::query(
        "DELETE FROM agent_service WHERE agent_id = $1 AND service_id = $2"
    )
    .bind(&agent_id)
    .bind(body.service_id)
    .execute(pool.get_ref())
    .await;

    match result {
        Ok(_) => {
            sync_agent_ports(&agent_id, pool.get_ref(), &agent_service).await;
            HttpResponse::Ok().json(serde_json::json!({"status": "unbound"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Push SnifferConfig with ports from bound services to an agent
pub async fn sync_agent_ports(agent_id: &str, pool: &sqlx::PgPool, agent_svc: &AgentService) {
    let ports = sqlx::query_scalar::<_, i32>(
        "SELECT s.port FROM service s JOIN agent_service a ON a.service_id = s.id WHERE a.agent_id = $1"
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let cmd = serde_json::json!({
        "type": "SnifferConfig",
        "ports": ports
    });

    let _ = agent_svc.send_command(agent_id, &cmd).await;
    log::info!("Synced ports {:?} to agent '{}'", ports, agent_id);
}

/// Bind all services to an agent (for new agents)
pub async fn bind_all_services_to_agent(agent_id: &str, pool: &sqlx::PgPool) {
    if let Err(e) = sqlx::query(
        "INSERT INTO agent_service (agent_id, service_id)
         SELECT $1, id FROM service
         ON CONFLICT DO NOTHING"
    )
    .bind(agent_id)
    .execute(pool)
    .await
    {
        // A failed binding means the agent sniffs none of the services —
        // the operator must see it, not wonder why there is no traffic.
        log::error!(
            "Failed to bind services to agent '{}': {} — agent will not sniff any services until rebound",
            agent_id, e
        );
    }
}
