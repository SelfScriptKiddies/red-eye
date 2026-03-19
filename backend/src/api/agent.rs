pub mod agent_dto;
pub mod agent_service;

use std::sync::Arc;

use actix_identity::Identity;
use actix_web::{get, post, web, HttpResponse, Responder};

use self::agent_dto::{AgentCommandPayload, BroadcastCommandPayload};
use self::agent_service::AgentService;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_all_agents)
        .service(get_agent_by_id)
        .service(send_agent_command)
        .service(broadcast_agent_command);
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
