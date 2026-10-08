pub mod service_dto;
pub mod service_repository;
pub mod service_service;

use actix_identity::Identity;
use actix_web::{web, HttpResponse, Responder};
use crate::api::service::service_dto::{Service, ServiceTransfer};
use crate::api::service::service_service::ServiceService;
use crate::api::updater::UpdateAgent;
use actix_web::Either;
use actix_web::{get, post, put, delete};


pub fn config(cfg: &mut web::ServiceConfig) {
    cfg 
        .service(get_all_services)
        .service(get_service_by_id)
        .service(create_service)
        .service(update_service)
        .service(delete_service);
}


#[utoipa::path(
    get,
    path = "/api/service",
    responses(
        (status = 200, body = Vec<Service>, description = "Services retrieved successfully", example = json!([{
            "id": 1,
            "name": "music",
            "port": 23779,
            "chunked_transfer_encoding": true,
            "urldecode": false,
            "merge_adjacent_packets": true,
            "inflate_websockets": true,
            "decrypt_tls": true
        },
        {
            "id": 2,
            "name": "serious",
            "port": 15778,
            "chunked_transfer_encoding": true,
            "urldecode": true,
            "merge_adjacent_packets": false,
            "inflate_websockets": false,
            "decrypt_tls": false
        }])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[get("/service")]
async fn get_all_services(
    _identity: Identity,
    service: web::Data<ServiceService>
) -> impl Responder {
    match service.get_all_services().await {
        Ok(services) => HttpResponse::Ok().json(services),
        Err(e) => crate::api::sqlx_error_response(&e)
    }
}

#[utoipa::path(
    get,
    path = "/api/service/{id}",
    responses(
        (status = 200, body = Service, description = "Service retrieved successfully", example = json!({
            "id": 1,
            "name": "music",
            "port": 23779,
            "chunked_transfer_encoding": true,
            "urldecode": false,
            "merge_adjacent_packets": true,
            "inflate_websockets": true,
            "decrypt_tls": true
        })),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[get("/service/{id}")]
async fn get_service_by_id(
    _identity: Identity,
    service: web::Data<ServiceService>,
    id: web::Path<i64>
) -> impl Responder {
    match service.get_service_by_id(id.into_inner()).await {
        Ok(service) => HttpResponse::Ok().json(service),
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish()
    }
}

#[utoipa::path(
    post,
    path = "/api/service",
    responses(
        (status = 200, body = Service, description = "Service created successfully", example = json!({
            "id": 1,
            "name": "music",
            "port": 23779,
            "chunked_transfer_encoding": true,
            "urldecode": false,
            "merge_adjacent_packets": true,
            "inflate_websockets": true,
            "decrypt_tls": true
        })),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[post("/service")]
async fn create_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    updater: web::Data<UpdateAgent>,
    pool: web::Data<sqlx::PgPool>,
    agent_svc: web::Data<std::sync::Arc<crate::api::agent::agent_service::AgentService>>,
    new_service: Either<web::Form<ServiceTransfer>, web::Json<ServiceTransfer>>
) -> impl Responder {
    match service.create_service(&new_service.into_inner().into()).await {
        Ok(created) => {
            if let Err(e) = updater.add_service(created.clone().into(), created.id.try_into().unwrap()).await {
                log::error!("Failed to send update: {}", e);
            }

            // Bind new service to all live agents and sync their ports.
            // Agent ids live only in the in-memory registry (NATS heartbeats);
            // there is no `agent` table in the DB, so the ids must come from
            // AgentService, not from a set-based INSERT ... SELECT FROM agent.
            let agents = agent_svc.list_agents().await;
            if agents.is_empty() {
                log::info!(
                    "No live agents yet: service '{}' (id {}) will be bound when agents connect",
                    created.name, created.id
                );
            }
            for a in &agents {
                let bind = sqlx::query(
                    "INSERT INTO agent_service (agent_id, service_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
                )
                .bind(&a.agent_id)
                .bind(created.id)
                .execute(pool.get_ref())
                .await;
                if let Err(e) = bind {
                    log::error!(
                        "Failed to bind service '{}' (id {}) to agent '{}': {} — service will not be sniffed on this agent",
                        created.name, created.id, a.agent_id, e
                    );
                }
            }

            // Sync ports for all agents
            for a in &agents {
                crate::api::agent::sync_agent_ports(&a.agent_id, pool.get_ref(), &agent_svc).await;
            }

            HttpResponse::Created().json(created)
        }
        Err(e) => crate::api::sqlx_error_response(&e)
    }
}

#[utoipa::path(
    put,
    path = "/api/service/{id}",
    responses(
        (status = 200, body = ServiceTransfer, description = "Service updated successfully", example = json!({
            "id": 1,
            "name": "music",
            "port": 23779,
            "chunked_transfer_encoding": true,
            "urldecode": false,
            "merge_adjacent_packets": true,
            "inflate_websockets": true,
            "decrypt_tls": true
        })),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[put("/service/{id}")]
async fn update_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    updater: web::Data<UpdateAgent>,
    id: web::Path<i64>,
    updated_service: web::Json<ServiceTransfer>
) -> impl Responder {
    let mut service_data: Service = updated_service.into_inner().into();
    service_data.id = id.into_inner();
    
    match service.update_service(&service_data).await {
        Ok(updated) => {
            if let Err(e) = updater.update_service(updated.id.try_into().unwrap(), updated.clone().into()).await {
                log::error!("Failed to send update: {}", e);
            }
            HttpResponse::Ok().json(updated)
        }
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish()
    }
}

#[utoipa::path(
    delete,
    path = "/api/service/{id}",
    responses(
        (status = 200, description = "Service deleted successfully"),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[delete("/service/{id}")]
async fn delete_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    updater: web::Data<UpdateAgent>,
    id: web::Path<i64>
) -> impl Responder {
    let id = id.into_inner();
    match service.delete_service(id).await {
        Ok(_) => {
            if let Err(e) = updater.remove_service(id.try_into().unwrap()).await {
                log::error!("Failed to send update: {}", e);
            }
            HttpResponse::Ok().finish()
        }
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish()
    }
}
