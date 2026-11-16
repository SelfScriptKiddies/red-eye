pub mod service_dto;
pub mod service_repository;
pub mod service_service;

use actix_identity::Identity;
use actix_web::Either;
use actix_web::{HttpResponse, Responder, web};
use actix_web::{delete, get, post, put};

use self::service_dto::{MutationResult, Service, ServiceTransfer};
use self::service_service::{ServiceService, is_unique_service_violation};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_all_services)
        .service(get_service_by_id)
        .service(create_service)
        .service(update_service)
        .service(delete_service);
}

fn valid_port(port: i32) -> bool {
    (1..=65535).contains(&port)
}

/// Validation for create: protocol required, port within 1..=65535, name
/// non-empty. Returns the normalized service (protocol uppercased).
fn validate_new(transfer: &ServiceTransfer) -> Result<Service, HttpResponse> {
    let bad = |msg: &str| HttpResponse::BadRequest().json(serde_json::json!({ "error": msg }));
    if transfer.name.trim().is_empty() {
        return Err(bad("Service name must not be empty"));
    }
    if !valid_port(transfer.port) {
        return Err(bad("Port must be between 1 and 65535"));
    }
    let Some(protocol) = transfer.normalized_protocol() else {
        return Err(bad("protocol is required and must be one of: TCP, UDP"));
    };
    let mut service: Service = transfer.clone().into();
    service.protocol = protocol;
    Ok(service)
}

/// Validation for update: protocol optional (omitted keeps the row's
/// current protocol, e.g. pre-migration TCP rows), port validated when
/// present.
fn validate_update(existing: Service, transfer: &ServiceTransfer) -> Result<Service, HttpResponse> {
    let bad = |msg: &str| HttpResponse::BadRequest().json(serde_json::json!({ "error": msg }));
    if transfer.name.trim().is_empty() {
        return Err(bad("Service name must not be empty"));
    }
    if !valid_port(transfer.port) {
        return Err(bad("Port must be between 1 and 65535"));
    }
    let mut service: Service = transfer.clone().into();
    service.id = existing.id;
    match transfer.normalized_protocol() {
        Some(protocol) => service.protocol = protocol,
        None => service.protocol = existing.protocol,
    }
    // Assignment mode defaults to the row's current mode when omitted.
    if transfer.is_global.is_none() {
        service.is_global = existing.is_global;
    }
    Ok(service)
}

fn sync_response(status: actix_web::http::StatusCode, result: MutationResult) -> HttpResponse {
    HttpResponse::build(status).json(serde_json::json!({
        "service": result.service,
        "revision": result.revision,
        // "synced" = post-commit publication delivered; "pending" = the
        // change is committed but NATS publication failed and periodic
        // reconciliation will deliver it. A successful DB write is never
        // presented as confirmed delivery to every agent.
        "sync": result.sync,
    }))
}

#[utoipa::path(
    get,
    path = "/api/service",
    responses(
        (status = 200, body = Vec<Service>, description = "Services retrieved successfully", example = json!([{
            "id": 1,
            "name": "music",
            "port": 23779,
            "protocol": "TCP",
            "is_global": true,
            "agent_ids": [],
            "chunked_transfer_encoding": true,
            "urldecode": false,
            "merge_adjacent_packets": true,
            "inflate_websockets": true,
            "decrypt_tls": true
        }])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "service"
)]
#[get("/service")]
async fn get_all_services(
    _identity: Identity,
    service: web::Data<ServiceService>,
) -> impl Responder {
    match service.get_all_services().await {
        Ok(services) => HttpResponse::Ok().json(services),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(
    get,
    path = "/api/service/{id}",
    responses(
        (status = 200, body = Service, description = "Service retrieved successfully"),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "service"
)]
#[get("/service/{id}")]
async fn get_service_by_id(
    _identity: Identity,
    service: web::Data<ServiceService>,
    id: web::Path<i64>,
) -> impl Responder {
    match service.get_service_by_id(id.into_inner()).await {
        Ok(service) => HttpResponse::Ok().json(service),
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[utoipa::path(
    post,
    path = "/api/service",
    responses(
        (status = 201, description = "Service created; body carries the new control-plane revision and sync state"),
        (status = 400, description = "Invalid service (protocol required, port 1-65535)", body = serde_json::Value),
        (status = 409, description = "A service with this name, port and protocol already exists"),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "service"
)]
#[post("/service")]
async fn create_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    new_service: Either<web::Form<ServiceTransfer>, web::Json<ServiceTransfer>>,
) -> impl Responder {
    let transfer = new_service.into_inner().into();
    let validated = match validate_new(&transfer) {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let agent_ids = transfer.agent_ids.clone().unwrap_or_default();
    match service.create_service(&validated, &agent_ids).await {
        Ok(result) => sync_response(actix_web::http::StatusCode::CREATED, result),
        Err(e) if is_unique_service_violation(&e) => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": "A service with this name, port and protocol already exists"
            }))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(
    put,
    path = "/api/service/{id}",
    responses(
        (status = 200, description = "Service updated; body carries the new control-plane revision and sync state"),
        (status = 400, description = "Invalid service", body = serde_json::Value),
        (status = 409, description = "A service with this name, port and protocol already exists"),
        (status = 404, description = "Service not found"),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "service"
)]
#[put("/service/{id}")]
async fn update_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    id: web::Path<i64>,
    updated_service: web::Json<ServiceTransfer>,
) -> impl Responder {
    let id = id.into_inner();
    let existing = match service.get_service_by_id(id).await {
        Ok(s) => s,
        Err(e) if crate::api::is_pool_timeout(&e) => {
            return crate::api::sqlx_error_response(&e);
        }
        Err(_) => return HttpResponse::NotFound().finish(),
    };

    let transfer = updated_service.into_inner();
    let mut validated = match validate_update(existing, &transfer) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    validated.id = id;

    match service
        .update_service(&validated, transfer.agent_ids.clone())
        .await
    {
        Ok(result) => sync_response(actix_web::http::StatusCode::OK, result),
        Err(e) if is_unique_service_violation(&e) => {
            HttpResponse::Conflict().json(serde_json::json!({
                "error": "A service with this name, port and protocol already exists"
            }))
        }
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[utoipa::path(
    delete,
    path = "/api/service/{id}",
    responses(
        (status = 200, description = "Service deleted; body carries the new control-plane revision and sync state"),
        (status = 404, description = "Service not found"),
        (status = 401, description = "Unauthorized", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "service"
)]
#[delete("/service/{id}")]
async fn delete_service(
    _identity: Identity,
    service: web::Data<ServiceService>,
    id: web::Path<i64>,
) -> impl Responder {
    match service.delete_service(id.into_inner()).await {
        Ok(result) => sync_response(actix_web::http::StatusCode::OK, result),
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer(protocol: Option<&str>, port: i32, name: &str) -> ServiceTransfer {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "port": port,
            "protocol": protocol,
        }))
        .unwrap()
    }

    fn existing(protocol: &str, is_global: bool) -> Service {
        serde_json::from_value(serde_json::json!({
            "id": 5,
            "name": "dns",
            "port": 53,
            "chunked_transfer_encoding": false,
            "urldecode": false,
            "merge_adjacent_packets": false,
            "inflate_websockets": false,
            "decrypt_tls": false,
            "http_parse": false,
            "http2_parse": false,
            "alert_flag_leak": false,
            "protocol": protocol,
            "is_global": is_global,
            "agent_ids": [],
        }))
        .unwrap()
    }

    #[test]
    fn create_requires_protocol() {
        let err = validate_new(&transfer(None, 53, "dns")).unwrap_err();
        assert_eq!(err.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_rejects_unknown_protocol_and_bad_ports() {
        assert!(validate_new(&transfer(Some("SCTP"), 53, "dns")).is_err());
        assert!(validate_new(&transfer(Some("TCP"), 0, "dns")).is_err());
        assert!(validate_new(&transfer(Some("TCP"), 65536, "dns")).is_err());
        assert!(validate_new(&transfer(Some("TCP"), 8080, " ")).is_err());
    }

    #[test]
    fn create_normalizes_protocol_case() {
        let service = validate_new(&transfer(Some("udp"), 53, "dns")).unwrap();
        assert_eq!(service.protocol, "UDP");
    }

    #[test]
    fn update_keeps_existing_protocol_and_scope_when_omitted() {
        let existing = existing("UDP", false);
        let updated = validate_update(existing.clone(), &transfer(None, 53, "dns")).unwrap();
        assert_eq!(updated.protocol, "UDP");
        assert!(!updated.is_global, "scope omitted keeps the row's mode");

        let updated = validate_update(existing, &transfer(Some("tcp"), 53, "dns")).unwrap();
        assert_eq!(updated.protocol, "TCP");
    }

    #[test]
    fn update_defaults_missing_flags_like_the_legacy_payload() {
        let existing = existing("TCP", true);
        // Pre-migration rows and old clients send no protocol at all.
        let payload: ServiceTransfer = serde_json::from_value(serde_json::json!({
            "name": "dns", "port": 53
        }))
        .unwrap();
        let updated = validate_update(existing, &payload).unwrap();
        assert_eq!(updated.protocol, "TCP");
        assert!(updated.is_global);
    }
}
