pub mod proto_dto;
pub mod proto_repository;

use actix_identity::Identity;
use actix_web::{delete, get, post, web, HttpResponse, Responder};
use sqlx::PgPool;

use self::proto_dto::ProtoFileCreate;
use self::proto_repository::ProtoRepository;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_protos_by_service)
        .service(upload_proto)
        .service(delete_proto)
        .service(get_all_protos);
}

#[utoipa::path(get, path = "/api/proto/service/{service_id}", tag = "proto",
    responses((status = 200, description = "Proto files for service")))]
#[get("/proto/service/{service_id}")]
async fn get_protos_by_service(
    _identity: Identity,
    pool: web::Data<PgPool>,
    service_id: web::Path<i32>,
) -> impl Responder {
    let repo = ProtoRepository::new(pool.get_ref().clone());
    match repo.get_by_service(service_id.into_inner()).await {
        Ok(protos) => HttpResponse::Ok().json(protos),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(get, path = "/api/proto", tag = "proto",
    responses((status = 200, description = "All proto files")))]
#[get("/proto")]
async fn get_all_protos(
    _identity: Identity,
    pool: web::Data<PgPool>,
) -> impl Responder {
    let repo = ProtoRepository::new(pool.get_ref().clone());
    match repo.get_all().await {
        Ok(protos) => HttpResponse::Ok().json(protos),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(post, path = "/api/proto", tag = "proto",
    responses((status = 201, description = "Proto file uploaded")))]
#[post("/proto")]
async fn upload_proto(
    _identity: Identity,
    pool: web::Data<PgPool>,
    body: web::Json<ProtoFileCreate>,
) -> impl Responder {
    let repo = ProtoRepository::new(pool.get_ref().clone());
    match repo.create(body.service_id, &body.filename, &body.content).await {
        Ok(proto) => HttpResponse::Created().json(proto),
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": format!("{}", e)})),
    }
}

#[utoipa::path(delete, path = "/api/proto/{id}", tag = "proto",
    responses((status = 200, description = "Proto file deleted")))]
#[delete("/proto/{id}")]
async fn delete_proto(
    _identity: Identity,
    pool: web::Data<PgPool>,
    id: web::Path<i32>,
) -> impl Responder {
    let repo = ProtoRepository::new(pool.get_ref().clone());
    match repo.delete(id.into_inner()).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": "deleted"})),
        Err(e) if crate::api::is_pool_timeout(&e) => crate::api::sqlx_error_response(&e),
        Err(_) => HttpResponse::NotFound().finish(),
    }
}
