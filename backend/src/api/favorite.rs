use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, delete, get, post, put, web};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub mod favorite_dto;
pub(crate) mod favorite_service;

use favorite_dto::{FavoriteStateResponse, FavoriteUsageResponse};
use favorite_service::{FavoriteLimits, FavoriteService, StarOutcome};

fn service_from(pool: &web::Data<sqlx::PgPool>) -> FavoriteService {
    FavoriteService::new(pool.get_ref().clone())
}

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct UpdateFavoriteRequest {
    pub favorite: bool,
}

/// 409 body for a rejected star: the stream is untouched and no retention
/// guarantee was created.
fn capacity_response(out: StarOutcome) -> HttpResponse {
    if let StarOutcome::CapacityExceeded {
        total_reserved,
        reservation,
        max_total,
    } = out
    {
        HttpResponse::Conflict().json(json!({
            "error": "favorite_capacity_exceeded",
            "detail": "no retention guarantee was created for this stream",
            "max_total_bytes": max_total,
            "reserved_bytes": total_reserved,
            "required_reservation": reservation,
        }))
    } else {
        unreachable!("capacity_response called with a non-capacity outcome")
    }
}

fn state_response(result: Result<StarOutcome, sqlx::Error>) -> HttpResponse {
    match result {
        Ok(StarOutcome::Started(state)) | Ok(StarOutcome::Unchanged(state)) => {
            HttpResponse::Ok().json(state)
        }
        Ok(cap @ StarOutcome::CapacityExceeded { .. }) => capacity_response(cap),
        Err(sqlx::Error::RowNotFound) => {
            HttpResponse::NotFound().json(json!({"error": "Stream not found"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(
    post,
    path = "/api/stream/{id}/favorite",
    responses(
        (status = 200, description = "Star accepted, archive scheduled", body = FavoriteStateResponse),
        (status = 404, description = "Stream not found", body = serde_json::Value),
        (status = 409, description = "Capacity exceeded, stream unchanged", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "stream"
)]
#[post("/stream/{id}/favorite")]
async fn star_stream(
    _identity: Identity,
    path: web::Path<(i64,)>,
    pool: web::Data<sqlx::PgPool>,
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    let stream_id = path.into_inner().0;
    let limits = FavoriteLimits::load(pool.get_ref(), config.get_ref()).await;
    let result = service_from(&pool).star(stream_id, &limits).await;
    state_response(result)
}

#[utoipa::path(
    delete,
    path = "/api/stream/{id}/favorite",
    responses(
        (status = 200, description = "Favorite cleared, saved cleanup scheduled", body = FavoriteStateResponse),
        (status = 404, description = "Stream not found", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "stream"
)]
#[delete("/stream/{id}/favorite")]
async fn unstar_stream(
    _identity: Identity,
    path: web::Path<(i64,)>,
    pool: web::Data<sqlx::PgPool>,
) -> impl Responder {
    let stream_id = path.into_inner().0;
    match service_from(&pool).unstar(stream_id).await {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(sqlx::Error::RowNotFound) => {
            HttpResponse::NotFound().json(json!({"error": "Stream not found"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Legacy PUT alias (older UI/scripts send {"favorite": bool}); delegates
/// to the same server-confirmed star/unstar operations.
#[utoipa::path(
    put,
    path = "/api/stream/{id}/favorite",
    request_body = UpdateFavoriteRequest,
    responses(
        (status = 200, description = "Favorite state updated", body = FavoriteStateResponse),
        (status = 404, description = "Stream not found", body = serde_json::Value),
        (status = 409, description = "Capacity exceeded on star", body = serde_json::Value),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "stream"
)]
#[put("/stream/{id}/favorite")]
async fn update_stream_favorite(
    _identity: Identity,
    path: web::Path<(i64,)>,
    payload: web::Json<UpdateFavoriteRequest>,
    pool: web::Data<sqlx::PgPool>,
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    let stream_id = path.into_inner().0;
    let limits = FavoriteLimits::load(pool.get_ref(), config.get_ref()).await;
    let service = service_from(&pool);
    let result = if payload.favorite {
        service.star(stream_id, &limits).await
    } else {
        service.unstar(stream_id).await.map(StarOutcome::Unchanged)
    };
    state_response(result)
}

#[utoipa::path(
    get,
    path = "/api/favorite/usage",
    responses(
        (status = 200, description = "Favorite storage usage and limits", body = FavoriteUsageResponse),
        (status = 500, description = "Internal server error", body = serde_json::Value)
    ),
    tag = "stream"
)]
#[get("/favorite/usage")]
async fn favorite_usage(
    _identity: Identity,
    pool: web::Data<sqlx::PgPool>,
    config: web::Data<crate::configuration::ApplicationConfig>,
) -> impl Responder {
    let limits = FavoriteLimits::load(pool.get_ref(), config.get_ref()).await;
    match service_from(&pool).usage_response(&limits).await {
        Ok(usage) => HttpResponse::Ok().json(usage),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

pub fn favorite_config(cfg: &mut web::ServiceConfig) {
    cfg.service(star_stream)
        .service(unstar_stream)
        .service(update_stream_favorite)
        .service(favorite_usage);
}
