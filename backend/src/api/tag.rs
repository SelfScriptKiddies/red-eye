use crate::api::tag::tag_dto::Tag;
use crate::api::tag::tag_dto::TagName;
use crate::api::tag::tag_service::TagService;
use crate::api::updater::UpdateAgent;
use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, delete, get, post, put, web};
use serde_json::json;
/// Map a service error to an HTTP response: validation -> 400 with the
/// reason, pool exhaustion -> 503, everything else -> 500.
fn tag_service_error_response(
    error: crate::api::tag::tag_service::TagServiceError,
) -> HttpResponse {
    match error {
        crate::api::tag::tag_service::TagServiceError::Validation(e) => {
            HttpResponse::BadRequest().json(json!({"error": e.to_string()}))
        }
        crate::api::tag::tag_service::TagServiceError::Sqlx(e) => {
            crate::api::sqlx_error_response(&e)
        }
    }
}

pub(crate) mod tag_dto;
pub(crate) mod tag_repository;
pub(crate) mod tag_service;

use crate::api::tag::tag_dto::TagNameCreate;

#[utoipa::path(
    get,
    path = "/api/tag/stream_id/{id}",
    responses(
        (status = 200, description = "Tags found", body = Vec<Tag>, example = json!([
            {
              "id": 1,
              "name": "property",
              "color": 180,
              "packet_id": 5,
              "tag_id": 3,
              "start_index": 61,
              "end_index": 91
            },
            {
              "id": 2,
              "name": "think",
              "color": 123,
              "packet_id": 8,
              "tag_id": 9,
              "start_index": 51,
              "end_index": 84
            }])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Tags for stream not found", body = serde_json::Value, example = json!({"error": "Tags for stream not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[get("/tag/stream_id/{id}")]
async fn get_tags_by_stream_id(
    _identity: Identity,
    path: web::Path<(i64,)>,
    tag_service: web::Data<TagService>,
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i64 = path.into_inner().0;

    match tag_service.get_tags_by_stream_id(stream_id).await {
        Ok(stream) => Ok(HttpResponse::Ok().json(stream)),
        Err(sqlx::Error::RowNotFound) => {
            Ok(HttpResponse::NotFound().json(json!({"error": "Tags for stream not found"})))
        }
        Err(e) => Ok(crate::api::sqlx_error_response(&e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/tag/names",
    responses(
        (status = 200, description = "Tag names found", body = Vec<TagName>, example = json!([
            {
              "id": 1,
              "name": "authentication",
              "color": 180,
              "template_type": "regex",
              "template": "\\d{3}\\.\\d{3}\\.\\d{3}\\.\\d{3}"
            },
            {
              "id": 2,
              "name": "error",
              "color": 123,
              "template_type": "substring",
              "template": "payload"
            }])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[get("/tag/names")]
async fn get_all_tag_names(
    _identity: Identity,
    tag_service: web::Data<TagService>,
) -> Result<impl Responder, actix_web::error::Error> {
    match tag_service.get_all_tag_names().await {
        Ok(tag_names) => Ok(HttpResponse::Ok().json(tag_names)),
        Err(e) => Ok(crate::api::sqlx_error_response(&e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/tag/names/{id}",
    responses(
        (status = 200, description = "Tag name found", body = TagName, example = json!({
            "id": 1,
            "name": "authentication", 
            "color": 180,
            "template_type": "regex",
            "template": "\\d{3}\\.\\d{3}\\.\\d{3}\\.\\d{3}"
        })),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Tag name not found", body = serde_json::Value, example = json!({"error": "Tag name not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[get("/tag/names/{id}")]
async fn get_tag_name_by_id(
    _identity: Identity,
    path: web::Path<(i64,)>,
    tag_service: web::Data<TagService>,
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.get_tag_name_by_id(id).await {
        Ok(tag_name) => Ok(HttpResponse::Ok().json(tag_name)),
        Err(sqlx::Error::RowNotFound) => {
            Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"})))
        }
        Err(e) => Ok(crate::api::sqlx_error_response(&e)),
    }
}

#[utoipa::path(
    post,
    path = "/api/tag/names",
    request_body = TagNameCreate,
    responses(
        (status = 200, description = "Tag name created", body = TagName),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[post("/tag/names")]
async fn create_tag_name(
    _identity: Identity,
    tag_name: web::Json<TagNameCreate>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>,
) -> Result<impl Responder, actix_web::error::Error> {
    let tag_name = tag_name.into_inner();
    match tag_service.create_tag_name(tag_name).await {
        Ok(created_tag) => {
            // The revision bump inside the transaction already schedules
            // fresh agent snapshots (heartbeat-driven); this publish gives
            // analyzers the new filter immediately. A failed publish stays
            // visible as pending sync, never a rollback.
            let id = u64::try_from(created_tag.id).unwrap_or_default();
            if let Err(e) = updater.add_filter(created_tag.clone().into(), id).await {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::Ok().json(created_tag))
        }
        Err(e) => Ok(tag_service_error_response(e)),
    }
}

#[utoipa::path(
    put,
    path = "/api/tag/names/{id}",
    request_body = TagName,
    responses(
        (status = 200, description = "Tag name updated", body = TagName),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Tag name not found", body = serde_json::Value, example = json!({"error": "Tag name not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[put("/tag/names/{id}")]
async fn update_tag_name(
    _identity: Identity,
    path: web::Path<(i64,)>,
    tag_name: web::Json<TagNameCreate>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>,
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    let tag_name = tag_name.into_inner();
    match tag_service.update_tag_name(id, tag_name).await {
        Ok(updated_tag) => {
            let id = u64::try_from(updated_tag.id).unwrap_or_default();
            if let Err(e) = updater.update_filter(id, updated_tag.clone().into()).await {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::Ok().json(updated_tag))
        }
        Err(e) => Ok(tag_service_error_response(e)),
    }
}

#[utoipa::path(
    delete,
    path = "/api/tag/names/{id}",
    responses(
        (status = 204, description = "Tag name deleted"),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Tag name not found", body = serde_json::Value, example = json!({"error": "Tag name not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    )
)]
#[delete("/tag/names/{id}")]
async fn delete_tag_name(
    _identity: Identity,
    path: web::Path<(i64,)>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>,
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.delete_tag_name(id).await {
        Ok(_) => {
            if let Err(e) = updater
                .remove_filter(u64::try_from(id).unwrap_or_default())
                .await
            {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::NoContent().finish())
        }
        Err(sqlx::Error::RowNotFound) => {
            Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"})))
        }
        Err(e) => Ok(crate::api::sqlx_error_response(&e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/tag/metrics",
    responses(
        (status = 200, description = "Tag trigger metrics per minute"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "tag"
)]
#[get("/tag/metrics")]
async fn get_tag_metrics(
    _identity: Identity,
    tag_service: web::Data<TagService>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let minutes = query
        .get("minutes")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(30);

    match tag_service.get_tag_metrics(minutes).await {
        Ok(data) => {
            let metrics: Vec<serde_json::Value> = data
                .iter()
                .map(|(tag_id, tag_name, minute_ts, count)| {
                    json!({
                        "tag_id": tag_id,
                        "tag_name": tag_name,
                        "timestamp": minute_ts,
                        "count": count
                    })
                })
                .collect();
            HttpResponse::Ok().json(metrics)
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[utoipa::path(
    get,
    path = "/api/tag/flag-balance",
    params(("minutes" = Option<i32>, Query, description = "Lookback window in minutes")),
    responses(
        (status = 200, description = "Flag balance per service"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "tag"
)]
#[get("/tag/flag-balance")]
async fn get_flag_balance(
    _identity: Identity,
    tag_service: web::Data<TagService>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let minutes = query
        .get("minutes")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(5);

    match tag_service.get_flag_balance(minutes).await {
        Ok(data) => {
            let result: Vec<serde_json::Value> = data
                .iter()
                .map(|(service_id, service_name, incoming, outgoing)| {
                    serde_json::json!({
                        "service_id": service_id,
                        "service_name": service_name,
                        "incoming": incoming,
                        "outgoing": outgoing
                    })
                })
                .collect();
            HttpResponse::Ok().json(result)
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Enable/disable a filter without rewriting its definition. Disabled
/// filters stop matching on every consumer via the next update.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct TagEnabledRequest {
    pub enabled: bool,
}

#[utoipa::path(post, path = "/api/tag/names/{id}/enabled",
    responses((status = 200, description = "Enabled state stored"),
              (status = 400, description = "Invalid request"),
              (status = 404, description = "Tag name not found")))]
#[post("/tag/names/{id}/enabled")]
async fn set_tag_name_enabled(
    _identity: Identity,
    path: web::Path<(i64,)>,
    body: web::Json<TagEnabledRequest>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>,
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.set_tag_name_enabled(id, body.enabled).await {
        Ok(_) => {
            // Re-publish the full definition so consumers swap the whole
            // entry (revision bump rides the periodic sync; the FilterUpdate
            // gives analyzers the change immediately).
            if let Ok(tag) = tag_service.get_tag_name_by_id(id).await {
                let wire_id = u64::try_from(id).unwrap_or_default();
                if let Err(e) = updater.update_filter(wire_id, tag.clone().into()).await {
                    log::error!("Failed to send update: {}", e);
                }
            }
            Ok(HttpResponse::Ok().json(json!({"enabled": body.enabled})))
        }
        Err(sqlx::Error::RowNotFound) => {
            Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"})))
        }
        Err(e) => Ok(crate::api::sqlx_error_response(&e)),
    }
}

pub fn tag_config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_tags_by_stream_id)
        .service(get_all_tag_names)
        .service(get_tag_name_by_id)
        .service(create_tag_name)
        .service(update_tag_name)
        .service(set_tag_name_enabled)
        .service(delete_tag_name)
        .service(get_tag_metrics)
        .service(get_flag_balance);
}
