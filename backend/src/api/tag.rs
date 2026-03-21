use actix_identity::Identity;
use actix_web::{get, put, post, delete, web, Responder, HttpResponse};
use serde_json::json;
use crate::api::tag::tag_service::TagService;
use crate::api::tag::tag_dto::Tag;
use crate::api::tag::tag_dto::TagName;
use crate::api::updater::UpdateAgent;

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
    path: web::Path<(i32,)>,
    tag_service: web::Data<TagService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i32 = path.into_inner().0;

    match tag_service.get_tags_by_stream_id(stream_id).await {
        Ok(stream) => Ok(HttpResponse::Ok().json(stream)),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Tags for stream not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    tag_service: web::Data<TagService>
) -> Result<impl Responder, actix_web::error::Error> {
    match tag_service.get_all_tag_names().await {
        Ok(tag_names) => Ok(HttpResponse::Ok().json(tag_names)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    path: web::Path<(i32,)>,
    tag_service: web::Data<TagService>
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.get_tag_name_by_id(id).await {
        Ok(tag_name) => Ok(HttpResponse::Ok().json(tag_name)),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    updater: web::Data<UpdateAgent>
) -> Result<impl Responder, actix_web::error::Error> {
    match tag_service.create_tag_name(tag_name.into_inner()).await {
        Ok(created_tag) => {
            let id = created_tag.id.try_into().unwrap();
            if let Err(e) = updater.add_filter(created_tag.clone().into(), id).await {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::Ok().json(created_tag))
        }
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    path: web::Path<(i32,)>,
    tag_name: web::Json<TagNameCreate>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.update_tag_name(id, tag_name.into_inner()).await {
        Ok(updated_tag) => {
            let id = updated_tag.id.try_into().unwrap();
            if let Err(e) = updater.update_filter(id, updated_tag.clone().into()).await {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::Ok().json(updated_tag))
        }
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    path: web::Path<(i32,)>,
    tag_service: web::Data<TagService>,
    updater: web::Data<UpdateAgent>
) -> Result<impl Responder, actix_web::error::Error> {
    let id = path.into_inner().0;
    match tag_service.delete_tag_name(id).await {
        Ok(_) => {
            if let Err(e) = updater.remove_filter(id.try_into().unwrap()).await {
                log::error!("Failed to send update: {}", e);
            }
            Ok(HttpResponse::NoContent().finish())
        }
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Tag name not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
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
    let minutes = query.get("minutes")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(30);

    match tag_service.get_tag_metrics(minutes).await {
        Ok(data) => {
            let metrics: Vec<serde_json::Value> = data.iter().map(|(tag_id, tag_name, minute_ts, count)| {
                json!({
                    "tag_id": tag_id,
                    "tag_name": tag_name,
                    "timestamp": minute_ts,
                    "count": count
                })
            }).collect();
            HttpResponse::Ok().json(metrics)
        }
        Err(_) => HttpResponse::InternalServerError().finish()
    }
}

pub fn tag_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_tags_by_stream_id)
        .service(get_all_tag_names)
        .service(get_tag_name_by_id)
        .service(create_tag_name)
        .service(update_tag_name)
        .service(delete_tag_name)
        .service(get_tag_metrics);
}