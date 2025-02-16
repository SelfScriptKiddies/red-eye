use actix_identity::Identity;
use actix_web::{get, web, Responder, HttpResponse};
use serde_json::json;
use crate::api::tag::tag_service::TagService;
use crate::api::tag::tag_dto::Tag;
use crate::api::tag::tag_dto::TagName;
pub(crate) mod tag_dto;
pub(crate) mod tag_repository;
pub(crate) mod tag_service;

#[utoipa::path(
    get,
    path = "/api/tag/stream_id/{id}",
    responses(
        (status = 200, description = "Tags found", body = Vec<Tag>, example = json!([
            {
              "id": 1,
              "name": "property",
              "packet_id": 5,
              "tag_id": 3,
              "start_index": 61,
              "end_index": 91
            },
            {
              "id": 2,
              "name": "think",
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
              "name": "authentication"
            },
            {
              "id": 2,
              "name": "error"
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



pub fn tag_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_tags_by_stream_id)
        .service(get_all_tag_names);
}