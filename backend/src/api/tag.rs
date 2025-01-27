use actix_identity::Identity;
use actix_web::{get, web, Responder, HttpResponse};
use serde_json::json;
use crate::api::tag::tag_service::TagService;

pub(crate) mod tag_dto;
pub(crate) mod tag_repository;
pub(crate) mod tag_service;

#[get("/tag/stream_id/{id}")]
async fn get_tags_by_stream_id(
    _identity: Identity,
    path: web::Path<(i32,)>,
    tag_service: web::Data<TagService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i32 = path.into_inner().0;

    match tag_service.get_tags_by_stream_id(stream_id).await {
        Ok(stream) => Ok(HttpResponse::Ok().json(stream)),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packet not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

pub fn tag_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_tags_by_stream_id);
}