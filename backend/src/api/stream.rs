use actix_identity::Identity;
use actix_web::{get, HttpResponse, Responder, web};
use serde_json::json;
use stream_service::StreamService;
use crate::api::packet::packet_service::PacketService;
use serde::Deserialize;

pub mod stream_dto;
mod stream_repository;
pub(crate) mod stream_service;

#[get("/stream/{id}")]
async fn get_stream_by_id(
    _identity: Identity,
    path: web::Path<(i32,)>,
    stream_service: web::Data<StreamService>,
    packet_service: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i32 = path.into_inner().0;

    match stream_service.get_stream_by_id(stream_id).await {
        Ok(stream) => Ok(HttpResponse::Ok().json(stream.to_response(&packet_service).await)),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packet not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    pub limit: Option<i32>,
    pub offset: Option<i32>
}

#[get("/stream")]
async fn get_all_streams(
    _identity: Identity,
    stream_service: web::Data<StreamService>,
    query: web::Query<StreamQuery>
) -> Result<impl Responder, actix_web::error::Error> {
    match stream_service.get_all_streams_with_details(query.limit, query.offset).await {
        Ok(streams) => Ok(HttpResponse::Ok().json(streams)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

pub fn stream_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_all_streams)
        .service(get_stream_by_id);
}