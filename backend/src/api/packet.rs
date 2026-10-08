use actix_identity::Identity;
use actix_web::{get, HttpResponse, Responder, web};
use serde::Deserialize;
use serde_json::json;
use crate::api::packet::packet_dto::PacketResponse;
use crate::api::packet::packet_service::PacketService;
use utoipa::IntoParams;

pub(crate) mod packet_dto;
pub(crate) mod packet_repository;
pub(crate) mod packet_service;

#[utoipa::path(
    get,
    path = "/api/packet/{id}",
    responses(
        (status = 200, description = "Packet found", body = PacketResponse, example = json!({"id": 1, "direction": "dest", "stream_id": 1, "timestamp": 1738095449035_i64, "data": "cGF5bG9hZA=="})),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Packet not found", body = serde_json::Value, example = json!({"error": "Packet not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "packet"
)]
#[get("/packet/{id}")]
async fn get_packet_by_id(
    _identity: Identity,
    path: web::Path<(i64,)>,
    packet_repository: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let packet_id: i64 = path.into_inner().0;

    match packet_repository.get_packet_by_id(packet_id).await {
        Ok(pkt) => Ok(HttpResponse::Ok().json(pkt.to_response())),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packet not found"}))),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

#[utoipa::path(
    get,
    path = "/api/packet/stream_id/{id}",
    responses(
        (status = 200, description = "Packets found", body = Vec<PacketResponse>, example = json!([{"id": 1, "direction": "dest", "stream_id": 1, "timestamp": 1738095449035_i64, "data": "cGF5bG9hZA=="}, {"id": 2, "direction": "src", "stream_id": 1, "timestamp": 1738095449036_i64, "data": "cGF5bG9hZA=="}])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Packets for stream not found", body = serde_json::Value, example = json!({"error": "Packets for stream not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "packet"
)]
#[get("/packet/stream_id/{id}")]
async fn get_packets_by_stream_id(
    _identity: Identity,
    path: web::Path<(i64,)>,
    packet_repository: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i64 = path.into_inner().0;

    match packet_repository.get_packets_by_stream_id(stream_id).await {
        Ok(packets) => {
            let response_packets: Vec<PacketResponse> = packets
                .iter()
                .map(|packet| packet.to_response())
                .collect();
            Ok(HttpResponse::Ok().json(json!(response_packets)))
        },
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packets for stream not found"}))),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema, IntoParams)]
pub struct GetPacketsQuery {
    limit: Option<u32>,
    last_id: Option<u32>,
}

#[utoipa::path(
    get,
    path = "/api/packet",
    params(
        GetPacketsQuery
    ),
    responses(
        (status = 200, description = "Packets found", body = Vec<PacketResponse>, example = json!([{"id": 1, "direction": "dest", "stream_id": 1, "timestamp": 1738095449035_i64, "data": "cGF5bG9hZA=="}, {"id": 2, "direction": "src", "stream_id": 1, "timestamp": 1738095449036_i64, "data": "cGF5bG9hZA=="}])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "packet"
)]
#[get("/packet")]
async fn get_packet(
    _identity: Identity,
    query: web::Query<GetPacketsQuery>,
    packet_service: web::Data<PacketService>
) -> impl Responder {
    let limit = query.limit.unwrap_or(100).min(1000);
    let last_id = query.last_id;
    
    match packet_service.get_packets(limit, last_id).await {
        Ok(packets) => {
            let response_packets: Vec<PacketResponse> = packets
                .iter()
                .map(|packet| packet.to_response())
                .collect();
            HttpResponse::Ok().json(json!(response_packets))
        },
        Err(e) => crate::api::sqlx_error_response(&e)
    }
}

pub fn packet_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_packet)
        .service(get_packet_by_id)
        .service(get_packets_by_stream_id);
}