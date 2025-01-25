use actix_identity::Identity;
use actix_web::{get, HttpResponse, Responder, web};
use serde::Deserialize;
use serde_json::json;
use crate::api::packet::packet_dto::PacketResponse;
use crate::api::packet::packet_service::PacketService;

pub(crate) mod packet_dto;
pub(crate) mod packet_repository;
pub(crate) mod packet_service;

#[get("/packet/{id}")]
async fn get_packet_by_id(
    _identity: Identity,
    path: web::Path<(i32,)>,
    packet_repository: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let packet_id: i32 = path.into_inner().0;

    match packet_repository.get_packet_by_id(packet_id).await {
        Ok(pkt) => Ok(HttpResponse::Ok().json(pkt.to_response())),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packet not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

#[get("/packet/stream_id/{id}")]
async fn get_packets_by_stream_id(
    _identity: Identity,
    path: web::Path<(i32,)>,
    packet_repository: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let packet_id: i32 = path.into_inner().0;

    match packet_repository.get_packets_by_stream_id(packet_id).await {
        Ok(packets) => {
            let response_packets: Vec<PacketResponse> = packets
                .iter()
                .map(|packet| packet.to_response())
                .collect();
            Ok(HttpResponse::Ok().json(json!(response_packets)))
        },
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Packets for stream not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

#[derive(Debug, Deserialize)]
struct GetPacketsQuery {
    limit: Option<u32>,
    last_id: Option<u32>,
}

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
        Err(e) => HttpResponse::InternalServerError().json(json!({
            "error": format!("Error occurred: {:?}", e)
        }))
    }
}

pub fn packet_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_packet)
        .service(get_packet_by_id)
        .service(get_packets_by_stream_id);
}