use actix_identity::Identity;
use actix_web::{get, HttpResponse, Responder, web};
use sqlx::PgPool;
use base64::prelude::*;
use log::error;
use serde_json::json;
use crate::api::packet::packet_dto::PacketResponse;
use crate::api::packet::packet_repository::PacketRepository;
use crate::api::packet::packet_service::PacketService;

mod packet_dto;
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

#[get("/packet")]
async fn get_packet(
    _identity: Identity,
    path: web::Path<(u32,)>,
    pool: web::Data<PgPool>
) -> impl Responder {
    let packet_id: u32 = path.into_inner().0;
    HttpResponse::Ok().body(format!("User detail: {packet_id}"))
}

pub fn packet_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_packet)
        .service(get_packet_by_id)
        .service(get_packets_by_stream_id);
}