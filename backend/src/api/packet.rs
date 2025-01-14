use actix_identity::Identity;
use actix_web::{HttpResponse, get, Responder, web};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use base64::encode;
use crate::api::packet_dto::{Packet, PacketResponse};
use log::error;
use serde_json::json;

#[get("/{id}")]
async fn get_packet_by_id(
    identity: Identity,
    path: web::Path<(i32,)>,
    pool: web::Data<PgPool>
) -> Result<impl Responder, actix_web::error::Error> {
    let packet_id: i32 = path.into_inner().0;

    let packet = sqlx::query_as::<_, Packet>(
            r#"
            SELECT id, timestamp, direction, stream_id, packet_data
            FROM packets
            WHERE id = $1
            "#)
        .bind(packet_id)
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            error!("Database query failed: {}", e);
            actix_web::error::ErrorInternalServerError("Internal Server Error")
        })?;

    match packet {
        Some(pkt) => {
            let packet_data_base64 = encode(&pkt.packet_data);

            let response = PacketResponse {
                id: pkt.id,
                timestamp: pkt.timestamp,
                direction: pkt.direction,
                stream_id: pkt.stream_id,
                packet_data: packet_data_base64,
            };

            Ok(HttpResponse::Ok().json(response))
        },
        None => Ok(HttpResponse::NotFound().json(json!({"error": "Packet not found"}))),
    }
}

// #[get("/")]
// async fn get_packet_by_id(
//     identity: Identity,
//     path: web::Path<(u32,)>,
//     pool: web::Data<PgPool>
// ) -> impl Responder {
//     let packet_id: u32 = path.into_inner().0;
//     HttpResponse::Ok().body(format!("User detail: {packet_id}"))
// }

pub fn packet_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(
            web::scope("/packet")
                .service(get_packet_by_id)
        )

    ;
}