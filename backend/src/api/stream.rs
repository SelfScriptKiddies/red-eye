use actix_identity::Identity;
use actix_web::{get, HttpResponse, Responder, web};
use serde_json::json;
use stream_service::StreamService;
use crate::api::packet::packet_service::PacketService;
use serde::{Deserialize, Deserializer};
use stream_dto::StreamResponse;
use utoipa::IntoParams;
use log::info;

pub mod stream_dto;
mod stream_repository;
pub(crate) mod stream_service;

#[utoipa::path(
    get,
    path = "/api/stream/{id}",
    responses(
        (status = 200, description = "Stream found", body = StreamResponse, example = json!({
            "id": 1,
            "src_ip": "157.204.59.65",
            "src_port": 15498,
            "dst_ip": "1.159.249.192",
            "dst_port": 23779,
            "protocol": "TCP",
            "active": false,
            "packet_count": 1,
            "packets": [
                {
                    "id": 1,
                    "timestamp": 1738095449035_i64,
                    "direction": "dest",
                    "stream_id": 1,
                    "packet_data": "cGF5bG9hZA=="
                }
            ]
        })),
    (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
    (status = 404, description = "Stream not found", body = serde_json::Value, example = json!({"error": "Stream not found"})),
    (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"})),
    ),
    tag = "stream"
)]
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
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Stream not found"}))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
struct StreamQuery {
    pub limit: Option<i32>,
    pub offset: Option<i32>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_comma_separated_numbers")]
    pub tags: Option<Vec<i32>>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_comma_separated_numbers")]
    pub services: Option<Vec<i32>>
}

fn deserialize_comma_separated_numbers<'de, D>(deserializer: D) -> Result<Option<Vec<i32>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    if let Some(s) = s {
        if s.is_empty() {
            return Ok(Some(vec![]));
        }
        Ok(Some(
            s.split(',')
                .filter_map(|s| s.trim().parse::<i32>().ok())
                .collect()
        ))
    } else {
        Ok(None)
    }
}

#[utoipa::path(
    get,
    path = "/api/stream",
    params(
        StreamQuery
    ),
    responses(
        (status = 200, description = "Streams found", body = Vec<StreamResponse>, example = json!([{"id": 18, "service_id": "attention", "tags": [], "first_packet_time": 1738222782348_i64, "last_packet_time": 1738222782392_i64}, {"id": 17, "service_id": "number", "tags": [], "first_packet_time": 1738222782295_i64, "last_packet_time": 1738222782343_i64}])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "stream"
)]
#[get("/stream")]
async fn get_all_streams(
    _identity: Identity,
    stream_service: web::Data<StreamService>,
    query: web::Query<StreamQuery>
) -> Result<impl Responder, actix_web::error::Error> {
    info!("Getting all streams with query: {:?}", query);
    match stream_service.get_all_streams_with_details(
        query.limit, 
        query.offset, 
        query.tags.clone(), 
        query.services.clone()
    ).await {
        Ok(streams) => Ok(HttpResponse::Ok().json(streams)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(format!("Error occurred: {:?}", e)))
    }
}

pub fn stream_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_all_streams)
        .service(get_stream_by_id);
}