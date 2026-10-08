use actix_identity::Identity;
use actix_web::{get, put, HttpResponse, Responder, web};
use serde_json::json;
use stream_service::StreamService;
use crate::api::packet::packet_service::PacketService;
use serde::{Deserialize, Deserializer};
use stream_dto::StreamResponse;
use utoipa::IntoParams;
use log::info;
use utoipa::ToSchema;
use std::sync::Arc;
use crate::health::HealthMonitor;

pub mod stream_dto;
mod stream_repository;
pub(crate) mod stream_service;

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateFavoriteRequest {
    pub favorite: bool
}

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
    path: web::Path<(i64,)>,
    stream_service: web::Data<StreamService>,
    packet_service: web::Data<PacketService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id: i64 = path.into_inner().0;

    match stream_service.get_stream_by_id(stream_id).await {
        Ok(stream) => Ok(HttpResponse::Ok().json(stream.to_response(&packet_service).await)),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Stream not found"}))),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
struct StreamQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_comma_separated_numbers")]
    pub tags: Option<Vec<i64>>,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_comma_separated_numbers")]
    pub services: Option<Vec<i64>>,

    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
}

fn deserialize_comma_separated_numbers<'de, D>(deserializer: D) -> Result<Option<Vec<i64>>, D::Error>
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
                .filter_map(|s| s.trim().parse::<i64>().ok())
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
        (status = 200, description = "Streams found", body = Vec<StreamResponse>, example = json!([{"id": 18, "service_id": 1, "service_name": "attention", "tags": [], "first_packet_time": 1738222782348_i64, "last_packet_time": 1738222782392_i64}, {"id": 17, "service_id": 2, "service_name": "number", "tags": [], "first_packet_time": 1738222782295_i64, "last_packet_time": 1738222782343_i64}])),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "stream"
)]
#[get("/stream")]
async fn get_all_streams(
    _identity: Identity,
    stream_service: web::Data<StreamService>,
    health: web::Data<Arc<HealthMonitor>>,
    query: web::Query<StreamQuery>
) -> Result<impl Responder, actix_web::error::Error> {
    info!("Getting all streams with query: {:?}", query);
    let started = std::time::Instant::now();
    let result = stream_service.get_all_streams_with_details(
        query.limit,
        query.offset,
        query.tags.clone(),
        query.services.clone(),
        query.start_time,
        query.end_time,
    ).await;
    // Feed real query latency into the health monitor (reporting only, never gates).
    health.record(started.elapsed());
    match result {
        Ok(streams) => Ok(HttpResponse::Ok().json(streams)),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

#[utoipa::path(
    put,
    path = "/api/stream/{id}/favorite",
    request_body = UpdateFavoriteRequest,
    responses(
        (status = 200, description = "Favorite flag updated", body = serde_json::Value, example = json!({"id": 1, "favorite": true})),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"error": "non authorized"})),
        (status = 404, description = "Stream not found", body = serde_json::Value, example = json!({"error": "Stream not found"})),
        (status = 500, description = "Internal server error", body = serde_json::Value, example = json!({"error": "Internal server error"}))
    ),
    tag = "stream"
)]
#[put("/stream/{id}/favorite")]
async fn update_stream_favorite(
    _identity: Identity,
    path: web::Path<(i64,)>,
    payload: web::Json<UpdateFavoriteRequest>,
    stream_service: web::Data<StreamService>
) -> Result<impl Responder, actix_web::error::Error> {
    let stream_id = path.into_inner().0;
    match stream_service.set_favorite(stream_id, payload.favorite).await {
        Ok(favorite) => Ok(HttpResponse::Ok().json(json!({"id": stream_id, "favorite": favorite}))),
        Err(sqlx::Error::RowNotFound) => Ok(HttpResponse::NotFound().json(json!({"error": "Stream not found"}))),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
struct HistogramQuery {
    pub bucket_secs: Option<i32>,
}

#[utoipa::path(
    get,
    path = "/api/stream/histogram",
    params(HistogramQuery),
    responses(
        (status = 200, description = "Histogram data", body = Vec<stream_dto::HistogramBucket>),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tag = "stream"
)]
#[get("/stream/histogram")]
async fn get_stream_histogram(
    _identity: Identity,
    stream_service: web::Data<StreamService>,
    query: web::Query<HistogramQuery>,
) -> Result<impl Responder, actix_web::error::Error> {
    let bucket_secs = query.bucket_secs.unwrap_or(60);
    match stream_service.get_stream_histogram(bucket_secs).await {
        Ok(buckets) => Ok(HttpResponse::Ok().json(buckets)),
        Err(e) => Ok(crate::api::sqlx_error_response(&e))
    }
}

pub fn stream_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(get_stream_histogram)
        .service(get_all_streams)
        .service(get_stream_by_id)
        .service(update_stream_favorite);
}