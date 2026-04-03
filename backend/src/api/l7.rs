pub mod l7_dto;
pub mod l7_repository;
pub mod l7_service;

use actix_identity::Identity;
use actix_web::{get, post, web, HttpResponse, Responder};
use serde::Deserialize;

use self::l7_dto::{HeaderSearchQuery, HeaderSearchResult};
use self::l7_service::L7Service;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_http_messages)
        .service(get_http_message_full)
        .service(get_http_headers)
        .service(search_by_header)
        .service(search_by_headers)
        .service(get_grpc_messages);
}

#[utoipa::path(
    get,
    path = "/api/http/stream/{stream_id}",
    responses(
        (status = 200, description = "HTTP messages for stream"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "l7"
)]
#[get("/http/stream/{stream_id}")]
async fn get_http_messages(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    stream_id: web::Path<i32>,
) -> impl Responder {
    match l7_service.get_messages_by_stream(stream_id.into_inner()).await {
        Ok(messages) => HttpResponse::Ok().json(messages),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[utoipa::path(
    get,
    path = "/api/http/message/{id}",
    responses(
        (status = 200, description = "HTTP message with headers"),
        (status = 404, description = "Message not found"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "l7"
)]
#[get("/http/message/{id}")]
async fn get_http_message_full(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    id: web::Path<i32>,
) -> impl Responder {
    match l7_service.get_message_full(id.into_inner()).await {
        Ok(msg) => HttpResponse::Ok().json(msg),
        Err(sqlx::Error::RowNotFound) => HttpResponse::NotFound().finish(),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[utoipa::path(
    get,
    path = "/api/http/headers/{message_id}",
    responses(
        (status = 200, description = "HTTP headers for message"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "l7"
)]
#[get("/http/headers/{message_id}")]
async fn get_http_headers(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    message_id: web::Path<i32>,
) -> impl Responder {
    match l7_service.get_headers(message_id.into_inner()).await {
        Ok(headers) => HttpResponse::Ok().json(headers),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[utoipa::path(
    get,
    path = "/api/http/search",
    params(HeaderSearchQuery),
    responses(
        (status = 200, description = "Streams matching header criteria"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "l7"
)]
#[get("/http/search")]
async fn search_by_header(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    query: web::Query<HeaderSearchQuery>,
) -> impl Responder {
    let limit = query.limit.min(500);
    match l7_service
        .search_by_header(&query.name, query.value.as_deref(), limit)
        .await
    {
        Ok((stream_ids, total)) => {
            HttpResponse::Ok().json(HeaderSearchResult { stream_ids, total })
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

/// Search by multiple headers (AND logic)
/// Body: [{"name": "Host", "value": "example.com"}, {"name": "Cookie"}]
#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct MultiHeaderFilter {
    name: String,
    value: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/http/search/multi",
    responses(
        (status = 200, description = "Streams matching all header criteria (AND)"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "l7"
)]
#[post("/http/search/multi")]
async fn search_by_headers(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    body: web::Json<Vec<MultiHeaderFilter>>,
) -> impl Responder {
    let filters: Vec<(String, Option<String>)> = body
        .into_inner()
        .into_iter()
        .map(|f| (f.name, f.value))
        .collect();

    match l7_service.search_by_headers(&filters, 100).await {
        Ok(stream_ids) => {
            let total = stream_ids.len() as i64;
            HttpResponse::Ok().json(HeaderSearchResult { stream_ids, total })
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[utoipa::path(get, path = "/api/grpc/stream/{stream_id}", tag = "l7",
    responses((status = 200, description = "gRPC messages for stream")))]
#[get("/grpc/stream/{stream_id}")]
async fn get_grpc_messages(
    _identity: Identity,
    l7_service: web::Data<L7Service>,
    stream_id: web::Path<i32>,
) -> impl Responder {
    match l7_service.get_grpc_messages(stream_id.into_inner()).await {
        Ok(messages) => HttpResponse::Ok().json(messages),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}
