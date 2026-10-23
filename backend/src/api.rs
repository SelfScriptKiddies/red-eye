pub mod agent;
pub mod auth;
pub mod block_sync;
pub mod download;
pub mod l7;
pub mod metrics;
pub mod protect;
pub mod proto;
pub(crate) mod packet;
pub mod realtime;
pub mod service;
pub(crate) mod stream;
pub mod settings;
pub mod special_triggers;
pub mod stats;
pub mod tag;
pub mod updater;
pub mod version;
use actix_web::http::header::RETRY_AFTER;
use actix_web::web;
use actix_web::HttpResponse;
use serde_json::json;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

/// True when the sqlx error is DB pool exhaustion (timed out acquiring a
/// connection) — an overload condition, not a bug.
pub(crate) fn is_pool_timeout(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::PoolTimedOut)
}

/// 503 Service Unavailable + `Retry-After: 1` for DB pool exhaustion, so
/// clients (and nginx) can distinguish pool overload from real bugs and
/// back off briefly instead of hammering the pool.
pub(crate) fn pool_timeout_response() -> HttpResponse {
    HttpResponse::ServiceUnavailable()
        .insert_header((RETRY_AFTER, "1"))
        .json(json!({"error": "database connection pool exhausted, retry later"}))
}

/// Map a sqlx error to an HTTP response at the handler boundary:
/// pool exhaustion → 503 with `Retry-After: 1`; anything else → 500 with the
/// `{"error": ...}` JSON body shape the 404 arms already use.
pub(crate) fn sqlx_error_response(error: &sqlx::Error) -> HttpResponse {
    if is_pool_timeout(error) {
        pool_timeout_response()
    } else {
        HttpResponse::InternalServerError().json(json!({
            "error": format!("Error occurred: {:?}", error)
        }))
    }
}

pub fn api_config(cfg: &mut web::ServiceConfig) {
    // Prometheus metrics — outside /api scope, no auth
    metrics::register_metrics();
    cfg
    .configure(metrics::config)
    // Agent binary download — outside /api scope, PUBLIC by design
    // (operators wget it before they have credentials; see api/download.rs)
    .configure(download::public_config)
    .service(
        SwaggerUi::new("/api/docs/{_:.*}")
        .url("/api/docs/openapi.json", ApiDoc::openapi())
    )
    .service(
        web::scope("/api")
            .configure(packet::packet_config)
            .configure(stream::stream_config)
            .configure(tag::tag_config)
            .configure(service::config)
            .configure(realtime::config)
            .configure(agent::config)
            .configure(l7::config)
            .configure(settings::config)
            .configure(protect::config)
            .configure(proto::config)
            .configure(stats::config)
            .configure(version::config)
            .configure(download::config)
            .configure(crate::health::config)
            .service(web::redirect("/docs", "/api/docs/"))
    );
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "API",
        version = "1.0.0",
        description = "API for the red-eye traffic analyzer"
    ),
    paths(
        auth::login,
        auth::logout,
        auth::check_authorized,

        packet::get_packet,
        packet::get_packet_by_id,
        packet::get_packets_by_stream_id,

        stream::get_all_streams,
        stream::get_stream_by_id,
        stream::update_stream_favorite,
        stream::get_stream_histogram,

        tag::get_tags_by_stream_id,
        tag::get_all_tag_names,
        tag::get_tag_name_by_id,
        tag::create_tag_name,
        tag::update_tag_name,
        tag::delete_tag_name,
        tag::get_tag_metrics,
        tag::get_flag_balance,

        service::get_all_services,
        service::get_service_by_id,
        service::create_service,
        service::update_service,
        service::delete_service,

        agent::get_all_agents,
        agent::get_agent_by_id,
        agent::send_agent_command,
        agent::broadcast_agent_command,
        agent::get_agent_containers,

        download::download_agent_binary,
        download::get_install_command,

        version::get_versions,

        l7::get_http_messages,
        l7::get_http_message_full,
        l7::get_http_headers,
        l7::search_by_header,
        l7::search_by_headers
    ),
    components(
        schemas(auth::LoginData, packet::GetPacketsQuery)
    ),
    tags(
        (name = "auth", description = "Authentication endpoints"),
        (name = "packet", description = "Packet endpoints"),
        (name = "stream", description = "Stream endpoints"),
        (name = "tag", description = "Tag endpoints"),
        (name = "service", description = "Service endpoints"),
        (name = "agent", description = "Agent management endpoints"),
        (name = "l7", description = "L7 protocol analysis endpoints")
    )

)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;

    #[test]
    fn pool_timeout_maps_to_503_with_retry_after() {
        let resp = sqlx_error_response(&sqlx::Error::PoolTimedOut);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get(RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn pool_timeout_response_directly_sets_header() {
        let resp = pool_timeout_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get(RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn other_sqlx_errors_keep_500_without_retry_after() {
        let resp = sqlx_error_response(&sqlx::Error::RowNotFound);
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(resp.headers().get(RETRY_AFTER).is_none());
    }

    #[test]
    fn is_pool_timeout_detects_only_pool_exhaustion() {
        assert!(is_pool_timeout(&sqlx::Error::PoolTimedOut));
        assert!(!is_pool_timeout(&sqlx::Error::RowNotFound));
        assert!(!is_pool_timeout(&sqlx::Error::PoolClosed));
    }
}
