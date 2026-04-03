pub mod agent;
pub mod auth;
pub mod block_sync;
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
pub mod tag;
pub mod updater;
use actix_web::web;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub fn api_config(cfg: &mut web::ServiceConfig) {
    // Prometheus metrics — outside /api scope, no auth
    metrics::register_metrics();
    cfg
    .configure(metrics::config)
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

        tag::get_tags_by_stream_id,
        tag::get_all_tag_names,
        tag::get_tag_name_by_id,
        tag::create_tag_name,
        tag::update_tag_name,
        tag::delete_tag_name,
        tag::get_tag_metrics,

        service::get_all_services,
        service::get_service_by_id,
        service::create_service,
        service::update_service,
        service::delete_service,

        agent::get_all_agents,
        agent::get_agent_by_id,
        agent::send_agent_command,
        agent::broadcast_agent_command,

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
