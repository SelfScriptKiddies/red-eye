pub mod auth;
pub(crate) mod packet;
pub mod realtime;
pub mod service;
pub(crate) mod stream;
pub mod tag;
pub mod updater;
use actix_web::web;
use utoipa::OpenApi;

pub fn api_config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api")
            .configure(packet::packet_config)
            .configure(stream::stream_config)
            .configure(tag::tag_config)
            .configure(service::config)
            .configure(realtime::config),
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

        tag::get_tags_by_stream_id,
        tag::get_all_tag_names,
        tag::get_tag_name_by_id,
        tag::create_tag_name,
        tag::update_tag_name,
        tag::delete_tag_name,

        service::get_all_services,
        service::get_service_by_id,
        service::create_service,
        service::update_service,
        service::delete_service
    ),
    components(
        schemas(auth::LoginData, packet::GetPacketsQuery)
    ),
    tags(
        (name = "auth", description = "Authentication endpoints"),
        (name = "packet", description = "Packet endpoints"),
        (name = "stream", description = "Stream endpoints"),
        (name = "tag", description = "Tag endpoints"),
        (name = "service", description = "Service endpoints")
    )

)]
pub struct ApiDoc;
