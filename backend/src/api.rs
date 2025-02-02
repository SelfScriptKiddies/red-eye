pub(crate) mod packet;
pub(crate) mod stream;
pub mod auth;
pub mod tag;
pub mod service;

use utoipa::OpenApi;
use actix_web::web;

pub fn api_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(
            web::scope("/api")
                .configure(packet::packet_config)
                .configure(stream::stream_config)
                .configure(tag::tag_config)
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

        tag::get_tags_by_stream_id
    ),
    components(
        schemas(auth::LoginData, packet::GetPacketsQuery)
    ),
    tags(
        (name = "auth", description = "Authentication endpoints"),
        (name = "packet", description = "Packet endpoints"),
        (name = "stream", description = "Stream endpoints"),
        (name = "tag", description = "Tag endpoints")
    )

)]
pub struct ApiDoc;