pub(crate) mod packet;
pub(crate) mod stream;
pub mod auth;
pub mod tag;
pub mod service;

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