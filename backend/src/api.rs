mod packet;
mod stream;
mod packet_dto;

use actix_web::web;

pub fn api_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(
            web::scope("/api")
                .configure(packet::packet_config)
    );
}