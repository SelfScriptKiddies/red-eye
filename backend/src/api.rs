mod packet;
mod stream;
mod packet_dto;

use actix_identity::Identity;
use actix_web::{get, HttpRequest, HttpResponse, HttpMessage, Responder, web};

// TODO: REMOVE THIS ROUTE
#[get("/")]
async fn get_login(request: HttpRequest) -> impl Responder {
    let _ = Identity::login(&request.extensions(), "player".into());
    "Hello, world!"
}

pub fn api_config(cfg: &mut web::ServiceConfig) {
    cfg
        .configure(packet::packet_config)
        .service(get_login);
}