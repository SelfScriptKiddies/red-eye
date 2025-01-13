use std::convert::identity;
use actix_identity::Identity;
use actix_session::Session;
use actix_web::{Error, get, HttpMessage, HttpRequest, HttpResponse, post, Responder, web};
use serde::Deserialize;
use serde_json::json;
use crate::configuration::ApplicationConfig;
use log::debug;

#[derive(Debug, Deserialize)]
pub struct LoginData {
    password: String,
}

#[post("/login")]
async fn login(
    request: HttpRequest,
    data: web::Form<LoginData>,
    config: web::Data<ApplicationConfig>
) -> impl Responder {
    if config.web_password == data.password {
        Identity::login(&request.extensions(), "player".into());
        HttpResponse::Ok().json(json!({"status": "Logged in"}))
    } else {
        HttpResponse::Unauthorized().json(json!({"status": "Invalid password"}))
    }
}

#[post("/logout")]
async fn logout(
    identity: Identity
) -> impl Responder {
    identity.logout();
    HttpResponse::Ok().body("Successfully log out")
}

pub fn auth_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(login)
        .service(logout);
}
