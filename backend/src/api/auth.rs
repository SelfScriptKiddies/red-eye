use actix_identity::Identity;
use actix_web::{HttpMessage, HttpRequest, HttpResponse, post, Responder, web, Either};
use actix_web::dev::ServiceResponse;
use actix_web::middleware::ErrorHandlerResponse;
use serde::Deserialize;
use serde_json::json;
use crate::configuration::ApplicationConfig;
use log::warn;

#[derive(Debug, Deserialize)]
pub struct LoginData {
    password: String,
}

#[post("/api/login")]
async fn login(
    request: HttpRequest,
    data: Either<web::Json<LoginData>, web::Form<LoginData>>,
    config: web::Data<ApplicationConfig>
) -> impl Responder {
    let input_password = match data {
        Either::Left(json) => json.password.to_owned(),
        Either::Right(form) => form.password.to_owned()
    };

    if config.web_password == input_password {
        let _ = Identity::login(&request.extensions(), "player".into());
        HttpResponse::Ok().json(json!({"status": "Logged in"}))
    } else {
        HttpResponse::Unauthorized().json(json!({"status": "Invalid password"}))
    }
}

#[post("/api/logout")]
async fn logout(
    identity: Identity
) -> impl Responder {
    identity.logout();
    HttpResponse::Ok().body("Successfully log out")
}

#[post("/api/authorized")]
async fn check_authorized(
    identity: Option<Identity>
) -> impl Responder {
    match identity {
        Some(_) => HttpResponse::Ok().json(json!({"status": "authorized"})),
        None => HttpResponse::Unauthorized().json(json!({"status": "anonymous"}))
    }
}

pub fn add_error_body<B>(res: ServiceResponse<B>) -> actix_web::Result<ErrorHandlerResponse<B>> {
    // split service response into request and response components
    let (req, res) = res.into_parts();

    warn!("Unauthorized try to attempt {}", req.path());

    // set body of response to modified body
    let res = res.set_body(json!({"error": "non authorized"}).to_string());

    // modified bodies need to be boxed and placed in the "right" slot
    let res = ServiceResponse::new(req, res)
        .map_into_boxed_body()
        .map_into_right_body();

    Ok(ErrorHandlerResponse::Response(res))
}

pub fn auth_config(cfg: &mut web::ServiceConfig) {
    cfg
        .service(login)
        .service(check_authorized)
        .service(logout);
}
