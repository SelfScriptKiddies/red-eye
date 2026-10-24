use crate::configuration::ApplicationConfig;
use actix_identity::Identity;
use actix_web::body::{BodySize, MessageBody};
use actix_web::dev::ServiceResponse;
use actix_web::middleware::ErrorHandlerResponse;
use actix_web::{Either, HttpMessage, HttpRequest, HttpResponse, Responder, get, post, web};
use log::{info, warn};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct LoginData {
    password: String,
}

#[utoipa::path(
    post,
    path = "/api/login",
    request_body = LoginData,
    responses(
        (status = 200, description = "Login successful", body = serde_json::Value, example = json!({"status": "Logged in"})),
        (status = 401, description = "Invalid password", body = serde_json::Value, example = json!({"error": "Invalid password"}))
    )
)]
#[post("/api/login")]
async fn login(
    request: HttpRequest,
    data: Either<web::Json<LoginData>, web::Form<LoginData>>,
    config: web::Data<ApplicationConfig>,
) -> impl Responder {
    let input_password = match data {
        Either::Left(json) => json.password.to_owned(),
        Either::Right(form) => form.password.to_owned(),
    };

    if config.web_password == input_password {
        let _ = Identity::login(&request.extensions(), "player".into());
        HttpResponse::Ok().json(json!({"status": "Logged in"}))
    } else {
        HttpResponse::Unauthorized().json(json!({"error": "Invalid password"}))
    }
}

#[utoipa::path(
    get,
    path = "/api/logout",
    responses(
        (status = 200, description = "Logout successful", body = String, example = "Successfully log out")
    )
)]
#[get("/api/logout")]
async fn logout(identity: Identity) -> impl Responder {
    identity.logout();
    HttpResponse::Ok().body("Successfully log out")
}

#[utoipa::path(
    get,
    path = "/api/authorized",
    responses(
        (status = 200, description = "Authorized successful", body = serde_json::Value, example = json!({"status": "authorized"})),
        (status = 401, description = "Unauthorized", body = serde_json::Value, example = json!({"status": "anonymous"}))
    ),
    tag = ("auth")
)]
#[get("/api/authorized")]
async fn check_authorized(identity: Option<Identity>) -> impl Responder {
    match identity {
        Some(_) => HttpResponse::Ok().json(json!({"status": "authorized"})),
        None => HttpResponse::Unauthorized().json(json!({"status": "anonymous"})),
    }
}

pub fn add_error_body<B>(res: ServiceResponse<B>) -> actix_web::Result<ErrorHandlerResponse<B>>
where
    B: MessageBody + 'static,
{
    // split service response into request and response components
    let (req, res) = res.into_parts();

    warn!("Unauthorized try to attempt {}", req.path());
    let body_size = res.body().size();

    let res = match body_size {
        BodySize::None | BodySize::Sized(0) => {
            info!("zero bytes");
            HttpResponse::Unauthorized()
                .json(json!({"error": "non authorized"}))
                .map_into_boxed_body()
        }
        // Иначе оставляем как есть
        _ => {
            info!("Not zero bytes");
            res.map_into_boxed_body()
        }
    };
    // set body of response to modified body
    // let res = res.set_body(json!({"error": "non authorized"}).to_string());

    // modified bodies need to be boxed and placed in the "right" slot
    let res = ServiceResponse::new(req, res)
        .map_into_boxed_body()
        .map_into_right_body();

    Ok(ErrorHandlerResponse::Response(res))
}

pub fn auth_config(cfg: &mut web::ServiceConfig) {
    cfg.service(login).service(check_authorized).service(logout);
}
