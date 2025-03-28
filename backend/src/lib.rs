use crate::configuration::ApplicationConfig;
use crate::db::establish_connection;
use actix_identity::IdentityMiddleware;
use actix_session::config::CookieContentSecurity;
use actix_session::storage::CookieSessionStore;
use actix_session::SessionMiddleware;
use actix_web::cookie::{Key, SameSite};
use actix_web::middleware::{ErrorHandlers, Logger};
use actix_web::{App, HttpServer, web};
use actix_web::http::StatusCode;
use utoipa_swagger_ui::SwaggerUi;
use utoipa::OpenApi;
use config_manager::ConfigInit;
use log::debug;
use api::auth;
use crate::api::{
    packet::packet_service::PacketService, 
    stream::stream_service::StreamService, 
    tag::tag_service::TagService,
    service::service_service::ServiceService,
    updater::UpdateAgent
};


mod api;
mod configuration;
mod db;



fn init() -> ApplicationConfig {
    let config: ApplicationConfig = match ApplicationConfig::parse() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Got error while parsing configuration: {e:?}");
            std::process::exit(1);
        }
    };

    // Possible values: trace|debug|info|warn|error
    let logging_level = match config.logging_level.to_lowercase().as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Trace,
    };

    let mut clog = colog::default_builder();
    clog.filter(None, logging_level);
    clog.init();

    config
}

pub async fn run() -> std::io::Result<()> {
    let config = init();
    let configuration_data = web::Data::new(config.clone());
    let pool = establish_connection(&config).await;
    let packet_service = web::Data::new(PacketService::new_from_pgpool(pool.clone()));
    let stream_service = web::Data::new(StreamService::new_from_pgpool(pool.clone()));
    let tag_service = web::Data::new(TagService::new_from_pgpool(pool.clone()));
    let service_service = web::Data::new(ServiceService::new_from_pgpool(pool.clone()));
    let pg_pool = web::Data::new(pool.clone());
    let updater = web::Data::new(UpdateAgent::new(&config));
    debug!("{:?}", &config);

    HttpServer::new(move || {
        App::new()
            .app_data(pg_pool.clone())
            .app_data(configuration_data.clone())
            .app_data(packet_service.clone())
            .app_data(stream_service.clone())
            .app_data(tag_service.clone())
            .app_data(service_service.clone())
            .app_data(updater.clone())
            .wrap(ErrorHandlers::new().handler(StatusCode::UNAUTHORIZED, auth::add_error_body))
            .wrap(Logger::default())
            .wrap(IdentityMiddleware::default())
            .wrap(
                SessionMiddleware::builder(
                    CookieSessionStore::default(),
                    Key::from(config.cookie_key.clone().as_bytes()),
                )
                .cookie_secure(false)
                .cookie_http_only(false)
                .cookie_same_site(SameSite::Lax)
                .cookie_content_security(CookieContentSecurity::Private)
                .build()
            )
            .configure(auth::auth_config)
            .configure(api::api_config)

            .service(
                SwaggerUi::new("/api-docs/{_:.*}")
                .url("/api-docs/openapi.json", api::ApiDoc::openapi())
            )
    })
    .bind((config.web_host, config.web_port))?
    .run()
    .await
}
