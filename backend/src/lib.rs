use std::error::Error;
use actix_identity::IdentityMiddleware;
use actix_session::config::CookieContentSecurity;
use crate::configuration::ApplicationConfig;
use actix_session::storage::CookieSessionStore;
use actix_session::SessionMiddleware;
use actix_web::cookie::Key;
use actix_web::{App, get, HttpResponse, HttpServer, web, Responder};
use actix_web::middleware::Logger;
use config_manager::ConfigInit;
use log::{debug, error};
use crate::db::establish_connection;

mod db;
mod api;
mod auth;
mod configuration;


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
    let web_data = web::Data::new(config.clone());
    let pool = web::Data::new(establish_connection(&config).await);
    debug!("{:?}", &config);

    HttpServer::new(move || {
        App::new()
            .app_data(web_data.clone())
            .app_data(pool.clone())
            .wrap(Logger::default())
            .wrap(IdentityMiddleware::default())
            .wrap(SessionMiddleware::builder(
                CookieSessionStore::default(),
                Key::from(config.cookie_key.clone().as_bytes()),
            ).cookie_content_security(CookieContentSecurity::Private).build())
            .configure(auth::auth_config)
            .configure(api::api_config)
    })
    .bind((config.web_host, config.web_port))?
    .run()
    .await

}


