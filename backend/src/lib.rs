use actix_identity::IdentityMiddleware;
use actix_session::config::CookieContentSecurity;
use crate::configuration::ApplicationConfig;
use actix_session::storage::CookieSessionStore;
use actix_session::SessionMiddleware;
use actix_web::cookie::Key;
use actix_web::{App, HttpResponse, HttpServer, web};
use config_manager::ConfigInit;
use log::{debug, error};

mod auth;
mod db;
mod configuration;

fn init() -> ApplicationConfig {
    let config: ApplicationConfig = match ApplicationConfig::parse() {
        Ok(config) => config,
        Err(e) => {
            error!("Got error while parsing configuration: {e:?}");
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
    debug!("{:?}", config);

    HttpServer::new(move || {
        App::new()
            .app_data(web_data.clone())
            .wrap(IdentityMiddleware::default())
            .wrap(SessionMiddleware::builder(
                CookieSessionStore::default(),
                Key::from(config.cookie_key.clone().as_bytes()),
            ).cookie_content_security(CookieContentSecurity::Private).build())
            .configure(auth::auth_config)
    })
    .bind((config.web_host, config.web_port))?
    .run()
    .await
}


