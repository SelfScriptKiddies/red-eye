use log::{info, debug};
use crate::packet_parser::PacketCatcher;
use config_manager::ConfigInit;
use crate::configuration::ApplicationConfig;

mod configuration;
mod packet_parser;
mod nats;

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

pub async fn run() {
    let config = init();
    debug!("{config:?}");
    let mut packet_capture = PacketCatcher::new(&config).await;

    packet_capture.start().await.expect("TODO: panic message");

    tokio::signal::ctrl_c().await.expect("TODO: panic message");
    info!("Shutdown signal received, exiting.");
    packet_capture.stop_capturing();
}