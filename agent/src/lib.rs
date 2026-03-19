use clap::Parser;
use log::{debug, error, info};

mod commands;
mod config;
mod heartbeat;
mod nats;
mod sniffer;
mod updates;

use config::AgentConfig;
use nats::NatsClient;
use updates::{create_ports_cache, create_restart_notifier};

fn init_logging(level: &str) {
    let filter = match level.to_lowercase().as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Info,
    };

    let mut builder = colog::default_builder();
    builder.filter(None, filter);
    builder.init();
}

pub async fn run() {
    let config = AgentConfig::parse();
    init_logging(&config.logging_level);

    let agent_id = config.resolved_agent_id();
    info!("Starting Red Eye Agent '{}'", agent_id);
    debug!("Config: {:?}", config);

    // Connect to NATS
    let nats = match NatsClient::connect(&config, &agent_id).await {
        Ok(n) => n,
        Err(e) => {
            error!("Failed to connect to NATS: {}", e);
            std::process::exit(1);
        }
    };
    info!("Connected to NATS at {}", config.nats_host);

    // Shared port state
    let ports_cache = create_ports_cache(config.ports.clone());
    let (restart_tx, restart_rx) = create_restart_notifier();

    // Start background subsystems
    updates::spawn_updates_listener(nats.clone(), &config, ports_cache.clone(), restart_tx.clone());
    heartbeat::spawn_heartbeat(nats.clone(), &config, ports_cache.clone());
    commands::spawn_command_listener(nats.clone(), ports_cache.clone(), restart_tx.clone());

    // Run sniffer (blocks until shutdown)
    sniffer::run_sniffer_loop(nats, &config, ports_cache, restart_rx).await;
}
