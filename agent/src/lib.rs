use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use log::{debug, error, info, warn};
use tokio::sync::Mutex;

mod commands;
mod config;
mod harpoon;
mod heartbeat;
mod nats;
mod nftables;
mod packet_synthesis;
mod sniffer;
mod tls_certs;
mod updates;

use config::AgentConfig;
use harpoon::HarpoonEngine;
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

    // Initialize tracing subscriber for harpoon-core logs
    // tracing-log bridge forwards tracing events to the log crate
    let _ = tracing_log::LogTracer::init();
}

pub async fn run() {
    let config = AgentConfig::parse();
    init_logging(&config.logging_level);

    // Initialize rustls crypto provider (required by rustls 0.23+)
    let _ = rustls::crypto::ring::default_provider().install_default();

    let agent_id = config.resolved_agent_id();
    info!("Starting Red Eye Agent '{}'", agent_id);
    debug!("Config: {:?}", config);

    // Initialize TLS CA certificates
    let tls_certs = match tls_certs::ensure_ca_certs(Path::new(&config.data_dir)) {
        Ok(certs) => {
            info!("TLS CA certs ready at {}", config.data_dir);
            Some(certs)
        }
        Err(e) => {
            warn!("Failed to initialize TLS certs: {} (TLS MITM will be unavailable)", e);
            None
        }
    };

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

    // Harpoon engine with TLS certs and nftables manager
    let harpoon_engine = HarpoonEngine::new(
        ports_cache.clone(),
        restart_tx.clone(),
        tls_certs,
    );
    let harpoon = Arc::new(Mutex::new(harpoon_engine));

    // Capture forwarder: harpoon packets → NATS (same format as sniffer)
    let capture_mgr = harpoon.lock().await.capture_manager();
    harpoon::spawn_capture_forwarder(capture_mgr, nats.clone());

    // Start background subsystems
    updates::spawn_updates_listener(
        nats.clone(),
        &config,
        ports_cache.clone(),
        restart_tx.clone(),
    );
    heartbeat::spawn_heartbeat(nats.clone(), &config, ports_cache.clone(), harpoon.clone());
    commands::spawn_command_listener(
        nats.clone(),
        ports_cache.clone(),
        restart_tx.clone(),
        harpoon.clone(),
    );

    // Run sniffer (blocks until shutdown)
    sniffer::run_sniffer_loop(nats, &config, ports_cache, restart_rx).await;

    // Cleanup: stop harpoon + nftables
    harpoon.lock().await.shutdown().await;
}
