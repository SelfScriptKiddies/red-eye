use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use log::{debug, error, info, warn};
use tokio::sync::Mutex;

mod commands;
mod config;
/// Public for the `list_containers` live-verification example.
pub mod docker;
mod harpoon;
mod heartbeat;
mod nats;
mod nftables;
mod packet_synthesis;
mod sniffer;
mod tls_certs;
mod updates;
pub mod version;
mod wire;

use config::AgentConfig;
use harpoon::HarpoonEngine;
use nats::NatsClient;
use updates::{create_ports_cache, create_restart_notifier};

fn init_logging(level: &str) {
    use tracing_subscriber::{fmt, EnvFilter};

    // RUST_LOG takes priority; fallback includes harpoon_core
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{l},harpoon_core={l}", l = level)));

    fmt().with_env_filter(env_filter).with_target(true).init();
}

pub async fn run() {
    let config = AgentConfig::parse();
    init_logging(&config.logging_level);

    // Initialize rustls crypto provider (required by rustls 0.23+)
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Stable identity: AGENT_ID wins, else a persistent id under DATA_DIR
    // (survives machine-IP changes and restarts).
    let agent_id = config.stable_agent_id();
    // Boot identifier: fresh per process start, so the backend can detect
    // restarts of the same agent id and re-send a full snapshot.
    let boot_id = uuid::Uuid::new_v4().to_string();
    info!("Starting Red Eye Agent '{}' (boot {})", agent_id, boot_id);
    debug!("Config: {:?}", config);

    // Initialize TLS CA certificates
    let tls_certs = match tls_certs::ensure_ca_certs(Path::new(&config.data_dir)) {
        Ok(certs) => {
            info!("TLS CA certs ready at {}", config.data_dir);
            Some(certs)
        }
        Err(e) => {
            warn!(
                "Failed to initialize TLS certs: {} (TLS MITM will be unavailable)",
                e
            );
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

    // Shared control-plane state: ports are populated exclusively from
    // AgentConfigSnapshot updates (no PORTS bootstrap), network settings
    // start from the env/CLI bootstrap values.
    let (restart_tx, restart_rx) = create_restart_notifier();
    let sniffer_health = Arc::new(sniffer::SnifferHealth::default());
    let state = Arc::new(updates::AppState {
        ports: create_ports_cache(),
        config: Arc::new(std::sync::RwLock::new(updates::EffectiveConfig {
            interface: config.interface.clone(),
            machine_ip: config.machine_ip.parse().unwrap_or_else(|_| {
                warn!("Invalid machine_ip '{}', using 0.0.0.0", config.machine_ip);
                std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
            }),
            revision: 0,
        })),
        restart_tx: restart_tx.clone(),
        health: sniffer_health.clone(),
        pending_revision: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        applied_revision: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        last_sync_error: Arc::new(std::sync::RwLock::new(None)),
        agent_id: agent_id.clone(),
        boot_id: boot_id.clone(),
        config_source: config.clone(),
    });

    // Harpoon engine with TLS certs and nftables manager
    let tls_available = tls_certs.is_some();
    let harpoon_engine =
        HarpoonEngine::new(state.ports.clone(), state.restart_tx.clone(), tls_certs);
    let harpoon = Arc::new(Mutex::new(harpoon_engine));

    // Capture forwarder: harpoon packets -> NATS (reads per-packet machine
    // IP and rule protocol from the shared state).
    let capture_mgr = harpoon.lock().await.capture_manager();
    let rule_protocols = harpoon.lock().await.rule_protocols();
    harpoon::spawn_capture_forwarder(
        capture_mgr,
        nats.clone(),
        rule_protocols,
        state.config.clone(),
    );

    // Start background subsystems
    updates::spawn_updates_listener(nats.clone(), state.clone());
    heartbeat::spawn_heartbeat(nats.clone(), harpoon.clone(), state.clone(), tls_available);
    commands::spawn_command_listener(nats.clone(), state.clone(), harpoon.clone());

    // SIGTERM (systemctl restart/stop) and SIGINT must clean up nftables
    // before exit, otherwise the table keeps redirecting traffic to a dead
    // proxy port after the agent is gone.
    spawn_signal_handler(harpoon.clone(), nats.clone());

    // Run sniffer (blocks until shutdown)
    sniffer::run_sniffer_loop(
        nats,
        state.ports.clone(),
        restart_rx,
        sniffer_health,
        state.config.clone(),
        state.last_sync_error.clone(),
        state.pending_revision.clone(),
        state.applied_revision.clone(),
    )
    .await;

    // Cleanup: stop harpoon + nftables
    harpoon.lock().await.shutdown().await;
}

/// How long shutdown cleanup may take before we exit anyway.
const SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Listen for SIGTERM/SIGINT and run the same shutdown path as the NATS
/// Shutdown command.
fn spawn_signal_handler(harpoon: Arc<Mutex<HarpoonEngine>>, nats: NatsClient) {
    use tokio::signal::unix::{signal, SignalKind};

    tokio::spawn(async move {
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to install SIGTERM handler: {}", e);
                return;
            }
        };

        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received, shutting down"),
            _ = tokio::signal::ctrl_c() => info!("SIGINT received, shutting down"),
        }

        graceful_shutdown(harpoon, &nats).await;
    });
}

/// Graceful shutdown: stop the proxy engine, remove the nftables table,
/// flush NATS, exit 0. Shared by SIGTERM/SIGINT and the NATS Shutdown
/// command so there is exactly one cleanup path.
///
/// The cleanup work runs on a blocking thread: nftables cleanup shells out
/// to `nft`, and a hung subprocess must not wedge the async runtime (on a
/// single-vCPU box it would also prevent this timeout from ever firing).
/// All cleanup is idempotent, so a timeout followed by exit (and later a
/// repeated `Drop`) cannot double-remove anything.
pub(crate) async fn graceful_shutdown(harpoon: Arc<Mutex<HarpoonEngine>>, nats: &NatsClient) -> ! {
    let nats = nats.clone();
    let rt = tokio::runtime::Handle::current();

    let cleanup = tokio::task::spawn_blocking(move || {
        rt.block_on(async move {
            // Same path as before: stop engine, delete the nftables table.
            harpoon.lock().await.shutdown().await;
            // Push any buffered packets out before we die.
            nats.inner().flush().await
        })
    });

    match tokio::time::timeout(Duration::from_secs(SHUTDOWN_TIMEOUT_SECS), cleanup).await {
        Ok(Ok(Ok(()))) => info!("Shutdown cleanup complete"),
        Ok(Ok(Err(e))) => error!(
            "Shutdown cleanup failed: {} (nftables table may remain, remove with `nft delete table inet red-eye-agent`)",
            e
        ),
        Ok(Err(e)) => error!("Shutdown cleanup task panicked: {}", e),
        Err(_) => error!(
            "Shutdown cleanup timed out after {}s, exiting anyway \
             (nftables table may remain, remove with `nft delete table inet red-eye-agent`)",
            SHUTDOWN_TIMEOUT_SECS
        ),
    }

    std::process::exit(0)
}
