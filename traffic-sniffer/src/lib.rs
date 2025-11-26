use log::{info, debug, error};
use crate::packet_parser::PacketCatcher;
use config_manager::ConfigInit;
use crate::configuration::ApplicationConfig;

mod configuration;
mod packet_parser;
mod nats;

use crate::configuration::updater::{UpdateListener, create_ports_cache, create_restart_notifier};

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
    
    // Create shared ports cache
    let ports_cache = create_ports_cache(config.ports.0.clone());
    
    // Create restart notifier
    let (restart_sender, mut restart_receiver) = create_restart_notifier();
    
    // Start update listener in background
    let update_listener = UpdateListener::new(
        &config.nats_host,
        &config.nats_login,
        &config.nats_password,
        config.service_uuid.clone(),
    ).await.expect("Failed to create update listener");
    
    let updates_topic = config.nats_updates_topic.clone();
    let ports_cache_clone = ports_cache.clone();
    let restart_sender_clone = restart_sender.clone();
    tokio::spawn(async move {
        if let Err(e) = update_listener
            .listen_to_updates(&updates_topic, ports_cache_clone, restart_sender_clone)
            .await
        {
            log::error!("Update listener error: {}", e);
        }
    });
    
    // Pass ports_cache to packet catcher for dynamic filtering
    let mut packet_capture = PacketCatcher::new(&config, ports_cache.clone()).await;

    packet_capture.start().await.expect("Failed to start packet capture");
    info!("Packet capture started");

    // Main loop: wait for ctrl+c or restart signal
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received, exiting.");
                packet_capture.stop_capturing();
                break;
            }
            result = restart_receiver.changed() => {
                if result.is_err() {
                    // Sender dropped, exit
                    info!("Restart notifier dropped, exiting.");
                    packet_capture.stop_capturing();
                    break;
                }
                
                info!("Ports changed, restarting packet capture...");
                
                // Stop current capture
                packet_capture.stop_capturing();
                
                // Small delay to ensure cleanup
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                
                // Create new packet capture with updated ports
                packet_capture = PacketCatcher::new(&config, ports_cache.clone()).await;
                
                // Restart capture
                match packet_capture.start().await {
                    Ok(_) => {
                        info!("Packet capture restarted successfully with updated ports");
                    }
                    Err(e) => {
                        error!("Failed to restart packet capture: {}", e);
                        // Continue anyway, will retry on next change
                    }
                }
            }
        }
    }
}