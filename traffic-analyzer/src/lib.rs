pub mod configuration;
pub mod message_broker;
pub mod filters;
pub mod db;
pub mod ethernet;
pub mod cache;
pub mod process_packet;
pub mod http_parser;
use crate::configuration::init;
use log::{info, debug, error};
use std::sync::Arc;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    debug!("{:?}", config);
    
    let cache = Arc::new(
        cache::Cache::new(config.cache_streams_capacity)
    );

    // Periodic cleanup task for unused stream locks (every 5 minutes)
    // This prevents memory accumulation from old stream locks
    tokio::spawn({
        let cache = cache.clone();
        async move {
            use tokio::time::{interval, Duration};
            let mut cleanup_interval = interval(Duration::from_secs(5 * 60)); // 5 minutes
            cleanup_interval.tick().await; // Skip first tick
            
            loop {
                cleanup_interval.tick().await;
                cache.cleanup_unused_locks().await;
                debug!("Cleaned up unused stream locks");
            }
        }
    });

    // Update listener
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let update_listener = configuration::updater::UpdateListener::new(&config).await;
            update_listener.listen_to_updates(&config.nats_updates_topic, cache).await.unwrap();
        }
    });

    // Raw packets listener
    let consumer = message_broker::create_consumer(&config).await.unwrap();
    let db = db::DB::init(&config).await.unwrap();
    match message_broker::consume_raw_packets(consumer, cache, Arc::new(config), db).await {
        Ok(_) => (),
        Err(e) => {
            error!("Error consuming raw packets: {}", e);
        }
    }
}
