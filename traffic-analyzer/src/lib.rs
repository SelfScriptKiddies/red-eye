pub mod configuration;
pub mod message_broker;
pub mod filters;
pub mod db;
pub mod ethernet;
pub mod cache;
pub mod process_packet;
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
