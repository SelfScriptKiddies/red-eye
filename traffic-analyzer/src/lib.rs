pub mod configuration;
pub mod kafka;
pub mod filters;
pub mod db;
pub mod ethernet;
pub mod cache;
pub mod process_packet;
use crate::configuration::init;
use log::info;
use std::sync::Arc;
use std::time::Duration;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    println!("{:?}", config);
    
    let cache = Arc::new(
        cache::Cache::new(config.cache_streams_capacity)
    );

    // Update listener
    tokio::spawn({
        let config = config.clone();
        let cache = cache.clone();
        async move {
            let update_listener = configuration::updater::UpdateListener::new(&config.kafka_broker, &config.service_uuid);
            update_listener.listen_to_updates(&config.kafka_updates_topic, cache).await;
        }
    });

    // Raw packets listener
    let consumer = kafka::create_consumer(&config.kafka_broker).await.unwrap();
    let db = db::DB::init(&config).await.unwrap();
    kafka::consume_raw_packets(consumer, cache, Arc::new(config), db).await;
}
