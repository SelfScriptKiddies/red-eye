pub mod configuration;
pub mod kafka;
pub mod filters;
pub mod db;
pub mod ethernet;
use crate::configuration::init;
use log::info;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    println!("{:?}", config);
    
    
    let config_clone = config.clone();
    let broker = config_clone.kafka_broker.clone();
    tokio::spawn(async move {
        filters::listen_for_settings_updates(config_clone, &broker).await;
    });

    kafka::consume_raw_packets(kafka::create_consumer(&config.kafka_broker).await.unwrap()).await;
}
