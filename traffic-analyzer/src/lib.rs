pub mod configuration;
pub mod kafka;
pub mod filters;
pub mod db;
pub mod ethernet;
pub mod cache;
pub mod process_packet;
use crate::configuration::init;
use log::info;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    println!("{:?}", config);
    

    let config_clone = config.clone();
    let broker = config_clone.kafka_broker.clone();
    tokio::spawn(async move {
        let update_listener = configuration::updater::UpdateListener::new(&broker, &config_clone.service_uuid);
        update_listener.listen_to_updates(&config_clone.kafka_updates_topic).await;
    });

    kafka::consume_raw_packets(kafka::create_consumer(&config.kafka_broker).await.unwrap()).await;
}
