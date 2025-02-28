pub mod configuration;
pub mod kafka;

use crate::configuration::init;
use log::info;

pub async fn run() {
    let config = init();
    info!("Starting Red Eye traffic analyzer");
    println!("{:?}", config);
    kafka::consume_raw_packets(kafka::create_consumer(&config.kafka_broker).await.unwrap()).await;
}
