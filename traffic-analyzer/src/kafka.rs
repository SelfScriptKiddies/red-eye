use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use log::{debug, error, info, warn};
use tokio::task;
use crate::filters::apply_filters;

type LoggingConsumer = StreamConsumer;

pub async fn create_consumer(brokers: &str) -> KafkaResult<LoggingConsumer> {
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", "red-eye-traffic-analyzer")
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Error creating consumer");

    Ok(consumer)
}

pub async fn consume_raw_packets(consumer: LoggingConsumer) {
    info!("Starting to consume packets from topic raw_packets");
    consumer.subscribe(&["raw_packets"]).expect("Failed to subscribe to topic");
    loop {
        match consumer.recv().await {
            Ok(message) => {
                // Get payload and clone it so it can live in a separate task
                let payload = match message.payload() {
                    Some(p) => p.to_vec(), // Clone to Vec<u8>
                    None => {
                        warn!("Received message with no payload");
                        // Commit message even if it's empty
                        consumer.commit_message(&message, CommitMode::Async).unwrap();
                        continue;
                    }
                };

                // Processing packet in a separate task with cloned data
                task::spawn(async move {
                    debug!("Processing packet of size {} bytes: {:?}", payload.len(), payload); 
                    let triggers = apply_filters(&payload);
                    debug!("Triggers: {:?}", triggers);
                    
                });

                // Commit message after cloning payload
                consumer.commit_message(&message, CommitMode::Async).unwrap();
            }
            Err(e) => {
                error!("Error receiving message: {}", e);
            }
        }
    }
}
