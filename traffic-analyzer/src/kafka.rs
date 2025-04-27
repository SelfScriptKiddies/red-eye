use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use log::{trace, debug, error, info, warn};
use tokio::task;
use std::sync::Arc;
use crate::cache::Cache;
use crate::ethernet::parse_packet;
use crate::process_packet::PacketProcessor;
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

pub async fn consume_raw_packets(consumer: LoggingConsumer, cache: Arc<Cache>) {
    info!("Starting to consume packets from topic raw_packets");
    consumer.subscribe(&["raw_packets"]).expect("Failed to subscribe to topic");

    let packet_processor = Arc::new(PacketProcessor::new(cache));

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
                let processor_clone = packet_processor.clone();
                task::spawn(async move {
                    trace!("Processing packet of size {} bytes: {:?}", payload.len(), payload); 
                    let parsed_packet = parse_packet(payload);
                    
                    if let Some(packet) = parsed_packet {
                        processor_clone.process_packet(packet).unwrap_or_else(|e| {
                            error!("Error processing packet: {}", e);
                        });
                    }
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
