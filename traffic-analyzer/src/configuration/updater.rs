use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use log::{error, info, warn, debug};
use bincode::{config, Decode};

#[derive(Debug, Decode)]
pub enum SettingsUpdate {
    // Service updates
    ServiceAdd(settings_dto::ServiceDTO),
    ServiceUpdate(usize, settings_dto::ServiceDTO),
    ServiceRemove(usize),

    // Filter updates
    FilterAdd(settings_dto::FilterDTO),
    FilterUpdate(usize, settings_dto::FilterDTO),
    FilterRemove(usize),
}

mod settings_dto {
    use bincode::{Encode, Decode};

    #[derive(Debug, Encode, Decode)]
    pub struct FilterDTO {
        pub pattern_name: String,
        pub filter_type: String,
        pub filter_payload: Vec<u8>,
    }

    #[derive(Debug, Encode, Decode)]
    pub struct ServiceDTO {
        pub name: String,
        pub port: u16,
        pub chunked_transfer_encoding: bool,
        pub decrypt_tls: bool,
        pub inflate_websockets: bool,
        pub merge_adjacent_packets: bool,
        pub urldecode: bool,
    }
}

pub async fn create_consumer(brokers: &str, service_uuid: &str) -> KafkaResult<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", service_uuid)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Error creating consumer");

    Ok(consumer)
}

pub async fn listen_to_updates(consumer: StreamConsumer, topic: &str) {
    info!("Starting to consume packets from topic {}", topic);
    consumer.subscribe(&[topic]).expect("Failed to subscribe to topic");

    loop {
        match consumer.recv().await {
            Ok(message) => {
                let payload = match message.payload() {
                    Some(p) => p.to_vec(), // Clone to Vec<u8>
                    None => {
                        warn!("Received message with no payload");
                        // Commit message even if it's empty
                        consumer.commit_message(&message, CommitMode::Async).unwrap();
                        continue;
                    }
                };
                
                let update: (SettingsUpdate, usize) = match bincode::decode_from_slice(&payload, config::standard()) {
                    Ok(u) => u,
                    Err(e) => {
                        error!("Error decoding update: {}", e);
                        continue;
                    }
                };

                debug!("Received update: {:?}", update);
                // Commit message after cloning payload
                consumer.commit_message(&message, CommitMode::Async).unwrap();
            }
            Err(e) => {
                error!("Error receiving message: {}", e);
            }
        }
    }
}

