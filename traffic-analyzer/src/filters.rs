use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use once_cell::sync::Lazy;
use std::sync::RwLock;
use crate::configuration::ApplicationConfig;

pub struct PacketFilter {
    pub pattern_name: String,
    pub filter_function: Box<dyn Fn(&[u8]) -> bool + Send + Sync>,
}

// Global storage for filters
pub static PACKET_FILTERS: Lazy<RwLock<Vec<PacketFilter>>> = Lazy::new(|| {
    RwLock::new(Vec::new())
});

// Function to add filters
pub fn add_filter(name: &str, filter_fn: impl Fn(&[u8]) -> bool + Send + Sync + 'static) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    filters.push(PacketFilter {
        pattern_name: name.to_string(),
        filter_function: Box::new(filter_fn),
    });
}

// Function to apply all filters to a packet
pub fn apply_filters(packet: &[u8]) -> Vec<String> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut tags: Vec<String> = Vec::new();
    for filter in filters.iter() {
        if (filter.filter_function)(packet) {
            tags.push(filter.pattern_name.clone());
        }
    }
    tags
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PacketFilterDTO {
    pub pattern_name: String,
    pub filter_type: String,
    pub filter_payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum SettingsUpdateType {
    FilterAdd(PacketFilterDTO),
    FilterUpdate(usize, PacketFilterDTO),
    FilterRemove(usize),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub service_id: String,
    pub settings: SettingsUpdateType,
}

pub async fn listen_for_settings_updates(config: ApplicationConfig, brokers: &str) {
    let service_uuid = config.service_uuid.clone();
    info!("Starting to listen for settings updates from the settings_update topic");
    
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", service_uuid)
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Error creating Kafka consumer");

    consumer.subscribe(&["settings_update"]).expect("Failed to subscribe to the settings_update topic");
    
    loop {
        match consumer.recv().await {
            Ok(message) => {
                // Get the payload of the message
                let payload = match message.payload() {
                    Some(p) => p,
                    None => {
                        warn!("Received a message with no payload");
                        consumer.commit_message(&message, CommitMode::Async).unwrap();
                        continue;
                    }
                };

                // Try to deserialize the message
                match serde_json::from_slice::<SettingsUpdateType>(payload) {
                    Ok(update) => {
                        // Check if the update is intended for this service
                        info!("Received settings update for this service: {:?}", update);

                    },
                    Err(e) => {
                        debug!("Failed to deserialize the message as SettingsUpdate: {}", e);
                        // This is not a settings update for our service, skipping
                    }
                }

                consumer.commit_message(&message, CommitMode::Async).unwrap();
            },
            Err(e) => {
                error!("Error receiving message: {}", e);
            }
        }
    }
}