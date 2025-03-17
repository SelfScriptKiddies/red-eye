pub mod regex;
pub mod bsubstr;
pub mod substr;
pub mod filterDTO;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use once_cell::sync::Lazy;
use std::sync::RwLock;
use crate::configuration::ApplicationConfig;
use crate::filters::filterDTO::Filter;
use crate::filters::regex::RegexFilter;
use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::substr::SubstrFilter;

// Global storage for filters
pub static PACKET_FILTERS: Lazy<RwLock<Vec<Box<dyn Filter>>>> = Lazy::new(|| {
    RwLock::new(vec![
        Box::new(RegexFilter::new("regex_filter", "GET /\\w*").unwrap()),
        Box::new(BinarySubstrFilter::new("binary_substr_filter", b"1234567890")),
        Box::new(SubstrFilter::new("substr_filter", b"1234567890")),
    ])
});

// Function to add filters
pub fn add_filter(pattern_name: String, pattern_type: String, payload: &[u8]) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    match pattern_type.to_lowercase().as_str() {
        "regex" => {
            let payload_str = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Error converting payload to string: {}", e);
                    return;
                }
            };

            match RegexFilter::new(&pattern_name, &payload_str) {
                Ok(filter) => filters.push(Box::new(filter)),
                Err(e) => error!("Error creating regex filter: {}", e),
            }
        }
        "bsubstr" => {
            filters.push(Box::new(BinarySubstrFilter::new(&pattern_name, &payload)));
        }
        "substr" => {
            filters.push(Box::new(SubstrFilter::new(&pattern_name, &payload)));
        }
        _ => {
            error!("Unsupported filter type: {}", pattern_type);
        }
    }
}

// Function to apply all filters to a packet
pub fn apply_filters(packet: &[u8]) -> Vec<(String, Vec<filterDTO::FilterTrigger>)> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut results: Vec<(String, Vec<filterDTO::FilterTrigger>)> = Vec::new();
    
    for filter in filters.iter() {
        let triggers = filter.processed(packet);
        if !triggers.is_empty() {
            results.push((filter.get_pattern_name(), triggers));
        }
    }
    
    results
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