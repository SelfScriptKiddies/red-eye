use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use log::{error, info, warn, debug};
use bincode::{config, Decode, Encode};
use std::time::Duration;
use crate::configuration::ApplicationConfig;
#[derive(Debug, Decode, Encode)]
pub enum SettingsUpdate {
    // Service updates
    ServiceAdd(dto::Service),
    ServiceUpdate(usize, dto::Service),
    ServiceRemove(usize),

    // Filter updates
    FilterAdd(dto::Filter),
    FilterUpdate(usize, dto::Filter),
    FilterRemove(usize),
}

mod dto {
    use bincode::{Encode, Decode};

    #[derive(Debug, Encode, Decode)]
    pub struct Filter {
        pub pattern_name: String,
        pub filter_type: String,
        pub filter_payload: Vec<u8>,
    }

    #[derive(Debug, Encode, Decode)]
    pub struct Service {
        pub name: String,
        pub port: u16,
        pub chunked_transfer_encoding: bool,
        pub decrypt_tls: bool,
        pub inflate_websockets: bool,
        pub merge_adjacent_packets: bool,
        pub urldecode: bool,
    }
}

pub struct UpdateAgent {
    producer: FutureProducer,
    topic: String,
}

impl UpdateAgent {
    pub fn new(config: &ApplicationConfig) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.kafka_broker.as_str())
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Error creating Kafka producer");
            
        UpdateAgent { 
            producer,
            topic: config.kafka_settings_topic.clone(),
        }
    }
    
    async fn send_update(&self, update: SettingsUpdate, index: usize) -> Result<(), String> {
        let payload = match bincode::encode_to_vec((update, index), config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Error serializing: {}", e)),
        };
        
        match self.producer.send(
            FutureRecord::to(&self.topic)
                .payload(&payload)
                .key("settings"), // Use "settings" as key
            Timeout::After(Duration::from_secs(5)),
        ).await {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(format!("Error sending: {}", e)),
        }
    }
    
    // Methods for sending different updates
    
    // Services
    pub async fn add_service(&self, service: dto::Service, index: usize) -> Result<(), String> {
        debug!("Sending add service: {:?}", service);
        self.send_update(SettingsUpdate::ServiceAdd(service), index).await
    }
    
    pub async fn update_service(&self, index: usize, service: dto::Service, message_index: usize) -> Result<(), String> {
        debug!("Sending update service: (id={}, service={:?})", index, service);
        self.send_update(SettingsUpdate::ServiceUpdate(index, service), message_index).await
    }
    
    pub async fn remove_service(&self, index: usize, message_index: usize) -> Result<(), String> {
        debug!("Sending remove service: (id={})", index);
        self.send_update(SettingsUpdate::ServiceRemove(index), message_index).await
    }
    
    // Filters
    pub async fn add_filter(&self, filter: dto::Filter, index: usize) -> Result<(), String> {
        debug!("Sending add filter: {:?}", filter);
        self.send_update(SettingsUpdate::FilterAdd(filter), index).await
    }
    
    pub async fn update_filter(&self, index: usize, filter: dto::Filter, message_index: usize) -> Result<(), String> {
        debug!("Sending update filter: (id={}, filter={:?})", index, filter);
        self.send_update(SettingsUpdate::FilterUpdate(index, filter), message_index).await
    }
    
    pub async fn remove_filter(&self, index: usize, message_index: usize) -> Result<(), String> {
        debug!("Sending remove filter: (id={})", index);
        self.send_update(SettingsUpdate::FilterRemove(index), message_index).await
    }
}
