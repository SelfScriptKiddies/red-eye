use async_nats::{
    ConnectOptions, 
    Client, 
    jetstream::{self, stream, consumer::{pull, AckPolicy}}
};
use log::{error, info, warn, debug};
use bincode::{config, Decode, Encode};
use std::time::Duration;
use crate::configuration::ApplicationConfig;
pub use crate::api::updater::dto::FilterDirection;

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
    use crate::api::{
        service::service_dto,
        tag::tag_dto
    };

    #[derive(Debug, Encode, Decode)]
    pub enum FilterDirection {
        Inbound,
        Outbound,
        Both
    }

    #[derive(Debug, Encode, Decode)]
    pub struct Filter {
        pub pattern_name: String,
        pub filter_type: String,
        pub filter_payload: Vec<u8>,
        pub direction: FilterDirection,
    }

    impl From<tag_dto::TagName> for Filter {
        fn from(tag_name: tag_dto::TagName) -> Self {
            Filter {
                pattern_name: tag_name.name,
                filter_type: tag_name.template_type,
                filter_payload: tag_name.template.as_bytes().to_vec(),
                direction: {
                    match tag_name.direction.to_lowercase().as_str() {
                        "in" => FilterDirection::Inbound,
                        "out" => FilterDirection::Outbound,
                        "both" => FilterDirection::Both,
                        _ => FilterDirection::Both,
                    }
                },
            }
        }
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

    impl From<service_dto::Service> for Service {
        fn from(service: service_dto::Service) -> Self {
            Service {
                name: service.name,
                port: service.port.try_into().unwrap(),
                chunked_transfer_encoding: service.chunked_transfer_encoding,
                decrypt_tls: service.decrypt_tls,
                inflate_websockets: service.inflate_websockets,
                merge_adjacent_packets: service.merge_adjacent_packets,
                urldecode: service.urldecode,
            }
        }
    }
}

pub struct UpdateAgent {
    producer: Client,
    topic: String,
}

impl UpdateAgent {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let producer = ConnectOptions::with_user_and_password(
            config.nats_login.clone(), 
            config.nats_password.clone())
            .connect(config.nats_broker.clone())
            .await
            .expect("Error connecting to NATS");
            
        UpdateAgent { 
            producer,
            topic: config.nats_settings_topic.clone(),
        }
    }
    
    async fn send_update(&self, update: SettingsUpdate, index: usize) -> Result<(), String> {
        let payload = match bincode::encode_to_vec((update, index), config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Error serializing: {}", e)),
        };
        
        match self.producer.publish(self.topic.clone(), payload.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error sending: {}", e)),
        }
    }
    
    // Methods for sending different updates
    
    // Services
    pub async fn add_service(&self, service: dto::Service, index: usize) -> Result<(), String> {
        debug!("Sending add service: {:?}", service);
        self.send_update(SettingsUpdate::ServiceAdd(service), index).await
    }
    
    pub async fn update_service(&self, index: usize, service: dto::Service) -> Result<(), String> {
        debug!("Sending update service: (id={}, service={:?})", index, service);
        self.send_update(SettingsUpdate::ServiceUpdate(index, service), index).await
    }
    
    pub async fn remove_service(&self, index: usize) -> Result<(), String> {
        debug!("Sending remove service: (id={})", index);
        self.send_update(SettingsUpdate::ServiceRemove(index), index).await
    }
    
    // Filters
    pub async fn add_filter(&self, filter: dto::Filter, index: usize) -> Result<(), String> {
        debug!("Sending add filter: {:?}", filter);
        self.send_update(SettingsUpdate::FilterAdd(filter), index).await
    }
    
    pub async fn update_filter(&self, index: usize, filter: dto::Filter) -> Result<(), String> {
        debug!("Sending update filter: (id={}, filter={:?})", index, filter);
        self.send_update(SettingsUpdate::FilterUpdate(index, filter), index).await
    }
    
    pub async fn remove_filter(&self, index: usize) -> Result<(), String> {
        debug!("Sending remove filter: (id={})", index);
        self.send_update(SettingsUpdate::FilterRemove(index), index).await
    }
}
