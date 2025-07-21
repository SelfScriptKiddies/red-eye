use async_nats::{
    ConnectOptions, 
    Client, 
    jetstream::{self, stream, consumer::{pull, AckPolicy}}
};
use anyhow::{Context, Result};
use crate::message_broker::create_consumer;
use crate::configuration::ApplicationConfig;
use log::{error, info, debug, trace};
use futures::StreamExt;
use bincode::{config, Decode};
use crate::filters::{add_filter, edit_filter, remove_filter};
use std::sync::Arc;
use crate::cache;

#[derive(Debug, Decode)]
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

pub mod dto {
    use bincode::{Encode, Decode};

    #[derive(Debug, Encode, Decode, Clone)]
    pub enum FilterDirection {
        Inbound,
        Outbound,
        Both
    }

    #[derive(Debug, Encode, Decode, Clone)]
    pub struct Filter {
        pub pattern_name: String,
        pub filter_type: String,
        pub filter_payload: Vec<u8>,
        pub direction: FilterDirection,
    }

    #[derive(Debug, Encode, Decode, Clone)]
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


pub struct UpdateListener {
    client: Client,
    uuid: String
}

impl UpdateListener {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let client = create_consumer(config).await.unwrap();
        
        UpdateListener { 
            client,
            uuid: config.service_uuid.clone()
        }
    }

    pub async fn listen_to_updates(self, subject: &str, cache: Arc<cache::Cache>) -> Result<()> {
        let subject = subject.clone();
        info!("Starting to consume packets from topic {}", subject);
        let js = jetstream::new(self.client.clone()); 
        let stream = js
            .create_stream(stream::Config {
                name: "SETTINGS_UPDATES".to_string(),
                subjects: vec![subject.to_string()],
                ..Default::default()
            })
            .await?;

        let consumer = stream
            .create_consumer(pull::Config {
                durable_name: Some(self.uuid),
                filter_subject: subject.to_string(),
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            })
            .await?;  
    
        let mut messages = consumer.messages().await?; 
        info!("Starting to consume settings updates from topic {}", &subject);
    
        while let Some(message) = messages.next().await {
            match message {
                Ok(message) => {
                    let payload = message.payload.to_vec();
                    
                    let update: (SettingsUpdate, usize) = match bincode::decode_from_slice(&payload, config::standard()) {
                        Ok((u, _)) => u,
                        Err(e) => {
                            error!("Error decoding update: {}", e);
                            continue;
                        }
                    };

                    trace!("Received update: {:?}", update);

                    match update.0 {
                        SettingsUpdate::ServiceAdd(service) => {
                            debug!("Received service add: {:?} with id {}", service, update.1);
                            cache.services.write().unwrap().insert(update.1, service);
                            // add_service(update.1, service);
                        }
                        SettingsUpdate::ServiceUpdate(index, service) => {
                            debug!("Received service update: (id={}, service={:?})", index, service);
                            cache.services.write().unwrap().insert(index, service);
                        }
                        SettingsUpdate::ServiceRemove(index) => {
                            debug!("Received service remove: (id={})", index);
                            cache.services.write().unwrap().remove(&index);
                        }
                        SettingsUpdate::FilterAdd(filter) => {
                            debug!("Received filter add: (id={}, filter={:?})", update.1, filter);
                            add_filter(update.1, filter.pattern_name, filter.filter_type, &filter.filter_payload);
                        }
                        SettingsUpdate::FilterUpdate(index, filter) => {
                            debug!("Received filter update: (id={}, filter={:?})", index, filter);
                            edit_filter(index, filter.pattern_name, filter.filter_type, &filter.filter_payload);
                        }
                        SettingsUpdate::FilterRemove(index) => {    
                            debug!("Received filter remove: {}", index);
                            remove_filter(index);
                        }
                        _ => {
                            debug!("Received unknown update");
                        }
                    }
    
                    // Commit message after cloning payload
                    match message.ack().await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error acknowledging message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving message: {}", e);
                }
            }
        }
        Ok(())
    }        

    
}


