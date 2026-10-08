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
use crate::filters::{add_filter, edit_filter, remove_filter, sync_filters};
use std::collections::HashSet;
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

    ConfigSync {
        services: Vec<(usize, dto::Service)>,
        filters: Vec<(usize, dto::Filter)>,
    }
}

pub mod dto {
    use bincode::{Encode, Decode};
    // FilterDirection lives with the filter types (filters::filterDTO) so the
    // filters storage can use it without depending on the configuration module.
    pub use crate::filters::filterDTO::FilterDirection;

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
        pub http_parse: bool,
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
                            add_filter(update.1, filter.pattern_name, filter.filter_type, &filter.filter_payload, filter.direction);
                        }
                        SettingsUpdate::FilterUpdate(index, filter) => {
                            debug!("Received filter update: (id={}, filter={:?})", index, filter);
                            edit_filter(index, filter.pattern_name, filter.filter_type, &filter.filter_payload, filter.direction);
                        }
                        SettingsUpdate::FilterRemove(index) => {    
                            debug!("Received filter remove: {}", index);
                            remove_filter(index);
                        }
                        SettingsUpdate::ConfigSync { services, filters } => {
                            info!("Received config sync: {} services, {} filters", 
                                services.len(), filters.len());
                            
                            // Duplicate safety vs the startup DB warm-up
                            // (lib.rs::warm_filters_from_db): sync_filters does
                            // NOT blindly replace the set — it diffs BY ID,
                            // adding only missing ids and removing stale ones.
                            // A ConfigSync carrying the same rows the warm-up
                            // just loaded is therefore a no-op (no duplicates);
                            // this is pinned by tests in filters.rs
                            // (sync_filters_right_after_warmup_does_not_duplicate).
                            // Sync services
                            let incoming_ids: HashSet<usize> = services.iter().map(|(id, _)| *id).collect();
                            {
                                let existing_ids: HashSet<usize>;
                                let missing: Vec<_>;
                                let to_remove: Vec<usize>;
                                
                                // Read lock - проверяем что есть
                                {
                                    let services_cache = cache.services.read().unwrap();
                                    existing_ids = services_cache.keys().cloned().collect();
                                    missing = services.into_iter()
                                        .filter(|(id, _)| !existing_ids.contains(id))
                                        .collect();
                                    to_remove = existing_ids
                                        .difference(&incoming_ids)
                                        .cloned()
                                        .collect();
                                }
                                
                                if !missing.is_empty() || !to_remove.is_empty() {
                                    debug!("Services sync: adding {} missing, removing {} stale", 
                                        missing.len(), to_remove.len());
                                    
                                    // Write lock - вносим изменения
                                    let mut services_cache = cache.services.write().unwrap();
                                    for id in to_remove {
                                        services_cache.remove(&id);
                                    }
                                    for (id, service) in missing {
                                        services_cache.insert(id, service);
                                    }
                                } else {
                                    debug!("Services sync: no changes needed");
                                }
                            }
                            
                            // Sync filters
                            let filters_data: Vec<_> = filters
                                .into_iter()
                                .map(|(id, f)| (id, f.pattern_name, f.filter_type, f.filter_payload, f.direction))
                                .collect();
                            sync_filters(filters_data);
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


