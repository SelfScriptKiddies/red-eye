use async_nats::{
    Client, 
    jetstream::{self, stream, consumer::{pull, AckPolicy}}
};
use anyhow::Result;
use log::{error, info, debug};
use futures::StreamExt;
use bincode::{config, Decode};
use std::sync::{Arc, RwLock};
use std::collections::HashSet;

#[derive(Debug, Decode)]
pub enum SettingsUpdate {
    // Service updates (используем только порты)
    ServiceAdd(dto::Service),
    ServiceUpdate(usize, dto::Service),
    ServiceRemove(usize),

    // Filter updates (игнорируем в sniffer)
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

#[derive(Debug, Decode, Clone)]
pub struct Service {
    pub name: String,
    pub port: u16,
    pub chunked_transfer_encoding: bool,
    pub decrypt_tls: bool,
    pub inflate_websockets: bool,
    pub merge_adjacent_packets: bool,
    pub urldecode: bool,
}


/// Shared state for ports
pub type PortsCache = Arc<RwLock<HashSet<u16>>>;

/// Channel for notifying about port changes that require restart
pub type RestartNotifier = tokio::sync::watch::Sender<()>;
pub type RestartReceiver = tokio::sync::watch::Receiver<()>;

pub fn create_ports_cache(initial_ports: Vec<u16>) -> PortsCache {
    Arc::new(RwLock::new(initial_ports.into_iter().collect()))
}

pub fn create_restart_notifier() -> (RestartNotifier, RestartReceiver) {
    tokio::sync::watch::channel(())
}

pub struct UpdateListener {
    client: Client,
    uuid: String,
}

impl UpdateListener {
    pub async fn new(
        nats_host: &str,
        nats_login: &str,
        nats_password: &str,
        uuid: String,
    ) -> Result<Self> {
        let client = async_nats::ConnectOptions::with_user_and_password(
            nats_login.to_string(),
            nats_password.to_string()
        )
        .connect(nats_host)
        .await?;
        
        Ok(UpdateListener { client, uuid })
    }

    pub async fn listen_to_updates(
        self,
        subject: &str,
        ports_cache: PortsCache,
        restart_notifier: RestartNotifier,
    ) -> Result<()> {
        info!("Starting to listen for settings updates on topic {}", subject);
        
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

        while let Some(message) = messages.next().await {
            match message {
                Ok(message) => {
                    let payload = message.payload.to_vec();
                    
                    let update: (SettingsUpdate, usize) = match bincode::decode_from_slice(
                        &payload, 
                        config::standard()
                    ) {
                        Ok((u, _)) => u,
                        Err(e) => {
                            error!("Error decoding update: {}", e);
                            continue;
                        }
                    };

                    match update.0 {
                        SettingsUpdate::ServiceAdd(service) => {
                            debug!("Adding port {} for service {}", service.port, service.name);
                            let changed = ports_cache.write().unwrap().insert(service.port);
                            if changed {
                                let _ = restart_notifier.send(());
                                info!("New port {} added, restart signal sent", service.port);
                            }
                        }
                        SettingsUpdate::ServiceUpdate(_, service) => {
                            debug!("Updating port {} for service {}", service.port, service.name);
                            // При апдейте просто добавляем новый порт
                            // Старый порт удалится при ConfigSync если нужно
                            let changed = ports_cache.write().unwrap().insert(service.port);
                            if changed {
                                let _ = restart_notifier.send(());
                                info!("Port {} updated, restart signal sent", service.port);
                            }
                        }
                        SettingsUpdate::ServiceRemove(_) => {
                            // Не можем удалить без знания порта,
                            // полагаемся на ConfigSync
                            debug!("Service removed, waiting for ConfigSync");
                        }
                        SettingsUpdate::ConfigSync { services, .. } => {
                            info!("Received config sync: {} services", services.len());
                            let mut cache = ports_cache.write().unwrap();
                            
                            // Проверяем какие порты есть сейчас
                            let existing_ports: HashSet<u16> = cache.clone();
                            let incoming_ports: HashSet<u16> = services.iter().map(|(_, s)| s.port).collect();
                            
                            // Находим отсутствующие и лишние
                            let missing: Vec<u16> = incoming_ports
                                .difference(&existing_ports)
                                .cloned()
                                .collect();
                            let to_remove: Vec<u16> = existing_ports
                                .difference(&incoming_ports)
                                .cloned()
                                .collect();
                            
                            if missing.is_empty() && to_remove.is_empty() {
                                debug!("Ports sync: no changes needed");
                            } else {
                                debug!("Ports sync: adding {} missing ports, removing {} stale ports", 
                                    missing.len(), to_remove.len());
                                
                                // Удаляем лишние
                                for port in to_remove {
                                    cache.remove(&port);
                                }
                                
                                // Добавляем отсутствующие
                                for port in missing {
                                    cache.insert(port);
                                }
                                
                                // Уведомляем о необходимости перезапуска
                                let _ = restart_notifier.send(());
                                info!("Ports changed, restart signal sent");
                            }
                            
                            debug!("Ports after sync: {:?}", *cache);
                        }
                        // Игнорируем filter updates
                        _ => {}
                    }

                    if let Err(e) = message.ack().await {
                        error!("Error acknowledging message: {}", e);
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

