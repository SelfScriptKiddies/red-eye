use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use bincode::Decode;
use futures::StreamExt;
use log::{debug, error, info};

use crate::config::AgentConfig;
use crate::nats::NatsClient;

// Matches the SettingsUpdate enum from the backend (bincode-encoded).
// All fields must be present for bincode deserialization compatibility.
#[derive(Debug, Decode)]
#[allow(dead_code)]
pub enum SettingsUpdate {
    ServiceAdd(ServiceDto),
    ServiceUpdate(usize, ServiceDto),
    ServiceRemove(usize),
    FilterAdd(FilterDto),
    FilterUpdate(usize, FilterDto),
    FilterRemove(usize),
    ConfigSync {
        services: Vec<(usize, ServiceDto)>,
        filters: Vec<(usize, FilterDto)>,
    },
}

#[derive(Debug, Decode, Clone)]
#[allow(dead_code)]
pub struct ServiceDto {
    pub name: String,
    pub port: u16,
    pub chunked_transfer_encoding: bool,
    pub decrypt_tls: bool,
    pub inflate_websockets: bool,
    pub merge_adjacent_packets: bool,
    pub urldecode: bool,
    pub http_parse: bool,
}

#[derive(Debug, Decode, Clone)]
#[allow(dead_code)]
pub struct FilterDto {
    pub pattern_name: String,
    pub filter_type: String,
    pub filter_payload: Vec<u8>,
    pub direction: FilterDirection,
}

#[derive(Debug, Decode, Clone)]
pub enum FilterDirection {
    Inbound,
    Outbound,
    Both,
}

/// Shared set of active ports
pub type PortsCache = Arc<RwLock<HashSet<u16>>>;

pub type RestartNotifier = tokio::sync::watch::Sender<()>;

pub fn create_ports_cache(initial_ports: Vec<u16>) -> PortsCache {
    Arc::new(RwLock::new(initial_ports.into_iter().collect()))
}

pub fn create_restart_notifier() -> (RestartNotifier, tokio::sync::watch::Receiver<()>) {
    tokio::sync::watch::channel(())
}

/// Spawn the settings update listener as a background task
pub fn spawn_updates_listener(
    nats: NatsClient,
    config: &AgentConfig,
    ports_cache: PortsCache,
    restart_tx: RestartNotifier,
) {
    let updates_topic = config.nats_updates_topic.clone();
    let agent_id = nats.agent_id.clone();

    tokio::spawn(async move {
        if let Err(e) =
            listen_for_updates(nats.inner().clone(), &updates_topic, &agent_id, ports_cache, restart_tx)
                .await
        {
            error!("Update listener error: {}", e);
        }
    });
}

async fn listen_for_updates(
    client: async_nats::Client,
    subject: &str,
    consumer_id: &str,
    ports_cache: PortsCache,
    restart_tx: RestartNotifier,
) -> Result<()> {
    info!("Listening for settings updates on '{}'", subject);

    let js = jetstream::new(client);
    let stream = js
        .create_stream(stream::Config {
            name: "SETTINGS_UPDATES".to_string(),
            subjects: vec![subject.to_string()],
            ..Default::default()
        })
        .await?;

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(consumer_id.to_string()),
            filter_subject: subject.to_string(),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;

    let mut messages = consumer.messages().await?;

    while let Some(msg) = messages.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                error!("Error receiving update: {}", e);
                continue;
            }
        };

        let update: SettingsUpdate = match bincode::decode_from_slice(
            &msg.payload,
            bincode::config::standard(),
        ) {
            Ok((u, _)) => u,
            Err(e) => {
                error!("Error decoding update: {}", e);
                continue;
            }
        };

        let changed = apply_update(&update, &ports_cache);

        if changed {
            let _ = restart_tx.send(());
            info!("Ports changed, restart signal sent");
        }

        if let Err(e) = msg.ack().await {
            error!("Error acknowledging message: {}", e);
        }
    }

    Ok(())
}

/// Apply a settings update to the ports cache. Returns true if ports changed.
fn apply_update(update: &SettingsUpdate, ports_cache: &PortsCache) -> bool {
    match update {
        SettingsUpdate::ServiceAdd(svc) => {
            debug!("Service added: {} (port {})", svc.name, svc.port);
            ports_cache.write().unwrap().insert(svc.port)
        }
        SettingsUpdate::ServiceUpdate(_, svc) => {
            debug!("Service updated: {} (port {})", svc.name, svc.port);
            ports_cache.write().unwrap().insert(svc.port)
        }
        SettingsUpdate::ServiceRemove(_) => {
            debug!("Service removed, waiting for ConfigSync");
            false
        }
        SettingsUpdate::ConfigSync { services, .. } => {
            info!("Config sync: {} services", services.len());
            let mut cache = ports_cache.write().unwrap();
            let existing: HashSet<u16> = cache.clone();
            let incoming: HashSet<u16> = services.iter().map(|(_, s)| s.port).collect();

            if existing == incoming {
                debug!("Ports unchanged after sync");
                false
            } else {
                *cache = incoming;
                debug!("Ports after sync: {:?}", *cache);
                true
            }
        }
        // Filter updates don't affect the sniffer
        _ => false,
    }
}
