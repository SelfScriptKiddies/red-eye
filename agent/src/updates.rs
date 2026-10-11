use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull, stream};
use futures::StreamExt;
use log::{debug, error, info};
use prost::Message as _;

use crate::config::AgentConfig;
use crate::nats::NatsClient;
use crate::wire::{self, SettingsEnvelope, WireService};

// Switched to protobuf
pub const SETTINGS_WIRE_VERSION: u32 = 3;

/// Decode the versioned settings envelope and check the version
fn decode_settings_update(payload: &[u8]) -> Result<SettingsEnvelope, String> {
    let envelope =
        SettingsEnvelope::decode(payload).map_err(|e| format!("protobuf decode failed: {e}"))?;
    if envelope.version != SETTINGS_WIRE_VERSION {
        return Err(format!(
            "unknown settings wire version {} (expected {}) - backend/agent skew?",
            envelope.version, SETTINGS_WIRE_VERSION
        ));
    }
    Ok(envelope)
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
        if let Err(e) = listen_for_updates(
            nats.inner().clone(),
            &updates_topic,
            &agent_id,
            ports_cache,
            restart_tx,
        )
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
            // Adding version for loud errors
            name: "SETTINGS_UPDATES_V3".to_string(),
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

        let envelope = match decode_settings_update(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                error!("Error decoding update ({} bytes): {}", msg.payload.len(), e);
                // Ack anyway: a message this consumer cannot parse will never
                // become parseable on retry
                if let Err(ack_err) = msg.ack().await {
                    error!("Error acknowledging undecodable update: {}", ack_err);
                }
                continue;
            }
        };

        let changed = apply_update(&envelope, &ports_cache);

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
fn apply_update(envelope: &SettingsEnvelope, ports_cache: &PortsCache) -> bool {
    match &envelope.update {
        Some(wire::settings_envelope::Update::ServiceAdd(op)) => {
            debug!("Service added: {:?}", op.service);
            service_port(op.service.as_ref())
                .map_or(false, |port| ports_cache.write().unwrap().insert(port))
        }
        Some(wire::settings_envelope::Update::ServiceUpdate(op)) => {
            debug!("Service updated: {:?}", op.service);
            service_port(op.service.as_ref())
                .map_or(false, |port| ports_cache.write().unwrap().insert(port))
        }
        Some(wire::settings_envelope::Update::ServiceRemove(_)) => {
            debug!("Service removed, waiting for ConfigSync");
            false
        }
        Some(wire::settings_envelope::Update::ConfigSync(sync)) => {
            info!("Config sync: {} services", sync.services.len());
            let mut cache = ports_cache.write().unwrap();
            let existing: HashSet<u16> = cache.clone();
            let incoming: HashSet<u16> = sync
                .services
                .iter()
                .filter_map(|op| service_port(op.service.as_ref()))
                .collect();

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

fn service_port(service: Option<&WireService>) -> Option<u16> {
    service.map(|s| s.port as u16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::red_eye::wire::{ConfigSync, ServiceOp};

    fn envelope(update: Option<wire::settings_envelope::Update>) -> Vec<u8> {
        SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update,
        }
        .encode_to_vec()
    }

    fn sample_service(port: u32) -> WireService {
        WireService {
            name: "team-site".into(),
            port,
            chunked_transfer_encoding: false,
            decrypt_tls: false,
            inflate_websockets: false,
            merge_adjacent_packets: false,
            urldecode: false,
            http_parse: false,
        }
    }

    #[test]
    fn decode_roundtrips_what_the_backend_encodes() {
        let bytes = envelope(Some(wire::settings_envelope::Update::ServiceAdd(
            ServiceOp {
                id: 7,
                service: Some(sample_service(8080)),
            },
        )));
        let decoded = decode_settings_update(&bytes).unwrap();

        match decoded.update {
            Some(wire::settings_envelope::Update::ServiceAdd(op)) => {
                assert_eq!(op.id, 7);
                assert_eq!(op.service.unwrap().port, 8080);
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn wrong_version_is_a_clean_error() {
        // All version, except actual, must be rejected by the version gate.
        for wrong in [0u32, 1, 2] {
            let bytes = SettingsEnvelope {
                version: wrong,
                update: None,
            }
            .encode_to_vec();
            assert!(
                decode_settings_update(&bytes).is_err(),
                "version {wrong} passed"
            );
        }
        assert!(decode_settings_update(&[]).is_err());
    }

    #[test]
    fn garbage_and_lying_lengths_error_instead_of_allocating() {
        // Random garbage (bincode-era bytes of the old settings format).
        assert!(decode_settings_update(&[0xfc, 0x28, 0x5c, 0x22, 0x06, 0x00]).is_err());

        // A lying field length: field 8 (config_sync, length-delimited)
        // declares ~34 GB. prost validates the length against the bytes
        // that remain and errors - no oversized allocation is attempted.
        let mut lie = envelope(None);
        lie.extend_from_slice(&[0x42, 0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode_settings_update(&lie).is_err());
    }

    #[test]
    fn apply_update_tracks_ports_from_service_ops() {
        let ports = create_ports_cache(vec![80]);

        let add = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(wire::settings_envelope::Update::ServiceAdd(ServiceOp {
                id: 2,
                service: Some(sample_service(8080)),
            })),
        };
        assert!(apply_update(&add, &ports));
        assert!(ports.read().unwrap().contains(&8080));

        let sync = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(wire::settings_envelope::Update::ConfigSync(ConfigSync {
                services: vec![ServiceOp {
                    id: 1,
                    service: Some(sample_service(80)),
                }],
                filters: vec![],
            })),
        };
        assert!(apply_update(&sync, &ports));
        assert!(!ports.read().unwrap().contains(&8080));
        assert!(ports.read().unwrap().contains(&80));
    }
}
