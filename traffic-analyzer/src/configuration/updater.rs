use crate::cache;
use crate::configuration::ApplicationConfig;
use crate::filters::{add_filter, edit_filter, remove_filter, sync_filters};
use crate::message_broker::create_consumer;
use crate::wire::{self, SettingsEnvelope, WireConfigSync};
use anyhow::Result;
use async_nats::{
    jetstream::{
        self,
        consumer::{pull, AckPolicy},
        stream,
    },
    Client,
};
use futures::StreamExt;
use log::{debug, error, info, trace};
use prost::Message as _;
use std::collections::HashSet;
use std::sync::Arc;

// Wire v4: coordinated between-games upgrade; v3 producers/consumers are
// unsupported and rejected by the version gate below.
pub const SETTINGS_WIRE_VERSION: u32 = 4;

/// JetStream stream carrying settings updates. New name for v4: the v4
/// subject (updates_v4) differs from the v3 one, so history is not migrated.
pub const SETTINGS_STREAM_NAME: &str = "SETTINGS_UPDATES_V4";

/// Configuration history is bounded; snapshots are complete, so expired
/// history is recoverable via warm-up + reconciliation instead of
/// unbounded stream growth.
pub const SETTINGS_STREAM_MAX_AGE_SECS: u64 = 7 * 24 * 3600;

pub mod dto {
    // FilterDirection lives with the filter types (filters::filterDTO) so the
    // filters storage can use it without depending on the configuration module.
    pub use crate::filters::filterDTO::FilterDirection;
    use crate::wire::{WireFilter, WireFilterDirection, WireProtocol, WireService};

    #[derive(Debug, Clone)]
    pub struct Filter {
        pub pattern_name: String,
        pub filter_type: String,
        pub filter_payload: Vec<u8>,
        pub direction: FilterDirection,
    }

    /// Transport protocol of a service. Unknown wire values must never
    /// degrade into a guess (silently treating UDP as TCP would misroute
    /// traffic), so an unrecognized protocol makes the whole service
    /// mapping fail and the caller skips it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TransportProto {
        Tcp,
        Udp,
    }

    impl TransportProto {
        pub fn from_wire(value: i32) -> Option<Self> {
            WireProtocol::try_from(value).ok().map(|p| match p {
                WireProtocol::Tcp => TransportProto::Tcp,
                WireProtocol::Udp => TransportProto::Udp,
            })
        }

        /// DB-facing spelling (service.protocol)
        pub fn as_str(&self) -> &'static str {
            match self {
                TransportProto::Tcp => "TCP",
                TransportProto::Udp => "UDP",
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct Service {
        pub name: String,
        pub port: u16,
        pub protocol: TransportProto,
        pub chunked_transfer_encoding: bool,
        pub decrypt_tls: bool,
        pub inflate_websockets: bool,
        pub merge_adjacent_packets: bool,
        pub urldecode: bool,
        pub http_parse: bool,
    }

    /// None when the wire entry carries data this build cannot interpret
    /// faithfully (unknown protocol enum, port outside 1..=65535): the
    /// caller drops that service instead of reinterpreting it.
    pub fn service_from_wire(service: &WireService) -> Option<Service> {
        let protocol = TransportProto::from_wire(service.protocol)?;
        let port = u16::try_from(service.port).ok()?;
        if port == 0 {
            return None;
        }
        Some(Service {
            name: service.name.clone(),
            port,
            protocol,
            chunked_transfer_encoding: service.chunked_transfer_encoding,
            decrypt_tls: service.decrypt_tls,
            inflate_websockets: service.inflate_websockets,
            merge_adjacent_packets: service.merge_adjacent_packets,
            urldecode: service.urldecode,
            http_parse: service.http_parse,
        })
    }

    pub fn filter_from_wire(filter: &WireFilter) -> Filter {
        Filter {
            pattern_name: filter.pattern_name.clone(),
            filter_type: filter.filter_type.clone(),
            filter_payload: filter.filter_payload.clone(),
            direction: match WireFilterDirection::try_from(filter.direction) {
                Ok(WireFilterDirection::Inbound) => FilterDirection::Inbound,
                Ok(WireFilterDirection::Outbound) => FilterDirection::Outbound,
                _ => FilterDirection::Both,
            },
        }
    }
}

/// Decode the versioned settings envelope.
/// Malformed payloads and version desync are clean errors
fn decode_settings_update(payload: &[u8]) -> Result<SettingsEnvelope, String> {
    let envelope =
        SettingsEnvelope::decode(payload).map_err(|e| format!("protobuf decode failed: {e}"))?;
    if envelope.version != SETTINGS_WIRE_VERSION {
        return Err(format!(
            "unknown settings wire version {} (expected {}) - backend/analyzer skew?",
            envelope.version, SETTINGS_WIRE_VERSION
        ));
    }
    Ok(envelope)
}

pub struct UpdateListener {
    client: Client,
    uuid: String,
}

impl UpdateListener {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let client = create_consumer(config).await.unwrap();

        UpdateListener {
            client,
            uuid: config.service_uuid.clone(),
        }
    }

    pub async fn listen_to_updates(self, subject: &str, cache: Arc<cache::Cache>) -> Result<()> {
        info!("Starting to consume packets from topic {}", subject);
        let js = jetstream::new(self.client.clone());
        let stream = js
            .create_stream(stream::Config {
                // Added version for loud errors
                name: SETTINGS_STREAM_NAME.to_string(),
                max_age: std::time::Duration::from_secs(SETTINGS_STREAM_MAX_AGE_SECS),
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
        info!(
            "Starting to consume settings updates from topic {}",
            &subject
        );

        while let Some(message) = messages.next().await {
            match message {
                Ok(message) => {
                    let payload = message.payload.to_vec();

                    let envelope = match decode_settings_update(&payload) {
                        Ok(e) => e,
                        Err(e) => {
                            error!(
                                "Error decoding settings update ({} bytes): {}",
                                payload.len(),
                                e
                            );
                            // Ack anyway: a message this consumer cannot parse
                            // will never become parseable on retry
                            if let Err(ack_err) = message.ack().await {
                                error!("Error acknowledging undecodable update: {}", ack_err);
                            }
                            continue;
                        }
                    };

                    trace!("Received update: {:?}", envelope);

                    match envelope.update {
                        Some(wire::settings_envelope::Update::ServiceAdd(op)) => {
                            debug!(
                                "Received service add: (id={}, service={:?})",
                                op.id, op.service
                            );
                            if let Some((index, service)) = service_op(op) {
                                cache.services.write().unwrap().insert(index, service);
                            }
                        }
                        Some(wire::settings_envelope::Update::ServiceUpdate(op)) => {
                            debug!(
                                "Received service update: (id={}, service={:?})",
                                op.id, op.service
                            );
                            if let Some((index, service)) = service_op(op) {
                                cache.services.write().unwrap().insert(index, service);
                            }
                        }
                        Some(wire::settings_envelope::Update::ServiceRemove(index)) => {
                            debug!("Received service remove: (id={})", index);
                            cache.services.write().unwrap().remove(&(index as usize));
                        }
                        Some(wire::settings_envelope::Update::FilterAdd(op)) => {
                            debug!(
                                "Received filter add: (id={}, filter={:?})",
                                op.id, op.filter
                            );
                            if let Some((index, filter)) = filter_op(op) {
                                add_filter(
                                    index,
                                    filter.pattern_name,
                                    filter.filter_type,
                                    &filter.filter_payload,
                                    filter.direction,
                                );
                            }
                        }
                        Some(wire::settings_envelope::Update::FilterUpdate(op)) => {
                            debug!(
                                "Received filter update: (id={}, filter={:?})",
                                op.id, op.filter
                            );
                            if let Some((index, filter)) = filter_op(op) {
                                edit_filter(
                                    index,
                                    filter.pattern_name,
                                    filter.filter_type,
                                    &filter.filter_payload,
                                    filter.direction,
                                );
                            }
                        }
                        Some(wire::settings_envelope::Update::FilterRemove(index)) => {
                            debug!("Received filter remove: {}", index);
                            remove_filter(index as usize);
                        }
                        Some(wire::settings_envelope::Update::ConfigSync(sync)) => {
                            apply_config_sync(sync, &cache);
                        }
                        // Agent-targeted snapshots carry the per-agent
                        // machine IP registry (consumed by the analyzer in
                        // a later step); the global view stays ConfigSync.
                        Some(wire::settings_envelope::Update::AgentSnapshot(snapshot)) => {
                            debug!(
                                "Received agent snapshot for '{}' (revision {}), not a global update",
                                snapshot.agent_id, snapshot.revision
                            );
                        }
                        None => {
                            error!("Settings update envelope carries no update variant");
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

fn service_op(op: wire::WireServiceOp) -> Option<(usize, dto::Service)> {
    let id = usize::try_from(op.id).ok()?;
    let service = dto::service_from_wire(op.service.as_ref()?)?;
    Some((id, service))
}

fn filter_op(op: wire::WireFilterOp) -> Option<(usize, dto::Filter)> {
    let id = usize::try_from(op.id).ok()?;
    let filter = dto::filter_from_wire(op.filter.as_ref()?);
    Some((id, filter))
}

fn apply_config_sync(sync: WireConfigSync, cache: &Arc<cache::Cache>) {
    info!(
        "Received config sync: {} services, {} filters",
        sync.services.len(),
        sync.filters.len()
    );

    // Sync services
    let mut incoming_ids: HashSet<usize> = HashSet::new();
    let mut missing: Vec<(usize, dto::Service)> = Vec::new();
    {
        let services_cache = cache.services.read().unwrap();
        for op in &sync.services {
            // Entries this build cannot interpret faithfully (unknown
            // protocol, id or port overflow) are skipped: the service
            // degrades to unknown-service drops instead of being
            // reinterpreted, and periodic reconciliation re-reads the DB.
            let Some((id, service)) = service_op(op.clone()) else {
                debug!(
                    "Skipping service entry id={} that cannot be mapped safely",
                    op.id
                );
                continue;
            };
            incoming_ids.insert(id);
            if !services_cache.contains_key(&id) {
                missing.push((id, service));
            }
        }
        let existing_ids: HashSet<usize> = services_cache.keys().cloned().collect();
        let to_remove: Vec<usize> = existing_ids.difference(&incoming_ids).cloned().collect();

        if !missing.is_empty() || !to_remove.is_empty() {
            debug!(
                "Services sync: adding {} missing, removing {} stale",
                missing.len(),
                to_remove.len()
            );

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
    let filters_data: Vec<(usize, String, String, Vec<u8>, dto::FilterDirection)> = sync
        .filters
        .iter()
        .filter_map(|op| {
            let id = usize::try_from(op.id).ok()?;
            let filter = dto::filter_from_wire(op.filter.as_ref()?);
            Some((
                id,
                filter.pattern_name,
                filter.filter_type,
                filter.filter_payload,
                filter.direction,
            ))
        })
        .collect();
    sync_filters(filters_data);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(update: Option<wire::settings_envelope::Update>) -> Vec<u8> {
        SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update,
        }
        .encode_to_vec()
    }

    fn sample_service_op(id: u64) -> wire::WireServiceOp {
        wire::WireServiceOp {
            id,
            service: Some(wire::WireService {
                name: "team-site".into(),
                port: 8080,
                chunked_transfer_encoding: true,
                decrypt_tls: false,
                inflate_websockets: true,
                merge_adjacent_packets: false,
                urldecode: true,
                http_parse: true,
                id,
                protocol: wire::WireProtocol::Tcp as i32,
                is_global: true,
            }),
        }
    }

    #[test]
    fn decode_roundtrips_what_the_backend_encodes() {
        let bytes = envelope(Some(wire::settings_envelope::Update::ServiceAdd(
            sample_service_op(7),
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
        for wrong in [0u32, 1, 2, 3] {
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

        // A lying field length: field 8 (config_sync, length-delimited) declares ~34 GB.
        let mut lie = envelope(None);
        lie.extend_from_slice(&[0x42, 0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode_settings_update(&lie).is_err());
    }
}
