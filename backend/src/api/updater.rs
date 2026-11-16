use crate::configuration::ApplicationConfig;
use crate::wire::{
    self, SettingsEnvelope, WireAgentConfigSnapshot, WireConfigSync, WireFilter,
    WireFilterDirection, WireFilterOp, WireProtocol, WireService, WireServiceOp,
};
use async_nats::{ConnectOptions, jetstream};
use log::{debug, error, info};
use prost::Message as _;

// Wire v4: coordinated between-games upgrade; v3 producers/consumers are
// unsupported and rejected by the version gate on every consumer.
pub const SETTINGS_WIRE_VERSION: u32 = 4;

/// JetStream stream carrying settings updates. New name for v4: the old
/// SETTINGS_UPDATES_V3 stream keeps its subject, so the v4 stream must own
/// a different one (see the updates_v4 default topic).
pub const SETTINGS_STREAM_NAME: &str = "SETTINGS_UPDATES_V4";

/// Configuration history is bounded: snapshots are complete, so expired
/// history is recoverable via warm-up + periodic reconciliation instead of
/// unbounded stream growth.
pub const SETTINGS_STREAM_MAX_AGE_SECS: u64 = 7 * 24 * 3600;

pub(crate) fn wire_protocol(protocol: &str) -> WireProtocol {
    if protocol.eq_ignore_ascii_case("udp") {
        WireProtocol::Udp
    } else {
        WireProtocol::Tcp
    }
}

impl From<crate::api::service::service_dto::Service> for WireService {
    fn from(service: crate::api::service::service_dto::Service) -> Self {
        WireService {
            name: service.name,
            port: (service.port as u16) as u32,
            chunked_transfer_encoding: service.chunked_transfer_encoding,
            decrypt_tls: service.decrypt_tls,
            inflate_websockets: service.inflate_websockets,
            merge_adjacent_packets: service.merge_adjacent_packets,
            urldecode: service.urldecode,
            http_parse: service.http_parse,
            id: u64::try_from(service.id).unwrap_or_default(),
            protocol: wire_protocol(&service.protocol) as i32,
            is_global: service.is_global,
        }
    }
}

impl From<crate::api::tag::tag_dto::TagName> for WireFilter {
    fn from(tag_name: crate::api::tag::tag_dto::TagName) -> Self {
        let (filter_type, filter_payload, ip_network, ip_endpoint, _expression_source) =
            match tag_name.template_type.as_str() {
                // ip_cidr: the pattern is the normalized CIDR; endpoint
                // rides its own field.
                "ip_cidr" => (
                    "ip_cidr".to_string(),
                    Vec::new(),
                    tag_name.ip_network.clone().unwrap_or_default(),
                    match tag_name.ip_endpoint.as_deref() {
                        Some("dst") => wire::WireIpEndpoint::Dst as i32,
                        _ => wire::WireIpEndpoint::Src as i32,
                    },
                    String::new(),
                ),
                // expression: the source text travels as the payload bytes.
                "expression" => (
                    "expression".to_string(),
                    tag_name
                        .expression_source
                        .clone()
                        .unwrap_or_default()
                        .into_bytes(),
                    String::new(),
                    wire::WireIpEndpoint::Src as i32,
                    String::new(),
                ),
                other => (
                    other.to_string(),
                    tag_name.template.as_bytes().to_vec(),
                    String::new(),
                    wire::WireIpEndpoint::Src as i32,
                    String::new(),
                ),
            };

        WireFilter {
            pattern_name: tag_name.name,
            filter_type,
            filter_payload,
            direction: match tag_name.direction.to_lowercase().as_str() {
                "in" => WireFilterDirection::Inbound as i32,
                "out" => WireFilterDirection::Outbound as i32,
                _ => WireFilterDirection::Both as i32,
            },
            enabled: tag_name.enabled,
            action: tag_name.action,
            revision: u64::try_from(tag_name.revision).unwrap_or_default(),
            ip_network,
            ip_endpoint,
            expires_at_ms: tag_name
                .expires_at
                .map(|t| t.timestamp_millis().max(0) as u64)
                .unwrap_or_default(),
            services_all: tag_name.service_targets_global,
            service_ids: tag_name
                .service_ids
                .iter()
                .map(|&s| u64::try_from(s).unwrap_or_default())
                .collect(),
            agents_all: tag_name.agent_targets_global,
            agent_ids: tag_name.agent_ids.clone(),
            threshold: tag_name.threshold_count.map(|count| wire::WireThreshold {
                count: count.max(0) as u32,
                window_seconds: tag_name.threshold_window_secs.unwrap_or(0).max(0) as u32,
                group_by: tag_name.threshold_group_by.clone().unwrap_or_default(),
                cooldown_seconds: tag_name.threshold_cooldown_secs.unwrap_or(0).max(0) as u32,
                block_seconds: tag_name.threshold_block_secs.unwrap_or(0).max(0) as u32,
            }),
        }
    }
}

pub struct UpdateAgent {
    js: jetstream::Context,
    topic: String,
}

impl UpdateAgent {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let client = ConnectOptions::with_user_and_password(
            config.nats_login.clone(),
            config.nats_password.clone(),
        )
        .connect(config.nats_broker.clone())
        .await
        .expect("Error connecting to NATS");

        let js = jetstream::new(client);
        let topic = config.nats_settings_topic.clone();

        // Stream will be created by consumers, we just publish to the subject
        UpdateAgent { js, topic }
    }

    async fn publish(&self, update: wire::settings_envelope::Update) -> Result<(), String> {
        let envelope = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(update),
        };
        let payload = envelope.encode_to_vec();

        match self.js.publish(self.topic.clone(), payload.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error sending: {}", e)),
        }
    }

    // Methods for sending different updates

    // Services
    pub async fn add_service(&self, service: WireService, index: u64) -> Result<(), String> {
        debug!("Sending add service: {:?}", service);
        self.publish(wire::settings_envelope::Update::ServiceAdd(WireServiceOp {
            id: index,
            service: Some(service),
        }))
        .await
    }

    pub async fn update_service(&self, index: u64, service: WireService) -> Result<(), String> {
        debug!(
            "Sending update service: (id={}, service={:?})",
            index, service
        );
        self.publish(wire::settings_envelope::Update::ServiceUpdate(
            WireServiceOp {
                id: index,
                service: Some(service),
            },
        ))
        .await
    }

    pub async fn remove_service(&self, index: u64) -> Result<(), String> {
        debug!("Sending remove service: (id={})", index);
        self.publish(wire::settings_envelope::Update::ServiceRemove(index))
            .await
    }

    // Filters
    pub async fn add_filter(&self, filter: WireFilter, index: u64) -> Result<(), String> {
        debug!("Sending add filter: {:?}", filter);
        self.publish(wire::settings_envelope::Update::FilterAdd(WireFilterOp {
            id: index,
            filter: Some(filter),
        }))
        .await
    }

    pub async fn update_filter(&self, index: u64, filter: WireFilter) -> Result<(), String> {
        debug!("Sending update filter: (id={}, filter={:?})", index, filter);
        self.publish(wire::settings_envelope::Update::FilterUpdate(
            WireFilterOp {
                id: index,
                filter: Some(filter),
            },
        ))
        .await
    }

    pub async fn remove_filter(&self, index: u64) -> Result<(), String> {
        debug!("Sending remove filter: (id={})", index);
        self.publish(wire::settings_envelope::Update::FilterRemove(index))
            .await
    }

    pub async fn sync_config(
        &self,
        services: Vec<(u64, WireService)>,
        filters: Vec<(u64, WireFilter)>,
    ) -> Result<(), String> {
        info!(
            "Sending config sync: {} services, {} filters",
            services.len(),
            filters.len()
        );

        let update = wire::settings_envelope::Update::ConfigSync(WireConfigSync {
            services: services
                .into_iter()
                .map(|(id, service)| WireServiceOp {
                    id,
                    service: Some(service),
                })
                .collect(),
            filters: filters
                .into_iter()
                .map(|(id, filter)| WireFilterOp {
                    id,
                    filter: Some(filter),
                })
                .collect(),
        });

        self.publish(update).await
    }

    /// Publish a complete per-agent configuration snapshot. Fire-and-forget:
    /// a failure leaves the change as pending sync (reconciliation or the
    /// agent's next heartbeat re-delivers), never a rollback.
    pub async fn publish_agent_snapshot(&self, snapshot: WireAgentConfigSnapshot) -> bool {
        debug!(
            "Sending agent snapshot: agent={} revision={} services={}",
            snapshot.agent_id,
            snapshot.revision,
            snapshot.services.len()
        );
        self.publish(wire::settings_envelope::Update::AgentSnapshot(snapshot))
            .await
            .is_ok()
    }
}

/// Запускает периодическую синхронизацию конфигурации каждые 5 минут
pub async fn start_config_sync_scheduler(
    updater: std::sync::Arc<UpdateAgent>,
    service_service: std::sync::Arc<crate::api::service::service_service::ServiceService>,
    tag_service: std::sync::Arc<crate::api::tag::tag_service::TagService>,
    control: std::sync::Arc<crate::api::agent::agent_config::ControlPlane>,
    agent_service: std::sync::Arc<crate::api::agent::agent_service::AgentService>,
) {
    use tokio::time::{Duration, interval};

    let mut interval = interval(Duration::from_secs(5 * 60)); // 5 минут
    interval.tick().await; // Пропускаем первый тик, чтобы не отправлять сразу при старте

    loop {
        interval.tick().await;

        // Получаем все сервисы
        let services = match service_service.get_all_services().await {
            Ok(services) => services,
            Err(e) => {
                error!("Failed to get services for config sync: {}", e);
                continue;
            }
        };

        // Получаем все теги (фильтры)
        let tag_names = match tag_service.get_all_tag_names().await {
            Ok(tags) => tags,
            Err(e) => {
                error!("Failed to get tag names for config sync: {}", e);
                continue;
            }
        };

        // Конвертируем сервисы
        let services_data: Vec<(u64, WireService)> = services
            .into_iter()
            .map(|s| (u64::try_from(s.id).unwrap_or_default(), s.into()))
            .collect();

        // Конвертируем теги в фильтры
        let filters_data: Vec<(u64, WireFilter)> = tag_names
            .into_iter()
            .map(|tag| (u64::try_from(tag.id).unwrap_or_default(), tag.into()))
            .collect();

        // Отправляем синхронизацию (глобальный view для анализаторов)
        if let Err(e) = updater.sync_config(services_data, filters_data).await {
            error!("Failed to send config sync: {}", e);
        }

        // Re-send snapshots to agents whose acknowledged revision lags the
        // current control-plane revision (bounded: one attempt per tick).
        control.reconcile_stale_agents(&agent_service).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_service() -> WireService {
        WireService {
            name: "team-site".into(),
            port: 8080,
            chunked_transfer_encoding: true,
            decrypt_tls: false,
            inflate_websockets: true,
            merge_adjacent_packets: false,
            urldecode: true,
            http_parse: true,
            id: 7,
            protocol: WireProtocol::Tcp as i32,
            is_global: true,
        }
    }

    #[test]
    fn envelope_carries_version_and_roundtrips() {
        let envelope = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(wire::settings_envelope::Update::ServiceAdd(WireServiceOp {
                id: 7,
                service: Some(sample_service()),
            })),
        };

        let bytes = envelope.encode_to_vec();
        let decoded = SettingsEnvelope::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded.version, SETTINGS_WIRE_VERSION);
        match decoded.update {
            Some(wire::settings_envelope::Update::ServiceAdd(op)) => {
                assert_eq!(op.id, 7);
                let service = op.service.unwrap();
                assert_eq!(service.port, 8080);
                assert_eq!(service.id, 7);
                assert_eq!(service.protocol, WireProtocol::Tcp as i32);
                assert!(service.is_global);
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn agent_snapshot_roundtrips_with_revision_and_target() {
        let snapshot = WireAgentConfigSnapshot {
            revision: 42,
            agent_id: "host-10.0.0.5".into(),
            network: Some(wire::WireAgentNetworkSettings {
                interface: "eth1".into(),
                machine_ip: "10.0.0.5".into(),
            }),
            services: vec![sample_service()],
            filters: vec![],
            blocks: vec![],
        };
        let envelope = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(wire::settings_envelope::Update::AgentSnapshot(snapshot)),
        };

        let decoded = SettingsEnvelope::decode(envelope.encode_to_vec().as_slice()).unwrap();
        match decoded.update {
            Some(wire::settings_envelope::Update::AgentSnapshot(snap)) => {
                assert_eq!(snap.revision, 42);
                assert_eq!(snap.agent_id, "host-10.0.0.5");
                let network = snap.network.unwrap();
                assert_eq!(network.interface, "eth1");
                assert_eq!(network.machine_ip, "10.0.0.5");
                assert_eq!(snap.services.len(), 1);
                assert!(snap.filters.is_empty());
                assert!(snap.blocks.is_empty());
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    /// The unified filter model must survive the wire roundtrip: matcher
    /// kind, action, targets, threshold, expiry and revision all decode
    /// back exactly.
    #[test]
    fn unified_filter_roundtrips_through_the_wire() {
        let tag = crate::api::tag::tag_dto::TagName {
            id: 12,
            name: "bruteforce".into(),
            color: 120,
            template_type: "ip_cidr".into(),
            template: String::new(),
            direction: "in".into(),
            special_type: None,
            action: "block".into(),
            enabled: true,
            ip_network: Some("10.6.0.0/16".into()),
            ip_endpoint: Some("src".into()),
            expression_source: None,
            service_targets_global: false,
            agent_targets_global: false,
            service_ids: vec![3, 9],
            agent_ids: vec!["agent-a".into(), "agent-b".into()],
            threshold_count: Some(4),
            threshold_window_secs: Some(10),
            threshold_group_by: Some("remote_ip".into()),
            threshold_cooldown_secs: Some(30),
            threshold_block_secs: Some(120),
            expires_at: Some(chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()),
            revision: 77,
        };
        let wire_filter: WireFilter = tag.into();

        let op = WireFilterOp {
            id: 12,
            filter: Some(wire_filter),
        };
        let envelope = SettingsEnvelope {
            version: SETTINGS_WIRE_VERSION,
            update: Some(wire::settings_envelope::Update::FilterAdd(op)),
        };
        let decoded = SettingsEnvelope::decode(envelope.encode_to_vec().as_slice()).unwrap();
        match decoded.update {
            Some(wire::settings_envelope::Update::FilterAdd(op)) => {
                assert_eq!(op.id, 12);
                let f = op.filter.unwrap();
                assert_eq!(f.filter_type, "ip_cidr");
                assert_eq!(f.action, "block");
                assert!(f.enabled);
                assert_eq!(f.ip_network, "10.6.0.0/16");
                assert_eq!(f.ip_endpoint, wire::WireIpEndpoint::Src as i32);
                assert!(!f.services_all);
                assert_eq!(f.service_ids, vec![3, 9]);
                assert!(!f.agents_all);
                assert_eq!(
                    f.agent_ids,
                    vec!["agent-a".to_string(), "agent-b".to_string()]
                );
                assert_eq!(f.expires_at_ms, 1_700_000_000_000);
                assert_eq!(f.revision, 77);
                let threshold = f.threshold.unwrap();
                assert_eq!(threshold.count, 4);
                assert_eq!(threshold.window_seconds, 10);
                assert_eq!(threshold.block_seconds, 120);
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }

    #[test]
    fn tag_direction_maps_onto_wire_enum() {
        let tag = |direction: &str| crate::api::tag::tag_dto::TagName {
            id: 1,
            name: "flag".into(),
            color: 0,
            template_type: "substr".into(),
            template: "flag{{".into(),
            direction: direction.into(),
            special_type: None,
            action: "tag".into(),
            enabled: true,
            ip_network: None,
            ip_endpoint: None,
            expression_source: None,
            service_targets_global: true,
            agent_targets_global: true,
            service_ids: vec![],
            agent_ids: vec![],
            threshold_count: None,
            threshold_window_secs: None,
            threshold_group_by: None,
            threshold_cooldown_secs: None,
            threshold_block_secs: None,
            expires_at: None,
            revision: 0,
        };

        assert_eq!(
            WireFilter::from(tag("in")).direction,
            WireFilterDirection::Inbound as i32
        );
        assert_eq!(
            WireFilter::from(tag("OUT")).direction,
            WireFilterDirection::Outbound as i32
        );
        assert_eq!(
            WireFilter::from(tag("garbage")).direction,
            WireFilterDirection::Both as i32
        );
    }

    #[test]
    fn udp_service_stays_udp_on_the_wire() {
        let service = crate::api::service::service_dto::Service {
            id: 9,
            name: "dns".into(),
            port: 53,
            chunked_transfer_encoding: false,
            urldecode: false,
            merge_adjacent_packets: false,
            inflate_websockets: false,
            decrypt_tls: false,
            http_parse: false,
            http2_parse: false,
            alert_flag_leak: false,
            protocol: "UDP".into(),
            is_global: false,
            agent_ids: vec![],
        };

        let wire_service: WireService = service.into();
        assert_eq!(wire_service.protocol, WireProtocol::Udp as i32);
        assert!(!wire_service.is_global);
    }
}
