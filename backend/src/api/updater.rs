use crate::configuration::ApplicationConfig;
use crate::wire::{
    self, SettingsEnvelope, WireConfigSync, WireFilter, WireFilterDirection, WireFilterOp,
    WireService, WireServiceOp,
};
use async_nats::{ConnectOptions, jetstream};
use log::{debug, error, info};
use prost::Message as _;

// Switched to protobuf in v3
pub const SETTINGS_WIRE_VERSION: u32 = 3;

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
        }
    }
}

impl From<crate::api::tag::tag_dto::TagName> for WireFilter {
    fn from(tag_name: crate::api::tag::tag_dto::TagName) -> Self {
        WireFilter {
            pattern_name: tag_name.name,
            filter_type: tag_name.template_type,
            filter_payload: tag_name.template.as_bytes().to_vec(),
            direction: match tag_name.direction.to_lowercase().as_str() {
                "in" => WireFilterDirection::Inbound as i32,
                "out" => WireFilterDirection::Outbound as i32,
                _ => WireFilterDirection::Both as i32,
            },
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
    pub async fn add_service(&self, service: WireService, index: u32) -> Result<(), String> {
        debug!("Sending add service: {:?}", service);
        self.publish(wire::settings_envelope::Update::ServiceAdd(WireServiceOp {
            id: index,
            service: Some(service),
        }))
        .await
    }

    pub async fn update_service(&self, index: u32, service: WireService) -> Result<(), String> {
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

    pub async fn remove_service(&self, index: u32) -> Result<(), String> {
        debug!("Sending remove service: (id={})", index);
        self.publish(wire::settings_envelope::Update::ServiceRemove(index))
            .await
    }

    // Filters
    pub async fn add_filter(&self, filter: WireFilter, index: u32) -> Result<(), String> {
        debug!("Sending add filter: {:?}", filter);
        self.publish(wire::settings_envelope::Update::FilterAdd(WireFilterOp {
            id: index,
            filter: Some(filter),
        }))
        .await
    }

    pub async fn update_filter(&self, index: u32, filter: WireFilter) -> Result<(), String> {
        debug!("Sending update filter: (id={}, filter={:?})", index, filter);
        self.publish(wire::settings_envelope::Update::FilterUpdate(
            WireFilterOp {
                id: index,
                filter: Some(filter),
            },
        ))
        .await
    }

    pub async fn remove_filter(&self, index: u32) -> Result<(), String> {
        debug!("Sending remove filter: (id={})", index);
        self.publish(wire::settings_envelope::Update::FilterRemove(index))
            .await
    }

    pub async fn sync_config(
        &self,
        services: Vec<(u32, WireService)>,
        filters: Vec<(u32, WireFilter)>,
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
}

/// Запускает периодическую синхронизацию конфигурации каждые 5 минут
pub async fn start_config_sync_scheduler(
    updater: std::sync::Arc<UpdateAgent>,
    service_service: std::sync::Arc<crate::api::service::service_service::ServiceService>,
    tag_service: std::sync::Arc<crate::api::tag::tag_service::TagService>,
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
        let services_data: Vec<(u32, WireService)> = services
            .into_iter()
            .map(|s| (s.id as u32, s.into()))
            .collect();

        // Конвертируем теги в фильтры
        let filters_data: Vec<(u32, WireFilter)> = tag_names
            .into_iter()
            .map(|tag| (tag.id as u32, tag.into()))
            .collect();

        // Отправляем синхронизацию
        if let Err(e) = updater.sync_config(services_data, filters_data).await {
            error!("Failed to send config sync: {}", e);
        }
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
                assert_eq!(op.service.unwrap().port, 8080);
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
}
