use async_nats::{
    ConnectOptions, 
    Client, 
    jetstream
};
use log::{error, info, debug};
use bincode::{config, Decode, Encode};
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

    // Full config sync
    ConfigSync {
        services: Vec<(usize, dto::Service)>,
        filters: Vec<(usize, dto::Filter)>,
    }
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
        pub http_parse: bool,
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
                http_parse: service.http_parse,
            }
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
            config.nats_password.clone())
            .connect(config.nats_broker.clone())
            .await
            .expect("Error connecting to NATS");
        
        let js = jetstream::new(client);
        let topic = config.nats_settings_topic.clone();
        
        // Stream will be created by consumers, we just publish to the subject
        UpdateAgent { 
            js,
            topic,
        }
    }
    
    async fn send_update(&self, update: SettingsUpdate, index: usize) -> Result<(), String> {
        let payload = match bincode::encode_to_vec((update, index), config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Error serializing: {}", e)),
        };
        
        match self.js.publish(self.topic.clone(), payload.into()).await {
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

    pub async fn sync_config(
        &self,
        services: Vec<(usize, dto::Service)>,
        filters: Vec<(usize, dto::Filter)>,
    ) -> Result<(), String> {
        info!("Sending config sync: {} services, {} filters", services.len(), filters.len());
        
        let update = SettingsUpdate::ConfigSync { services, filters };
        let payload = match bincode::encode_to_vec((update, 0usize), config::standard()) {
            Ok(bytes) => bytes,
            Err(e) => return Err(format!("Error serializing config sync: {}", e)),
        };
        
        match self.js.publish(self.topic.clone(), payload.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error sending config sync: {}", e)),
        }
    }
}

pub use dto::{Service as DtoService, Filter as DtoFilter};

/// Запускает периодическую синхронизацию конфигурации каждые 5 минут
pub async fn start_config_sync_scheduler(
    updater: std::sync::Arc<UpdateAgent>,
    service_service: std::sync::Arc<crate::api::service::service_service::ServiceService>,
    tag_service: std::sync::Arc<crate::api::tag::tag_service::TagService>,
) {
    use tokio::time::{interval, Duration};
    
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
        let services_data: Vec<(usize, dto::Service)> = services
            .into_iter()
            .map(|s| (s.id as usize, s.into()))
            .collect();
        
        // Конвертируем теги в фильтры
        let filters_data: Vec<(usize, dto::Filter)> = tag_names
            .into_iter()
            .map(|tag| (tag.id as usize, tag.into()))
            .collect();
        
        // Отправляем синхронизацию
        if let Err(e) = updater.sync_config(services_data, filters_data).await {
            error!("Failed to send config sync: {}", e);
        }
    }
}
