use crate::configuration::updater::dto;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

// Структура кеша сервисов
pub struct ServicesCache {
    services: Arc<RwLock<HashMap<usize, dto::Service>>>,
}

impl ServicesCache {
    pub fn new() -> Self {
        ServicesCache {
            services: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_service(&self, id: usize, service: dto::Service) {
        let mut services = self.services.write().unwrap();
        services.insert(id, service);
    }

    pub fn update_service(&self, id: usize, service: dto::Service) {
        let mut services = self.services.write().unwrap();
        services.insert(id, service);
    }

    pub fn remove_service(&self, id: usize) {
        let mut services = self.services.write().unwrap();
        services.remove(&id);
    }

    pub fn get_service(&self, id: usize) -> Option<dto::Service> {
        let services = self.services.read().unwrap();
        services.get(&id).cloned()
    }

    pub fn get_services(&self) -> Vec<(usize, dto::Service)> {
        let services = self.services.read().unwrap();
        services
            .iter()
            .map(|(id, service)| (*id, service.clone()))
            .collect()
    }
}
