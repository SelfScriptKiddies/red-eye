use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::hash::Hash;
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use crate::configuration::updater::dto;
pub mod services;
pub mod streams;
pub use services::ServicesCache as ServicesCache;
pub use streams::LRUCache as LRUCache;

// Кеш для сервисов
pub static SERVICES_CACHE: Lazy<ServicesCache> = Lazy::new(|| ServicesCache::new());

pub struct Cache {
    services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    streams: Arc<RwLock<LRUCache<String, Vec<u8>>>>
}

impl Cache {
    pub fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(LRUCache::new(capacity, ttl)))
        }
    }
}

