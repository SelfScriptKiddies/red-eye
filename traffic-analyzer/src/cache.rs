use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::hash::Hash;
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use crate::configuration::updater::dto;
pub mod services;
pub mod streams;
pub use services::ServicesCache as ServicesCache;
pub use streams::LRUCache as LRUCache;

pub struct Cache {
    pub services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    pub streams: Arc<Mutex<LRUCache<String, Vec<u8>>>>
}

impl Cache {
    pub fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(Mutex::new(LRUCache::new(capacity, ttl)))
        }
    }
}

