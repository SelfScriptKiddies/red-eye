use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use dashmap::DashMap;
use crate::configuration::updater::dto;
pub mod services;
pub mod streams;
pub use services::ServicesCache as ServicesCache;
pub use streams::StreamsCache as StreamsCache;

pub struct Cache {
    pub services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    pub streams: Arc<Mutex<StreamsCache>>,
    // Per-key locks to avoid global bottleneck while preventing race conditions
    // Using Arc<Mutex<()>> allows cloning the Arc to hold the lock
    pub stream_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl Cache {
    pub fn new(capacity: usize) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(Mutex::new(StreamsCache::new(capacity))),
            stream_locks: Arc::new(DashMap::new()),
        }
    }
}

