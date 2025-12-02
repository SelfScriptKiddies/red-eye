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

    /// Clean up unused stream locks - removes locks for keys that are not in the LRU cache
    /// This prevents memory accumulation from old stream locks
    pub async fn cleanup_unused_locks(&self) {
        let mut stream_cache = self.streams.lock().await;
        
        // Collect keys to remove (locks for streams not in cache)
        let keys_to_remove: Vec<String> = self.stream_locks
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if !stream_cache.contains_key(key) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove unused locks
        for key in keys_to_remove {
            self.stream_locks.remove(&key);
        }
    }
}

