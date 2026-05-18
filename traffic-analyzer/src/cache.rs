use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use dashmap::DashMap;
use crate::configuration::updater::dto;
use crate::proto_schema::ProtoSchema;
pub mod services;
pub mod streams;
pub use services::ServicesCache as ServicesCache;
pub use streams::StreamsCache as StreamsCache;

pub struct Cache {
    pub services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    pub streams: Arc<StreamsCache>,
    pub stream_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    pub proto_schemas: Arc<RwLock<HashMap<usize, ProtoSchema>>>,
    pub header_sets: Arc<DashMap<i64, i32>>,
}

impl Cache {
    pub fn new(stream_ttl_secs: u64) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(StreamsCache::new(stream_ttl_secs)),
            stream_locks: Arc::new(DashMap::new()),
            proto_schemas: Arc::new(RwLock::new(HashMap::new())),
            header_sets: Arc::new(DashMap::new()),
        }
    }

    pub fn cleanup_unused_locks(&self) {
        let keys_to_remove: Vec<String> = self.stream_locks
            .iter()
            .filter_map(|entry| {
                if !self.streams.contains_key(entry.key()) {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();
        for key in keys_to_remove {
            self.stream_locks.remove(&key);
        }
    }
}
