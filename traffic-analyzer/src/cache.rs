use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::hash::Hash;
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use crate::configuration::updater::dto;
pub mod services;
pub mod streams;
pub use services::ServicesCache as ServicesCache;
pub use streams::StreamsCache as StreamsCache;

pub struct Cache {
    pub services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    pub streams: Arc<Mutex<StreamsCache>>
}

impl Cache {
    pub fn new(capacity: usize) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(Mutex::new(StreamsCache::new(capacity)))
        }
    }
}

