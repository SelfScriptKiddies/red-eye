use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct StreamsCache {
    entries: Arc<DashMap<String, usize>>,
    last_seen: Arc<DashMap<String, Instant>>,
    ttl_secs: u64,
}

impl StreamsCache {
    pub fn new(ttl_secs: u64) -> Self {
        StreamsCache {
            entries: Arc::new(DashMap::new()),
            last_seen: Arc::new(DashMap::new()),
            ttl_secs,
        }
    }

    pub fn format_key(src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16) -> String {
        format!("{}:{}-{}:{}", src_ip, src_port, dst_ip, dst_port)
    }

    pub fn get_by_key(&self, key: &str) -> Option<usize> {
        if let Some(entry) = self.entries.get(key) {
            self.last_seen.insert(key.to_string(), Instant::now());
            Some(*entry.value())
        } else {
            None
        }
    }

    pub fn insert(&self, src_ip: &str, src_port: u16, dst_ip: &str, dst_port: u16, stream_id: usize) {
        let key = Self::format_key(src_ip, src_port, dst_ip, dst_port);
        self.entries.insert(key.clone(), stream_id);
        self.last_seen.insert(key, Instant::now());
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn evict_expired(&self) -> usize {
        let cutoff = Instant::now() - std::time::Duration::from_secs(self.ttl_secs);
        let mut removed = 0;
        let expired_keys: Vec<String> = self.last_seen
            .iter()
            .filter(|entry| *entry.value() < cutoff)
            .map(|entry| entry.key().clone())
            .collect();
        for key in expired_keys {
            self.entries.remove(&key);
            self.last_seen.remove(&key);
            removed += 1;
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
