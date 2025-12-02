use lru::LruCache;
use std::num::NonZeroUsize;
use log::trace;
pub struct StreamsCache {
    cache: LruCache<String, usize>,
}

impl StreamsCache {
    pub fn new(capacity: usize) -> Self {
        StreamsCache {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    pub fn format_key(first_ip: &str, first_port: u16, second_ip: &str, second_port: u16) -> String {
        format!("{}:{}-{}:{}", first_ip, first_port, second_ip, second_port)
    }

    pub fn get(&mut self, first_ip: &str, first_port: u16, second_ip: &str, second_port: u16) -> Option<usize> {
        let key = Self::format_key(first_ip, first_port, second_ip, second_port);
        self.get_by_key(&key)
    }

    pub fn get_by_key(&mut self, key: &str) -> Option<usize> {
        match self.cache.get(key) {
            Some(value) => {
                trace!("Found stream id for key: {}", key);
                Some(*value)
            },
            None => {
                trace!("No stream id found for key: {}", key);
                None
            }
        }
    }

    pub fn insert(&mut self, first_ip: &str, first_port: u16, second_ip: &str, second_port: u16, stream_id: usize) {
        let key = Self::format_key(first_ip, first_port, second_ip, second_port);
        trace!("Inserting stream id for key: {}", key);
        self.cache.put(key, stream_id);
    }

    // Check if key exists in cache (for cleanup purposes)
    // Note: This uses get() which updates LRU order, but that's acceptable for periodic cleanup
    pub fn contains_key(&mut self, key: &str) -> bool {
        self.cache.get(key).is_some()
    }
}
    

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut cache = StreamsCache::new(10);
        cache.insert("192.168.1.1", 8080, "192.168.1.2", 8080, 1);
        assert_eq!(cache.get("192.168.1.1", 8080, "192.168.1.2", 8080), Some(1));
    }
}