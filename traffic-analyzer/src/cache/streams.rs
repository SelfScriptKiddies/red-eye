use lru::LruCache;
use std::num::NonZeroUsize;

pub struct StreamsCache {
    cache: LruCache<String, usize>,
}

impl StreamsCache {
    pub fn new(capacity: usize) -> Self {
        StreamsCache {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    fn format_key(first_ip: &str, first_port: u16, second_ip: &str, second_port: u16) -> String {
        format!("{}:{}-{}:{}", first_ip, first_port, second_ip, second_port)
    }

    pub fn get(&mut self, first_ip: &str, first_port: u16, second_ip: &str, second_port: u16) -> Option<usize> {
        match self.cache.get(
            &Self::format_key(first_ip, first_port, second_ip, second_port)
        ) {
            Some(value) => Some(*value),
            None => None,
        }
    }

    pub fn insert(&mut self, first_ip: &str, first_port: u16, second_ip: &str, second_port: u16, stream_id: usize) {
        let key = format!("{}:{}-{}:{}", first_ip, first_port, second_ip, second_port);
        self.cache.put(key, stream_id);
    }
}
    