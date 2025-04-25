use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::RwLock;

// LRU кеш для последних потоков
pub struct LRUCache<K, V> 
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    capacity: usize,
    cache: HashMap<K, (V, Instant)>,
    keys: VecDeque<K>,
    ttl: Option<Duration>,
}

impl<K, V> LRUCache<K, V> 
where 
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        LRUCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            keys: VecDeque::with_capacity(capacity),
            ttl: ttl,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        self.remove_expired();

        if let Some((value, timestamp)) = self.cache.get(key) {
            // Обновляем позицию ключа в очереди
            if let Some(pos) = self.keys.iter().position(|k| k == key) {
                self.keys.remove(pos);
                self.keys.push_back(key.clone());
            }
            return Some(value.clone());
        }
        None
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.remove_expired();

        // Если ключ уже существует, обновляем значение и перемещаем ключ в конец
        if self.cache.contains_key(&key) {
            self.cache.insert(key.clone(), (value, Instant::now()));
            if let Some(pos) = self.keys.iter().position(|k| k == &key) {
                self.keys.remove(pos);
                self.keys.push_back(key);
            }
            return;
        }

        // Если кеш полон, удаляем старейший элемент
        if self.cache.len() >= self.capacity {
            if let Some(oldest) = self.keys.pop_front() {
                self.cache.remove(&oldest);
            }
        }

        // Добавляем новый элемент
        self.cache.insert(key.clone(), (value, Instant::now()));
        self.keys.push_back(key);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some((value, _)) = self.cache.remove(key) {
            if let Some(pos) = self.keys.iter().position(|k| k == key) {
                self.keys.remove(pos);
            }
            return Some(value);
        }
        None
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.keys.clear();
    }

    fn remove_expired(&mut self) {
        if let Some(ttl) = self.ttl {
            let now = Instant::now();
            let expired_keys: Vec<K> = self.cache.iter()
                .filter(|(_, (_, timestamp))| now.duration_since(*timestamp) > ttl)
                .map(|(k, _)| k.clone())
                .collect();

            for key in expired_keys {
                self.remove(&key);
            }
        }
    }
}