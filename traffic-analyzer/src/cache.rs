use crate::configuration::updater::dto;
use crate::proto_schema::ProtoSchema;
use dashmap::DashMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
pub mod services;
pub mod streams;
pub use services::ServicesCache;
pub use streams::StreamsCache;

/// Per-agent machine IP the packet consumer resolves direction with.
/// Wildcard values (0.0.0.0 / ::) are stored as-is; the matcher treats
/// them as protocol+port-only matching.
#[derive(Debug, Clone)]
pub struct AgentRegistryEntry {
    pub machine_ip: IpAddr,
    pub revision: u64,
}

pub struct Cache {
    pub services: Arc<RwLock<HashMap<usize, dto::Service>>>,
    pub streams: Arc<StreamsCache>,
    pub stream_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    pub proto_schemas: Arc<RwLock<HashMap<usize, ProtoSchema>>>,
    /// agent_id -> effective machine IP (warm from agent_config, updated
    /// by AgentConfigSnapshot updates).
    pub agent_registry: Arc<RwLock<HashMap<String, AgentRegistryEntry>>>,
    /// TCP lifecycle: tuple keys where a plain SYN opened a connection that
    /// has not carried data yet — a repeated plain SYN is a retransmission
    /// and must reuse the stream, not fork a new one. Cleared by the first
    /// payload-bearing packet on the tuple or by FIN/RST.
    syn_pending: Arc<DashMap<String, ()>>,
    /// TCP lifecycle: stream ids seen closing (FIN/RST) or retired by a new
    /// SYN on the same tuple, waiting for the batch flusher to run the
    /// deferred `UPDATE stream SET active = false`.
    closed_streams: Arc<DashMap<usize, ()>>,
}

impl Cache {
    pub fn new(stream_ttl_secs: u64) -> Self {
        Cache {
            services: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(StreamsCache::new(stream_ttl_secs)),
            stream_locks: Arc::new(DashMap::new()),
            proto_schemas: Arc::new(RwLock::new(HashMap::new())),
            agent_registry: Arc::new(RwLock::new(HashMap::new())),
            syn_pending: Arc::new(DashMap::new()),
            closed_streams: Arc::new(DashMap::new()),
        }
    }

    /// Record an agent's machine IP. Stale snapshots (older revision for
    /// the same agent) are ignored; equal revisions re-apply harmlessly.
    pub fn set_agent_network(&self, agent_id: &str, machine_ip: IpAddr, revision: u64) {
        let mut registry = self.agent_registry.write().unwrap();
        let entry = registry
            .entry(agent_id.to_string())
            .or_insert(AgentRegistryEntry {
                machine_ip,
                revision: 0,
            });
        if revision >= entry.revision {
            entry.machine_ip = machine_ip;
            entry.revision = revision;
        }
    }

    pub fn agent_machine_ip(&self, agent_id: &str) -> Option<IpAddr> {
        self.agent_registry
            .read()
            .unwrap()
            .get(agent_id)
            .map(|e| e.machine_ip)
    }

    pub fn agent_registry_len(&self) -> usize {
        self.agent_registry.read().unwrap().len()
    }

    pub fn mark_syn_pending(&self, key: &str) {
        self.syn_pending.insert(key.to_string(), ());
    }

    pub fn clear_syn_pending(&self, key: &str) {
        self.syn_pending.remove(key);
    }

    pub fn syn_pending(&self, key: &str) -> bool {
        self.syn_pending.contains_key(key)
    }

    /// Queue a stream id for the deferred active=false flush. Idempotent;
    /// drained by the batch flusher (re-queued there on UPDATE failure so
    /// the next batch retries).
    pub fn mark_stream_closed(&self, stream_id: usize) {
        self.closed_streams.insert(stream_id, ());
    }

    /// Take all queued stream ids for the close flush (removed optimistically;
    /// a failed UPDATE re-queues them via `requeue_closed_streams`, and a
    /// concurrent FIN for an id already being flushed only re-marks the same
    /// idempotent active=false).
    pub fn drain_closed_streams(&self) -> Vec<usize> {
        let ids: Vec<usize> = self.closed_streams.iter().map(|e| *e.key()).collect();
        for id in &ids {
            self.closed_streams.remove(id);
        }
        ids
    }

    /// Re-queue ids whose close UPDATE failed (retry on next batch).
    pub fn requeue_closed_streams(&self, ids: &[usize]) {
        for id in ids {
            self.closed_streams.insert(*id, ());
        }
    }

    pub fn cleanup_unused_locks(&self) {
        let keys_to_remove: Vec<String> = self
            .stream_locks
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
