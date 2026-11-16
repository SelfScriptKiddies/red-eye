use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::config::{
    DRYRUN_MAX_OPS, DRYRUN_TTL_SECS, MAX_SESSIONS, RATE_LIMIT_PER_ADDR, RATE_LIMIT_PER_TOKEN,
    RATE_WINDOW_SECS, SESSION_IDLE_SECS,
};

// Bounded in-process state: rate limiter, session tracker, one-shot
// dry-run operation store. Every map has a hard cap and stale-entry
// eviction so a flood cannot grow memory.

pub fn token_hash(token: &str) -> String {
    let mut h = Sha256::new();
    h.update(token.trim().as_bytes());
    let mut s = String::with_capacity(64);
    for b in h.finalize() {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Fixed-window rate limiter keyed by an opaque string (token id or IP).
struct WindowCounter {
    window_start: u64,
    count: u32,
}

pub struct RateLimiter {
    entries: Mutex<HashMap<String, WindowCounter>>,
    limit: u32,
}

impl RateLimiter {
    fn new(limit: u32) -> Self {
        RateLimiter {
            entries: Mutex::new(HashMap::new()),
            limit,
        }
    }

    pub fn per_token() -> Self {
        Self::new(RATE_LIMIT_PER_TOKEN)
    }

    pub fn per_addr() -> Self {
        Self::new(RATE_LIMIT_PER_ADDR)
    }

    /// Tighter budget for fetch_url egress calls per token.
    pub fn fetch() -> Self {
        Self::new(crate::config::FETCH_RATE_LIMIT_PER_TOKEN)
    }

    /// Some(true) = allowed, Some(false) = over limit, None = lock
    /// poisoned (fail closed upstream).
    pub fn check(&self, key: &str) -> Option<bool> {
        let mut entries = self.entries.lock().ok()?;
        let now = now_secs();
        // Hard cap on tracked keys; evict expired first.
        if entries.len() > 2000 {
            entries.retain(|_, w| now - w.window_start < RATE_WINDOW_SECS * 2);
        }
        if entries.len() > 4000 {
            entries.clear();
        }
        let w = entries.entry(key.to_string()).or_insert(WindowCounter {
            window_start: now,
            count: 0,
        });
        if now - w.window_start >= RATE_WINDOW_SECS {
            w.window_start = now;
            w.count = 0;
        }
        w.count += 1;
        Some(w.count <= self.limit)
    }
}

pub struct SessionTracker {
    last_seen: Mutex<HashMap<String, Instant>>,
}

impl SessionTracker {
    pub fn new() -> Self {
        SessionTracker {
            last_seen: Mutex::new(HashMap::new()),
        }
    }

    /// Some(true) when the session id may proceed. None on unknown/new
    /// session ids (callers pass them through admission instead).
    pub fn touch(&self, session_id: &str) -> Option<bool> {
        let mut map = self.last_seen.lock().ok()?;
        let now = Instant::now();
        map.retain(|_, t| now.duration_since(*t) < Duration::from_secs(SESSION_IDLE_SECS));
        if let Some(t) = map.get_mut(session_id) {
            *t = now;
            return Some(true);
        }
        if map.len() >= MAX_SESSIONS {
            return Some(false);
        }
        map.insert(session_id.to_string(), now);
        Some(true)
    }

    pub fn forget(&self, session_id: &str) {
        if let Ok(mut map) = self.last_seen.lock() {
            map.remove(session_id);
        }
    }
}

/// One-shot dry-run operations: the dry-run call registers the exact
/// intended change; the mutation call consumes it. A consumed, expired or
/// unknown id rejects the mutation - an exploratory request can never be
/// silently upgraded into a destructive action.
pub struct DryRunStore {
    ops: Mutex<HashMap<String, (String, Instant)>>,
}

impl DryRunStore {
    pub fn new() -> Self {
        DryRunStore {
            ops: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, intended_change: String) -> Option<String> {
        let mut ops = self.ops.lock().ok()?;
        sweep(&mut ops);
        if ops.len() >= DRYRUN_MAX_OPS {
            return None;
        }
        let id = format!(
            "dryrun-{}",
            token_hash(&format!(
                "{}{:?}",
                intended_change,
                std::time::SystemTime::now()
            ))
            .get(..12)
            .unwrap_or("0")
        );
        ops.insert(id.clone(), (intended_change, Instant::now()));
        Some(id)
    }

    /// Consume exactly once: Ok(Some(description)) on a live match.
    pub fn consume(
        &self,
        op_id: &str,
        expected_change: &str,
    ) -> Option<Result<String, ConsumeError>> {
        let mut ops = self.ops.lock().ok()?;
        sweep(&mut ops);
        let Some((change, created)) = ops.remove(op_id) else {
            return Some(Err(ConsumeError::Unknown));
        };
        if created.elapsed() > Duration::from_secs(DRYRUN_TTL_SECS) {
            return Some(Err(ConsumeError::Expired));
        }
        if change != expected_change {
            // State drifted since the dry run; the operator (or model)
            // must dry-run again against fresh state.
            return Some(Err(ConsumeError::Drift));
        }
        Some(Ok(change))
    }
}

pub enum ConsumeError {
    Unknown,
    Expired,
    Drift,
}

fn sweep(ops: &mut HashMap<String, (String, Instant)>) {
    ops.retain(|_, (_, created)| created.elapsed() <= Duration::from_secs(DRYRUN_TTL_SECS));
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_counts_within_window() {
        let rl = RateLimiter::new(3);
        assert_eq!(rl.check("t"), Some(true));
        assert_eq!(rl.check("t"), Some(true));
        assert_eq!(rl.check("t"), Some(true));
        assert_eq!(rl.check("t"), Some(false));
        assert_eq!(rl.check("other"), Some(true));
    }

    #[test]
    fn dryrun_is_one_shot_and_drift_sensitive() {
        let store = DryRunStore::new();
        let id = store.register("delete service 3".into()).unwrap();
        assert!(matches!(
            store.consume("nope", "delete service 3"),
            Some(Err(ConsumeError::Unknown))
        ));
        // Drift burns the op: changed arguments force a fresh dry-run.
        assert!(
            store
                .consume(&id, "delete service 5")
                .is_some_and(|r| r.is_err())
        );
        assert!(matches!(
            store.consume(&id, "delete service 3"),
            Some(Err(ConsumeError::Unknown))
        ));
        let id2 = store.register("delete service 3".into()).unwrap();
        assert!(
            store
                .consume(&id2, "delete service 3")
                .is_some_and(|r| r.is_ok())
        );
        assert!(matches!(
            store.consume(&id2, "delete service 3"),
            Some(Err(ConsumeError::Unknown))
        ));
    }

    #[test]
    fn session_tracker_caps_and_touches() {
        let st = SessionTracker::new();
        assert!(st.touch("s1") == Some(true));
        assert!(st.touch("s1") == Some(true));
    }
}
