use std::sync::atomic::AtomicU64;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Aggregate ingestion drop counters. Never unbounded labels: the counts
/// are global aggregates; details live in rate-limited log lines only.
pub static UNKNOWN_SERVICE_DROPS: AtomicU64 = AtomicU64::new(0);
pub static UNKNOWN_AGENT_DROPS: AtomicU64 = AtomicU64::new(0);
pub static AMBIGUOUS_DIRECTION_DROPS: AtomicU64 = AtomicU64::new(0);
/// Starred segment rotations at the byte limit (+ failed rotations).
pub static FAVORITE_ROTATIONS: AtomicU64 = AtomicU64::new(0);
pub static FAVORITE_ROTATION_FAILURES: AtomicU64 = AtomicU64::new(0);

/// User-Agent aggregate counters (see user_agent.rs). Aggregates are
/// degradable: every skipped update is counted, none may stall ingestion.
pub static UA_MESSAGES_AGGREGATED: AtomicU64 = AtomicU64::new(0);
pub static UA_AGGREGATE_ROWS: AtomicU64 = AtomicU64::new(0);
pub static UA_AGGREGATE_LOSS: AtomicU64 = AtomicU64::new(0);
pub static UA_OVERSIZED_VALUES: AtomicU64 = AtomicU64::new(0);
pub static UA_CARDINALITY_OVERFLOW: AtomicU64 = AtomicU64::new(0);

/// Minimum spacing between rate-limited log lines (shared guard so a
/// burst of drops cannot flood the log under game load).
const LOG_MIN_INTERVAL: Duration = Duration::from_secs(5);
static LAST_LOG: Mutex<Option<Instant>> = Mutex::new(None);

/// Log only when the last line with this tag is older than the rate
/// window. `count` is the counter value to include (current total).
pub fn rate_limited_log(tag: &str, count: u64, message: impl FnOnce() -> String) {
    let mut last = LAST_LOG.lock().unwrap();
    let now = Instant::now();
    let allowed = match *last {
        Some(t) => now.duration_since(t) >= LOG_MIN_INTERVAL,
        None => true,
    };
    if allowed {
        *last = Some(now);
        drop(last);
        log::warn!("[{}] {} (total: {})", tag, message(), count);
    }
}
