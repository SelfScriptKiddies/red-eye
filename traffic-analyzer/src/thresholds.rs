//! Bounded sliding-window threshold counters.
//!
//! Counters are partitioned by (filter_id, agent_id, group value). Each
//! persisted packet match counts ONCE per filter per packet (callers
//! dedupe offsets). The first hit that reaches N within the sliding
//! window T fires the action; further actions for that group are
//! suppressed until the cooldown expires.
//!
//! Memory bound: the counter map never grows beyond MAX_COUNTERS entries
//! (new keys are rejected and counted at capacity; existing counters keep
//! working), and idle counters expire via a periodic sweep. Capacity never
//! blocks packet ingestion: record_hit is a pure in-memory call.

use once_cell::sync::Lazy;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Maximum concurrent counters. Sized to cover a worst-case day: every
/// remote team IP x every threshold filter x every agent stays far below
/// this; saturation degrades to skipped new counters, never a panic or
/// unbounded growth.
pub const MAX_COUNTERS: usize = 100_000;

/// Counters idle for longer than (window + cooldown + slack) are swept.
const IDLE_SLACK_SECS: u64 = 60;

pub static COUNTER_CAPACITY_REJECTIONS: AtomicU64 = AtomicU64::new(0);
pub static IDLE_COUNTER_EVITIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CounterKey {
    /// Group value is the normalized remote peer address string.
    RemoteIp {
        filter_id: u64,
        agent_id: String,
        ip: String,
    },
    Stream {
        filter_id: u64,
        agent_id: String,
        stream_id: u64,
    },
    Service {
        filter_id: u64,
        agent_id: String,
        service_id: u64,
    },
}

struct Window {
    hits: VecDeque<u64>,
    cooldown_until: u64,
    last_activity: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdOutcome {
    /// Counted; below the fire count.
    Below,
    /// Threshold was reached but the cooldown still suppresses the action.
    Suppressed,
    /// The action fires now.
    Fire,
}

static COUNTERS: Lazy<Mutex<HashMap<CounterKey, Window>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Record one packet match for a counter and decide the action. `now_secs`
/// is the wall clock. One hit per (packet, filter): the caller dedupes.
pub fn record_hit(
    key: CounterKey,
    count: u32,
    window_secs: u32,
    cooldown_secs: u32,
    now_secs: u64,
) -> ThresholdOutcome {
    let mut counters = COUNTERS.lock().unwrap();

    if !counters.contains_key(&key) && counters.len() >= MAX_COUNTERS {
        // Bounded degradation: this group's action is not tracked; the
        // counter map and ingestion continue untouched.
        COUNTER_CAPACITY_REJECTIONS.fetch_add(1, Ordering::Relaxed);
        return ThresholdOutcome::Below;
    }

    let window = counters.entry(key).or_insert_with(|| Window {
        hits: VecDeque::new(),
        cooldown_until: 0,
        last_activity: now_secs,
    });

    window.last_activity = now_secs;
    let window_start = now_secs.saturating_sub(u64::from(window_secs));
    while let Some(&hit) = window.hits.front() {
        if hit < window_start {
            window.hits.pop_front();
        } else {
            break;
        }
    }
    window.hits.push_back(now_secs);

    if (window.hits.len() as u32) < count {
        return ThresholdOutcome::Below;
    }
    if now_secs < window.cooldown_until {
        return ThresholdOutcome::Suppressed;
    }
    window.cooldown_until = now_secs + u64::from(cooldown_secs);
    ThresholdOutcome::Fire
}

/// Remove counters with no activity beyond their window + cooldown. Called
/// periodically; the map lock is held once for the whole sweep.
pub fn sweep_idle(now_secs: u64, window_secs: u32, cooldown_secs: u32) {
    let idle_after = now_secs
        .saturating_sub(u64::from(window_secs))
        .saturating_sub(u64::from(cooldown_secs))
        .saturating_sub(IDLE_SLACK_SECS);
    let mut counters = COUNTERS.lock().unwrap();
    let before = counters.len();
    counters.retain(|_, window| window.last_activity > idle_after);
    let removed = before - counters.len();
    if removed > 0 {
        IDLE_COUNTER_EVITIONS.fetch_add(removed as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// COUNTERS is a process-global: tests mutate it, so they must not run
    /// concurrently and must start from a clean map.
    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear() {
        COUNTERS.lock().unwrap().clear();
    }

    fn key(ip: &str) -> CounterKey {
        CounterKey::RemoteIp {
            filter_id: 1,
            agent_id: "a".into(),
            ip: ip.into(),
        }
    }

    /// Sliding window: hits outside the window no longer count.
    #[test]
    fn sliding_window_counts_only_recent_hits() {
        let _g = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        clear();
        assert_eq!(
            record_hit(key("10.0.0.1"), 3, 10, 0, 100),
            ThresholdOutcome::Below
        );
        assert_eq!(
            record_hit(key("10.0.0.1"), 3, 10, 0, 105),
            ThresholdOutcome::Below
        );
        // Third hit: 2 previous hits are within 10s, so this reaches 3.
        assert_eq!(
            record_hit(key("10.0.0.1"), 3, 10, 0, 107),
            ThresholdOutcome::Fire
        );
        // Hit at 100 falls out of the window by t=150 (105, 107 remain).
        assert_eq!(
            record_hit(key("10.0.0.1"), 3, 10, 0, 150),
            ThresholdOutcome::Below
        );
    }

    /// Cooldown suppresses further fires for the group only.
    #[test]
    fn cooldown_suppresses_until_expiry() {
        let _g = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        clear();
        assert_eq!(
            record_hit(key("10.1.0.1"), 1, 10, 30, 200),
            ThresholdOutcome::Fire
        );
        assert_eq!(
            record_hit(key("10.1.0.1"), 1, 10, 30, 210),
            ThresholdOutcome::Suppressed
        );
        assert_eq!(
            record_hit(key("10.1.0.1"), 1, 10, 30, 229),
            ThresholdOutcome::Suppressed
        );
        assert_eq!(
            record_hit(key("10.1.0.1"), 1, 10, 30, 231),
            ThresholdOutcome::Fire
        );
        // A different group is unaffected by the cooldown above.
        assert_eq!(
            record_hit(key("10.1.0.2"), 1, 10, 30, 210),
            ThresholdOutcome::Fire
        );
    }

    /// Capacity: new counters rejected and counted, existing keep working.
    #[test]
    fn capacity_rejects_new_counters_only() {
        let _g = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        clear();
        // Fill to capacity with unique single-hit counters (count=5 so no
        // fires).
        for i in 0..MAX_COUNTERS {
            let k = CounterKey::Service {
                filter_id: 2,
                agent_id: "cap".into(),
                service_id: i as u64,
            };
            let _ = record_hit(k, 5, 3600, 0, 1000);
        }
        let before = COUNTER_CAPACITY_REJECTIONS.load(Ordering::Relaxed);
        assert_eq!(
            record_hit(key("10.9.9.9"), 5, 3600, 0, 1000),
            ThresholdOutcome::Below
        );
        assert!(COUNTER_CAPACITY_REJECTIONS.load(Ordering::Relaxed) > before);

        // Existing counters still fire at capacity.
        let existing = CounterKey::Service {
            filter_id: 2,
            agent_id: "cap".into(),
            service_id: 0,
        };
        assert_eq!(
            record_hit(existing, 2, 3600, 0, 1001),
            ThresholdOutcome::Fire
        );
    }

    /// Idle sweep removes expired windows.
    #[test]
    fn idle_sweep_removes_stale_counters() {
        let _g = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        clear();
        let k = key("10.8.8.8");
        let _ = record_hit(k.clone(), 5, 60, 30, 3000);
        // Nothing to remove while active.
        sweep_idle(3010, 60, 30);
        let counters = COUNTERS.lock().unwrap();
        assert!(counters.contains_key(&k));
        drop(counters);
        // Long idle: swept.
        sweep_idle(5000, 60, 30);
        let counters = COUNTERS.lock().unwrap();
        assert!(!counters.contains_key(&k));
    }
}
