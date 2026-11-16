use crate::db::DB;
use dashmap::DashMap;
use log::{info, warn};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};

/// Per-stream rotation metadata the packet path needs. `retained_bytes`
/// mirrors the DB retention_bytes column, adjusted by flush deltas between
/// polls; both sides only estimate, so a slightly late rotation is a
/// documented degradation, never a lost packet.
#[derive(Debug, Clone)]
pub struct StreamRetention {
    pub retained_bytes: i64,
    pub segment_index: i32,
}

pub struct RetentionTracker {
    entries: DashMap<i64, StreamRetention>,
    /// favorite_segment_bytes from app_settings (0 disables rotation).
    segment_limit: AtomicI64,
}

/// Pure rotation decision: a starred segment rotates when the incoming
/// packet would push it past the segment byte limit. Extracted for tests.
pub fn should_rotate(favorite: bool, retained: i64, incoming: usize, limit: i64) -> bool {
    favorite && limit > 0 && retained.saturating_add(incoming as i64) > limit
}

impl RetentionTracker {
    pub fn new() -> Self {
        RetentionTracker {
            entries: DashMap::new(),
            segment_limit: AtomicI64::new(0),
        }
    }

    pub fn segment_limit(&self) -> i64 {
        self.segment_limit.load(Ordering::Relaxed)
    }

    pub fn rotation_needed(&self, stream_id: usize, incoming: usize) -> bool {
        match self.entries.get(&(stream_id as i64)) {
            Some(entry) => {
                should_rotate(true, entry.retained_bytes, incoming, self.segment_limit())
            }
            None => false,
        }
    }

    pub fn add_bytes(&self, stream_id: usize, delta: i64) {
        if let Some(mut entry) = self.entries.get_mut(&(stream_id as i64)) {
            entry.retained_bytes = entry.retained_bytes.saturating_add(delta);
        }
    }

    /// Replace the tracked population with the current starred set. Local
    /// deltas made between the DB read and this call are absorbed by the
    /// next flush (the DB column is the source of truth).
    pub fn apply_snapshot(&self, snapshot: Vec<(i64, i64, i32)>) {
        let fresh: std::collections::HashSet<i64> = snapshot.iter().map(|(id, _, _)| *id).collect();
        for (id, bytes, segment_index) in snapshot {
            self.entries.insert(
                id,
                StreamRetention {
                    retained_bytes: bytes,
                    segment_index,
                },
            );
        }
        self.entries.retain(|k, _| fresh.contains(k));
    }

    fn set_segment_limit(&self, limit: i64) {
        self.segment_limit.store(limit, Ordering::Relaxed);
    }
}

impl Default for RetentionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Supervised poller: starred segments + the segment byte limit, both
/// bounded single queries. Poll failures are logged and retried on the next
/// tick; the packet path keeps running with the last snapshot (stale limit
/// delays or advances a rotation, it never loses data).
pub fn spawn_retention_poller(db: DB, tracker: Arc<RetentionTracker>, interval_secs: u64) {
    tokio::spawn(async move {
        let default_limit: i64 = 268_435_456;
        let mut ticker = interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            ticker.tick().await;
            match poll_once(&db, &tracker, default_limit).await {
                Ok(()) => {}
                Err(e) => warn!("Retention poll failed (retrying next tick): {}", e),
            }
        }
    });
}

async fn poll_once(
    db: &DB,
    tracker: &RetentionTracker,
    default_limit: i64,
) -> Result<(), sqlx::Error> {
    let limit = db.favorite_segment_limit().await?.unwrap_or(default_limit);
    let prev = tracker.segment_limit();
    tracker.set_segment_limit(limit);
    if prev != limit {
        info!("Favorite segment limit: {} bytes", limit);
    }
    let snapshot = db.starred_streams_snapshot().await?;
    tracker.apply_snapshot(snapshot);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unstarred_streams_never_rotate() {
        assert!(!should_rotate(false, i64::MAX, 10, 100));
    }

    #[test]
    fn zero_limit_disables_rotation() {
        // Missing/invalid setting degrades to no rotation (over-limit
        // growth) instead of rotating every packet.
        assert!(!should_rotate(true, 1000, 10, 0));
        assert!(!should_rotate(true, 1000, 10, -5));
    }

    #[test]
    fn rotation_fires_exactly_past_the_limit() {
        // At the limit exactly: still fits.
        assert!(!should_rotate(true, 90, 10, 100));
        // One byte over: rotate.
        assert!(should_rotate(true, 91, 10, 100));
    }

    #[test]
    fn oversized_packet_on_fresh_segment_rotates_once() {
        // A single packet bigger than the whole segment limit must rotate
        // (then land in the fresh, unstarred segment - which never rotates
        // again until starred).
        assert!(should_rotate(true, 0, 500, 100));
    }

    #[test]
    fn saturating_math_never_panics() {
        assert!(should_rotate(true, i64::MAX, usize::MAX, 100));
    }

    #[test]
    fn tracker_tracks_only_starred_and_applies_snapshots() {
        let tracker = RetentionTracker::new();
        assert!(
            !tracker.rotation_needed(1, 10),
            "unknown stream is unstarred"
        );

        tracker.apply_snapshot(vec![(1, 91, 1)]);
        tracker.set_segment_limit(100);
        assert!(tracker.rotation_needed(1, 10), "91 + 10 exceeds 100");
        assert!(!tracker.rotation_needed(1, 5), "91 + 5 still fits");
        assert_eq!(tracker.entries.get(&1).unwrap().segment_index, 1);

        // Flusher deltas accumulate between polls.
        tracker.add_bytes(1, 3);
        assert!(tracker.rotation_needed(1, 7), "94 + 7 exceeds 100");

        // A stream that stopped being starred leaves the tracker.
        tracker.apply_snapshot(vec![(2, 0, 1)]);
        assert!(!tracker.rotation_needed(1, 10_000_000));
        assert!(tracker.entries.get(&2).is_some());
    }
}
