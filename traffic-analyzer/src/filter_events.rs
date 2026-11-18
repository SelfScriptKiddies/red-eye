//! Threshold alert/block events: bounded in-memory queue, batched
//! analyzer-side DB writes with the same bounded-retry pattern as the
//! batch flusher. Losing one event is acceptable degradation (counted and
//! logged); blocking or panicking is not.

use log::{error, info, warn};
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::db::DB;

/// Queue bound: beyond this, new events are dropped and counted (a flood
/// of threshold fires must not grow analyzer memory without limit).
const MAX_QUEUE: usize = 10_000;

static QUEUE: Lazy<Mutex<Vec<FilterEventRow>>> = Lazy::new(|| Mutex::new(Vec::new()));
pub static EVENT_QUEUE_DROPS: AtomicU64 = AtomicU64::new(0);
static EVENT_WRITE_DROPS: AtomicU64 = AtomicU64::new(0);

/// One filter_event row (kind: "alert" | "block").
#[derive(Debug, Clone)]
pub struct FilterEventRow {
    pub filter_id: i64,
    pub agent_id: String,
    pub service_id: Option<i64>,
    pub stream_id: Option<i64>,
    pub remote_ip: Option<String>,
    pub kind: String,
    pub window_secs: Option<i32>,
    pub match_count: Option<i32>,
}

/// Queue one event. Never blocks; overflow drops the event and counts it.
pub fn queue_event(row: FilterEventRow) {
    let mut queue = QUEUE.lock().unwrap();
    if queue.len() >= MAX_QUEUE {
        EVENT_QUEUE_DROPS.fetch_add(1, Ordering::Relaxed);
        return;
    }
    queue.push(row);
}

/// Batch insert rows; one retry after a short backoff, then drop the batch
/// with a counter (same bounded-retry shape as insert_packets_with_retry).
async fn flush_events(db: &DB, rows: &[FilterEventRow]) {
    if rows.is_empty() {
        return;
    }
    if let Err(e) = db.insert_filter_events_batch(rows).await {
        error!("Filter event insert failed, retrying once: {:?}", e);
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        if let Err(e) = db.insert_filter_events_batch(rows).await {
            let dropped = EVENT_WRITE_DROPS.fetch_add(rows.len() as u64, Ordering::Relaxed)
                + rows.len() as u64;
            warn!(
                "Filter event insert failed after retry, dropping {} events ({} dropped total): {:?}",
                rows.len(),
                dropped,
                e
            );
            return;
        }
    }
    info!("Flushed {} filter events", rows.len());
}

/// Drain the queue into the DB. Called by the background flusher.
async fn flush_queue(db: &DB) {
    let rows: Vec<FilterEventRow> = {
        let mut queue = QUEUE.lock().unwrap();
        std::mem::take(&mut *queue)
    };
    flush_events(db, &rows).await;
}

/// Spawn the periodic filter-event flusher. A DB failure degrades to the
/// bounded retry above; the task itself never exits on a flush error.
pub fn spawn_event_flusher(db: DB) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.tick().await; // skip the immediate first tick
        loop {
            interval.tick().await;
            flush_queue(&db).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(n: u64) -> FilterEventRow {
        FilterEventRow {
            filter_id: n as i64,
            agent_id: "a".into(),
            service_id: None,
            stream_id: None,
            remote_ip: None,
            kind: "alert".into(),
            window_secs: Some(10),
            match_count: Some(3),
        }
    }

    #[test]
    fn queue_is_bounded_and_counts_drops() {
        let before = EVENT_QUEUE_DROPS.load(Ordering::Relaxed);
        {
            let mut queue = QUEUE.lock().unwrap();
            queue.clear();
            for _ in 0..MAX_QUEUE {
                queue.push(row(1));
            }
        }
        queue_event(row(2));
        assert!(EVENT_QUEUE_DROPS.load(Ordering::Relaxed) > before);
        let queue = QUEUE.lock().unwrap();
        assert_eq!(queue.len(), MAX_QUEUE);
        assert_ne!(queue.last().unwrap().filter_id, 2);
    }
}
