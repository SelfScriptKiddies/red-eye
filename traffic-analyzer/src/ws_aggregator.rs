//! Analyzer-side WebSocket activity aggregation.
//!
//! The analyzer used to publish ONE NATS message per packet to the
//! websocket topic and the backend aggregated them (PR #31) — at game load
//! NATS itself carried the full ~100k msg/s firehose. This module moves the
//! aggregation to the analyzer: flushers record per-packet events into a
//! shared [`WsActivityAggregator`] and a publisher task sends exactly ONE
//! summary frame per second on the topic.
//!
//! WIRE FORMAT (analyzer → backend, `nats.websocket_packets_topic`):
//! the frame below serialized as JSON. It is byte-for-byte the frame the
//! backend's `backend/src/ws_activity.rs` bridge re-broadcasts to browsers,
//! so the backend bridge is a thin typed relay (it validates and forwards
//! the frame unchanged). Both sides must keep this shape in sync:
//!
//! ```json
//! {"type":"activity",
//!  "streams":[{"id":123,"count":7,"last_time":1791464509}],
//!  "new_streams":[456],
//!  "packets_available":[123,789]}
//! ```
//!
//! - `streams`: per-stream packet-count DELTA of this window plus the
//!   latest packet timestamp.
//! - `packets_available`: stream ids with count > 0 this window.
//! - `new_streams`: stream ids seen for the first time since analyzer
//!   start (after an analyzer restart every stream is "new" once — the
//!   frontend just re-runs its streams query; same behavior the backend
//!   had on its own restarts).

use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Aggregation window: one frame per second (matches the backend ticker
/// this replaces — the browser contract is one frame/s).
const WINDOW_SECS: u64 = 1;

/// ONE wire frame per aggregation window. Field names are the wire
/// contract consumed by `backend/src/ws_activity.rs` (relay) and
/// `frontend/src/lib/useWebSocket.ts` (final consumer).
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct ActivityFrame {
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Sorted by stream id for a stable wire shape.
    pub streams: Vec<StreamActivity>,
    /// First-seen stream ids of this window, sorted.
    pub new_streams: Vec<usize>,
    /// Stream ids with count > 0 this window, sorted.
    pub packets_available: Vec<usize>,
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct StreamActivity {
    pub id: usize,
    /// Packet count DELTA of this window (not the stream total).
    pub count: u64,
    /// Latest packet timestamp (seconds) seen this window.
    pub last_time: u64,
}

#[derive(Default)]
struct WindowDelta {
    count_delta: u64,
    last_time: u64,
}

/// Aggregates per-packet events into per-second activity frames.
/// Same state machine the backend's `ActivityAggregator` ran (PR #31),
/// relocated to the analyzer so NATS carries one message per second
/// instead of one per packet.
///
/// Pure state machine (unit-testable without NATS): `record()` ingests
/// (stream_id, timestamp) pairs, `take_frame()` drains the window and
/// returns the frame, or `None` for idle seconds (nothing is published).
pub struct WsActivityAggregator {
    /// Stream ids seen at least once since analyzer start.
    seen_streams: HashSet<usize>,
    /// First-seen ids among the current window (drained by take_frame).
    new_streams: Vec<usize>,
    /// Per-stream deltas of the current window.
    window: HashMap<usize, WindowDelta>,
}

impl WsActivityAggregator {
    pub fn new() -> Self {
        Self {
            seen_streams: HashSet::new(),
            new_streams: Vec::new(),
            window: HashMap::new(),
        }
    }

    pub fn record(&mut self, stream_id: usize, timestamp_seconds: u64) {
        let entry = self.window.entry(stream_id).or_default();
        entry.count_delta += 1;
        if timestamp_seconds > entry.last_time {
            entry.last_time = timestamp_seconds;
        }
        if self.seen_streams.insert(stream_id) {
            self.new_streams.push(stream_id);
        }
    }

    /// Drain the window into a frame. `None` when no events were recorded —
    /// idle seconds emit nothing. Window state resets; `seen_streams` (the
    /// new-stream memory) deliberately survives across windows.
    pub fn take_frame(&mut self) -> Option<ActivityFrame> {
        if self.window.is_empty() {
            // Defensive: record() always populates the window together with
            // new_streams, so this only holds when new_streams is empty too.
            self.new_streams.clear();
            return None;
        }

        let mut streams: Vec<StreamActivity> = self
            .window
            .drain()
            .map(|(id, d)| StreamActivity {
                id,
                count: d.count_delta,
                last_time: d.last_time,
            })
            .collect();
        streams.sort_by_key(|s| s.id);

        let packets_available: Vec<usize> = streams
            .iter()
            .filter(|s| s.count > 0)
            .map(|s| s.id)
            .collect();

        let mut new_streams = std::mem::take(&mut self.new_streams);
        new_streams.sort_unstable();
        new_streams.dedup();

        Some(ActivityFrame {
            kind: "activity",
            streams,
            new_streams,
            packets_available,
        })
    }
}

impl Default for WsActivityAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle the batch flushers record into. `record` is a handful of
/// HashMap ops under a std Mutex — at ~100k pkt/s that is ~1% of one core,
/// and it replaces a per-packet NATS publish.
pub type SharedWsAggregator = Arc<Mutex<WsActivityAggregator>>;

pub fn shared_aggregator() -> SharedWsAggregator {
    Arc::new(Mutex::new(WsActivityAggregator::new()))
}

/// Spawn the per-second publisher: drains the shared aggregator and
/// publishes one JSON frame per non-idle window to `topic`.
/// Never blocks a flusher: a contended lock only delays the frame slightly.
pub fn spawn_ws_aggregate_publisher(
    client: async_nats::Client,
    topic: String,
    aggregator: SharedWsAggregator,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            "WS activity aggregate publisher started (topic {}, one frame per second)",
            topic
        );
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(WINDOW_SECS));
        // Never burst after a slow tick: skip missed windows instead of
        // flushing several combined frames back to back.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // interval() fires its first tick immediately — consume it so the
        // first aggregation window is a full second.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            let frame = aggregator
                .lock()
                .map(|mut agg| agg.take_frame())
                .ok()
                .flatten();
            let Some(frame) = frame else { continue };
            match serde_json::to_vec(&frame) {
                Ok(payload) => {
                    if let Err(e) = client.publish(topic.clone(), payload.into()).await {
                        error!("Failed to publish WS activity frame: {}", e);
                    } else {
                        debug!(
                            "Published activity frame: {} streams, {} new",
                            frame.streams.len(),
                            frame.new_streams.len()
                        );
                    }
                }
                Err(e) => error!("Failed to serialize activity frame: {}", e),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_accumulate_per_stream_within_window() {
        let mut agg = WsActivityAggregator::new();
        for ts in [100u64, 102, 104] {
            agg.record(7, ts);
        }
        agg.record(9, 50);

        let frame = agg.take_frame().expect("non-empty window must frame");
        assert_eq!(frame.kind, "activity");
        assert_eq!(
            frame.streams,
            vec![
                StreamActivity {
                    id: 7,
                    count: 3,
                    last_time: 104
                },
                StreamActivity {
                    id: 9,
                    count: 1,
                    last_time: 50
                },
            ]
        );
        assert_eq!(frame.packets_available, vec![7, 9]);
        assert_eq!(frame.new_streams, vec![7, 9]);
    }

    #[test]
    fn take_frame_resets_window_but_remembers_streams() {
        let mut agg = WsActivityAggregator::new();
        agg.record(7, 100);
        let first = agg.take_frame().unwrap();
        assert_eq!(first.new_streams, vec![7]);

        assert!(agg.take_frame().is_none(), "idle second emits nothing");

        agg.record(7, 200);
        agg.record(7, 201);
        let second = agg.take_frame().unwrap();
        assert_eq!(
            second.streams,
            vec![StreamActivity {
                id: 7,
                count: 2,
                last_time: 201
            }]
        );
        assert!(second.new_streams.is_empty());
        assert_eq!(second.packets_available, vec![7]);
    }

    #[test]
    fn last_time_is_max_of_out_of_order_timestamps() {
        let mut agg = WsActivityAggregator::new();
        agg.record(1, 300);
        agg.record(1, 100);
        agg.record(1, 200);
        assert_eq!(agg.take_frame().unwrap().streams[0].last_time, 300);
    }

    #[test]
    fn empty_window_emits_no_frame() {
        let mut agg = WsActivityAggregator::new();
        assert!(agg.take_frame().is_none());
        agg.record(1, 1);
        assert!(agg.take_frame().is_some());
        assert!(agg.take_frame().is_none());
    }

    #[test]
    fn wire_frame_json_shape_matches_backend_contract() {
        // Must stay byte-compatible with backend/src/ws_activity.rs (the
        // bridge re-broadcasts it to the frontend untouched).
        let mut agg = WsActivityAggregator::new();
        agg.record(123, 1_791_464_509);
        agg.record(456, 1_791_464_510);
        agg.record(123, 1_791_464_500);

        let frame = agg.take_frame().unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();

        assert_eq!(json["type"], "activity");
        assert_eq!(json["streams"][0]["id"], 123);
        assert_eq!(json["streams"][0]["count"], 2);
        assert_eq!(json["streams"][0]["last_time"], 1_791_464_509);
        assert_eq!(json["streams"][1]["id"], 456);
        assert_eq!(json["new_streams"], serde_json::json!([123, 456]));
        assert_eq!(json["packets_available"], serde_json::json!([123, 456]));
    }

    /// Live measurement for the analyzer→backend WS topic: the OLD wire
    /// (one JSON PacketEvent per packet) vs the NEW wire (one summary
    /// frame per second). Measures publish throughput and received message
    /// counts/bytes over a fixed window.
    ///
    /// Run with:
    /// `NATS_TEST_URL=nats://127.0.0.1:4222 cargo test -p traffic-analyzer ws_topic_rate -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires a scratch NATS broker (NATS_TEST_URL, default nats://127.0.0.1:4222)"]
    async fn ws_topic_rate_per_packet_vs_summary_live() {
        use futures::StreamExt;
        use std::time::Duration;

        let url = std::env::var("NATS_TEST_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let client = async_nats::connect(&url)
            .await
            .expect("scratch NATS reachable");
        let topic = format!("ws-rate-test-{}", std::process::id());
        let mut sub = client.subscribe(topic.clone()).await.unwrap();

        // Drain concurrently with publishing — otherwise the broker's
        // slow-consumer limits kick in and drop messages (itself the
        // lesson of this measurement: the old wire drowns idle consumers).
        let n = 100_000usize;
        let reader = tokio::spawn(async move {
            let mut received = 0usize;
            let mut wire_bytes = 0usize;
            while let Some(msg) = sub.next().await {
                received += 1;
                wire_bytes += msg.payload.len();
                if received >= n {
                    break;
                }
            }
            (received, wire_bytes)
        });

        // Leg 1 (old wire): one JSON PacketEvent per packet.
        let t0 = std::time::Instant::now();
        for i in 0..n {
            let event = serde_json::json!({
                "packet_id": i,
                "stream_id": i % 50,
                "timestamp_seconds": 1_700_000_000 + (i / 1000),
                "timestamp_microseconds": i % 1_000_000,
                "direction": "orig",
            });
            client
                .publish(topic.clone(), serde_json::to_vec(&event).unwrap().into())
                .await
                .unwrap();
        }
        let (received, wire_bytes) = reader.await.unwrap();
        let per_packet_ms = t0.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(received, n, "concurrent reader must see every event");

        // Leg 2 (new wire): the real publisher over the same window of
        // events fed through the aggregator — record all n events, then
        // count frames received in the following 3 seconds (expect ~3).
        let aggregator = shared_aggregator();
        let publisher =
            spawn_ws_aggregate_publisher(client.clone(), topic.clone(), aggregator.clone());
        tokio::time::sleep(Duration::from_millis(300)).await; // let the interval tick align
        {
            let mut agg = aggregator.lock().unwrap();
            for i in 0..n {
                agg.record(i % 50, 1_700_000_000u64 + (i as u64 / 1000));
            }
        }
        let mut frames = 0usize;
        let mut frame_bytes = 0usize;
        let mut frame_sum_counts = 0u64;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut sub2 = client.subscribe(topic.clone()).await.unwrap();
        while let Ok(msg) = tokio::time::timeout_at(deadline, sub2.next()).await {
            let msg = msg.expect("subscriber alive");
            frames += 1;
            frame_bytes += msg.payload.len();
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                if let Some(streams) = json["streams"].as_array() {
                    frame_sum_counts += streams
                        .iter()
                        .map(|s| s["count"].as_u64().unwrap_or(0))
                        .sum::<u64>();
                }
            }
        }
        publisher.abort();

        eprintln!(
            "BENCH ws topic, {} packet events:\n  old wire (per-packet JSON) : {:8.1} ms, {:8.0} msgs/s, {:6.2} MB on topic\n  new wire (1 frame/s)       : {} frames in 3 s, {:6.2} KB on topic (all events land in the one non-idle window)",
            n, per_packet_ms, n as f64 / (per_packet_ms / 1000.0), wire_bytes as f64 / 1e6,
            frames, frame_bytes as f64 / 1e3,
        );
        // All events were recorded inside one 1-second window, so exactly
        // one non-idle frame must carry them (idle seconds emit nothing).
        assert_eq!(
            frames, 1,
            "one non-idle window -> one frame, got {}",
            frames
        );
        assert_eq!(
            frame_sum_counts, n as u64,
            "every event accounted for in the summary frame"
        );
    }
}
