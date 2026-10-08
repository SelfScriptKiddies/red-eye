//! NATS → WebSocket activity bridge.
//!
//! The traffic-analyzer publishes one NATS message per packet to the
//! websocket topic (see `traffic-analyzer/src/batch_flusher.rs`). At game
//! load that is a firehose: the old bridge forwarded them to browsers as
//! `packet_batch` frames (~1000 frames/s, ~12 MB/s per socket) which the
//! frontend parsed and then discarded — nobody consumed the per-packet
//! payloads.
//!
//! This module instead aggregates the SAME NATS stream server-side and
//! broadcasts exactly ONE compact frame per second:
//!
//! ```json
//! {"type":"activity",
//!  "streams":[{"id":123,"count":7,"last_time":1791464509}],
//!  "new_streams":[456],
//!  "packets_available":[123,789]}
//! ```
//!
//! - `streams`: per-stream packet-count DELTA of this window plus the latest
//!   packet timestamp (the list UI adds the delta to its displayed count).
//! - `packets_available`: stream ids with count_delta > 0 (the frontend
//!   filters by its own selection; the server does not know it).
//! - `new_streams`: stream ids seen for the first time since backend start —
//!   the frontend invalidates its `streams` query. Streams created before
//!   backend start are already covered by the initial list query, so the
//!   in-memory set starts empty (first window marks everything new once).
//!
//! Alert frames (`{"type":"alert",...}` from `api::special_triggers`) share
//! the same broadcaster and pass through untouched — they are low-rate.
//!
//! NOTE: per-packet events still cross NATS → backend at full rate; this
//! bridge only fixes the BROWSER-side firehose. Thinning the NATS-side
//! per-packet traffic (e.g. publishing aggregates straight from the
//! analyzer's batch flusher) is a separate follow-up.

use futures_util::StreamExt;
use log::{debug, error, info};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Aggregation window length: one frame per second.
const WINDOW_SECS: u64 = 1;

/// Per-packet event decoded from the NATS websocket topic (same structure
/// as the one published by traffic-analyzer's batch_flusher).
#[derive(serde::Serialize, serde::Deserialize, bincode::Decode)]
pub struct PacketEvent {
    pub packet_id: i64,
    pub stream_id: usize,
    pub timestamp_seconds: u64,
    pub timestamp_microseconds: u64,
    pub direction: String,
}

/// ONE wire frame per aggregation window. Field names are the wire contract
/// consumed by `frontend/src/lib/useWebSocket.ts`.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct ActivityFrame {
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// Sorted by stream id for a stable wire shape.
    pub streams: Vec<StreamActivity>,
    /// First-seen stream ids of this window, sorted.
    pub new_streams: Vec<usize>,
    /// Stream ids with count_delta > 0 this window, sorted.
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
///
/// Pure state machine: `record()` ingests decoded NATS events, `take_frame()`
/// drains the window and returns the frame to broadcast (or `None` when the
/// window is empty — nothing is sent for idle seconds).
pub struct ActivityAggregator {
    /// Stream ids seen at least once since backend start (new-stream detector).
    seen_streams: HashSet<usize>,
    /// First-seen ids among the current window (drained by take_frame).
    new_streams: Vec<usize>,
    /// Per-stream deltas of the current window.
    window: HashMap<usize, WindowDelta>,
}

impl ActivityAggregator {
    pub fn new() -> Self {
        Self {
            seen_streams: HashSet::new(),
            new_streams: Vec::new(),
            window: HashMap::new(),
        }
    }

    pub fn record(&mut self, event: &PacketEvent) {
        let entry = self.window.entry(event.stream_id).or_default();
        entry.count_delta += 1;
        if event.timestamp_seconds > entry.last_time {
            entry.last_time = event.timestamp_seconds;
        }
        if self.seen_streams.insert(event.stream_id) {
            self.new_streams.push(event.stream_id);
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

impl Default for ActivityAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the NATS → WebSocket bridge: subscribe to `topic`, aggregate
/// per-packet events into ONE activity frame per `WINDOW_SECS`, broadcast
/// the serialized frame to all WebSocket clients via `tx`.
///
/// Plain tokio task over a NATS subscriber + broadcast sender (no actix
/// state), so tests can spawn it against a scratch NATS broker directly.
pub fn spawn_ws_bridge(
    nats_client: async_nats::Client,
    tx: Arc<broadcast::Sender<String>>,
    topic: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("Subscribing to NATS topic {} for WebSocket activity", topic);

        let mut subscriber = match nats_client.subscribe(topic.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to NATS topic {}: {}", topic, e);
                return;
            }
        };

        let mut aggregator = ActivityAggregator::new();
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(WINDOW_SECS));
        // Never burst after a slow tick (event flood stalling the loop):
        // skip missed windows instead of flushing several empty/combined
        // frames back to back.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // interval() fires its first tick immediately — consume it so the
        // first aggregation window is a full second.
        ticker.tick().await;

        loop {
            tokio::select! {
                maybe_msg = subscriber.next() => {
                    let Some(message) = maybe_msg else { break };
                    match serde_json::from_slice::<PacketEvent>(&message.payload) {
                        Ok(event) => aggregator.record(&event),
                        Err(e) => error!("Failed to decode packet event from NATS: {}", e),
                    }
                }
                _ = ticker.tick() => {
                    let Some(frame) = aggregator.take_frame() else { continue };
                    match serde_json::to_string(&frame) {
                        Ok(json) => match tx.send(json) {
                            Ok(_) => debug!(
                                "Broadcasted activity frame: {} streams, {} new",
                                frame.streams.len(),
                                frame.new_streams.len()
                            ),
                            Err(e) => debug!("No WebSocket clients connected or channel full: {}", e),
                        },
                        Err(e) => error!("Failed to serialize activity frame: {}", e),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt(stream_id: usize, timestamp_seconds: u64) -> PacketEvent {
        PacketEvent {
            packet_id: 0,
            stream_id,
            timestamp_seconds,
            timestamp_microseconds: 0,
            direction: "orig".to_string(),
        }
    }

    #[test]
    fn deltas_accumulate_per_stream_within_window() {
        let mut agg = ActivityAggregator::new();
        for ts in [100u64, 102, 104] {
            agg.record(&evt(7, ts));
        }
        agg.record(&evt(9, 50));

        let frame = agg.take_frame().expect("non-empty window must frame");
        assert_eq!(frame.kind, "activity");
        assert_eq!(
            frame.streams,
            vec![
                StreamActivity { id: 7, count: 3, last_time: 104 },
                StreamActivity { id: 9, count: 1, last_time: 50 },
            ]
        );
        assert_eq!(frame.packets_available, vec![7, 9]);
        // Both streams are first-seen.
        assert_eq!(frame.new_streams, vec![7, 9]);
    }

    #[test]
    fn take_frame_resets_window_but_remembers_streams() {
        let mut agg = ActivityAggregator::new();
        agg.record(&evt(7, 100));
        let first = agg.take_frame().unwrap();
        assert_eq!(first.new_streams, vec![7]);
        assert_eq!(first.streams[0].count, 1);

        // Window drained: idle second emits nothing.
        assert!(agg.take_frame().is_none());

        // Same stream again → delta counts only the new window, and the
        // stream is no longer "new".
        agg.record(&evt(7, 200));
        agg.record(&evt(7, 201));
        let second = agg.take_frame().unwrap();
        assert_eq!(second.streams, vec![StreamActivity { id: 7, count: 2, last_time: 201 }]);
        assert!(second.new_streams.is_empty());
        assert_eq!(second.packets_available, vec![7]);
    }

    #[test]
    fn last_time_is_max_of_out_of_order_timestamps() {
        let mut agg = ActivityAggregator::new();
        agg.record(&evt(1, 300));
        agg.record(&evt(1, 100));
        agg.record(&evt(1, 200));
        let frame = agg.take_frame().unwrap();
        assert_eq!(frame.streams[0].last_time, 300);
    }

    #[test]
    fn empty_window_emits_no_frame() {
        let mut agg = ActivityAggregator::new();
        assert!(agg.take_frame().is_none());
        agg.record(&evt(1, 1));
        assert!(agg.take_frame().is_some());
        assert!(agg.take_frame().is_none());
    }

    #[test]
    fn new_stream_appearing_later_is_reported_once() {
        let mut agg = ActivityAggregator::new();
        agg.record(&evt(1, 10));
        assert!(agg.take_frame().unwrap().new_streams.contains(&1));

        agg.record(&evt(2, 20)); // new mid-life
        agg.record(&evt(1, 11)); // old one keeps flowing
        let frame = agg.take_frame().unwrap();
        assert_eq!(frame.new_streams, vec![2]);
        assert_eq!(frame.streams.len(), 2);
    }

    #[test]
    fn wire_frame_json_shape_matches_frontend_contract() {
        let mut agg = ActivityAggregator::new();
        agg.record(&evt(123, 1791464509));
        agg.record(&evt(456, 1791464510));
        agg.record(&evt(123, 1791464500));

        let frame = agg.take_frame().unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();

        assert_eq!(json["type"], "activity");
        // Sorted by id: 123 first with merged delta 2.
        assert_eq!(json["streams"][0]["id"], 123);
        assert_eq!(json["streams"][0]["count"], 2);
        assert_eq!(json["streams"][0]["last_time"], 1791464509);
        assert_eq!(json["streams"][1]["id"], 456);
        assert_eq!(json["new_streams"], serde_json::json!([123, 456]));
        assert_eq!(json["packets_available"], serde_json::json!([123, 456]));
    }

    /// Live end-to-end bridge test against a scratch NATS broker.
    ///
    /// Ignored by default (needs a broker): run with
    /// `cargo test --lib ws_bridge_live -- --ignored --nocapture`
    /// and e.g. `docker run --rm -p 4222:4222 nats:2-alpine`.
    #[tokio::test]
    #[ignore = "requires a scratch NATS broker (NATS_TEST_URL, default nats://127.0.0.1:4222)"]
    async fn ws_bridge_live_one_frame_per_second() {
        use std::time::Duration;

        let url =
            std::env::var("NATS_TEST_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let client = async_nats::connect(&url).await.expect("scratch NATS reachable");

        let (tx, mut rx) = broadcast::channel::<String>(64);
        let topic = format!("ws-bridge-test-{}", std::process::id());
        let bridge = spawn_ws_bridge(client.clone(), Arc::new(tx), topic.clone());

        // Give the bridge a moment to register its subscription.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Publish ~2.5s of per-packet events: 25 events for stream 42
        // (timestamps rising), 3 events for stream 77 starting at t+1s.
        let publisher = {
            let client = client.clone();
            let topic = topic.clone();
            tokio::spawn(async move {
                for i in 0..25u64 {
                    let (stream_id, ts) = if i < 10 { (42, 1000) } else { (42, 1001) };
                    let event = serde_json::json!({
                        "packet_id": i,
                        "stream_id": stream_id,
                        "timestamp_seconds": ts,
                        "timestamp_microseconds": i * 1000,
                        "direction": "orig",
                    });
                    client
                        .publish(topic.clone(), serde_json::to_vec(&event).unwrap().into())
                        .await
                        .unwrap();
                    if i == 10 {
                        let event77 = serde_json::json!({
                            "packet_id": 9000,
                            "stream_id": 77,
                            "timestamp_seconds": 1001,
                            "timestamp_microseconds": 0,
                            "direction": "dest",
                        });
                        client
                            .publish(topic.clone(), serde_json::to_vec(&event77).unwrap().into())
                            .await
                            .unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };

        // Collect frames for long enough to cover the publish window (~2.5s)
        // plus the closing window flush.
        let mut frames: Vec<serde_json::Value> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while let Ok(Ok(msg)) =
            tokio::time::timeout_at(deadline, rx.recv()).await
        {
            frames.push(serde_json::from_str(&msg).expect("frame is valid JSON"));
        }
        publisher.await.unwrap();
        bridge.abort();

        eprintln!("--- live bridge frames ({}) ---", frames.len());
        for f in &frames {
            eprintln!("{}", serde_json::to_string(f).unwrap());
        }

        // ~3 windows of traffic → a handful of frames, NOT ~1000/s.
        assert!(
            frames.len() >= 2 && frames.len() <= 5,
            "expected 2..=5 frames over ~3s, got {}",
            frames.len()
        );

        for f in &frames {
            assert_eq!(f["type"], "activity", "every frame is an activity frame");
        }

        // Stream 42 saw all 25 packets, stream 77 saw 1.
        let count_of = |sid: i64| -> u64 {
            frames
                .iter()
                .flat_map(|f| f["streams"].as_array().cloned().unwrap_or_default())
                .filter(|s| s["id"].as_i64() == Some(sid))
                .map(|s| s["count"].as_u64().unwrap_or(0))
                .sum()
        };
        assert_eq!(count_of(42), 25, "all stream-42 events accounted for");
        assert_eq!(count_of(77), 1, "all stream-77 events accounted for");

        // last_time per stream = max published timestamp.
        let last_of = |sid: i64| -> u64 {
            frames
                .iter()
                .flat_map(|f| f["streams"].as_array().cloned().unwrap_or_default())
                .filter(|s| s["id"].as_i64() == Some(sid))
                .map(|s| s["last_time"].as_u64().unwrap_or(0))
                .max()
                .unwrap_or(0)
        };
        assert_eq!(last_of(42), 1001);
        assert_eq!(last_of(77), 1001);

        // Each stream reported "new" exactly once across all frames.
        let new_count_of = |sid: i64| -> usize {
            frames
                .iter()
                .flat_map(|f| f["new_streams"].as_array().cloned().unwrap_or_default())
                .filter(|v| v.as_i64() == Some(sid))
                .count()
        };
        assert_eq!(new_count_of(42), 1);
        assert_eq!(new_count_of(77), 1);

        // Every frame with stream activity lists the ids in packets_available.
        for f in &frames {
            let streams: Vec<i64> = f["streams"]
                .as_array()
                .map(|a| a.iter().filter_map(|s| s["id"].as_i64()).collect())
                .unwrap_or_default();
            let avail: Vec<i64> = f["packets_available"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
                .unwrap_or_default();
            assert_eq!(streams, avail);
        }
    }
}
