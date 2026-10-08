//! NATS → WebSocket activity bridge.
//!
//! The traffic-analyzer now aggregates per-packet events itself and
//! publishes ONE summary frame per second on the websocket topic (see
//! `traffic-analyzer/src/ws_aggregator.rs`). This bridge is a thin typed
//! relay: it subscribes to the topic, validates each frame against the
//! [`ActivityFrame`] shape and broadcasts it unchanged to all WebSocket
//! clients. Previously (PR #31) the aggregation itself lived here and the
//! analyzer published one NATS message per packet — at game load NATS
//! carried the full ~100k msg/s firehose; now it carries one message per
//! second.
//!
//! The frame relayed to browsers is byte-for-byte the analyzer's frame
//! (both sides must keep this shape in sync — it is the final consumer
//! contract of `frontend/src/lib/useWebSocket.ts`):
//!
//! ```json
//! {"type":"activity",
//!  "streams":[{"id":123,"count":7,"last_time":1791464509}],
//!  "new_streams":[456],
//!  "packets_available":[123,789]}
//! ```
//!
//! - `streams`: per-stream packet-count DELTA of this window plus the
//!   latest packet timestamp (the list UI adds the delta to its displayed
//!   count).
//! - `packets_available`: stream ids with count_delta > 0 (the frontend
//!   filters by its own selection; the server does not know it).
//! - `new_streams`: stream ids seen for the first time since analyzer
//!   start (after an analyzer restart every stream is "new" once — the
//!   frontend just re-runs its streams query).
//!
//! Alert frames (`{"type":"alert",...}` from `api::special_triggers`) share
//! the same broadcaster and pass through untouched — they are low-rate.

use futures_util::StreamExt;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::broadcast;

/// ONE wire frame per aggregation window, as published by the analyzer's
/// `ws_aggregator` and relayed to browsers unchanged.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActivityFrame {
    #[serde(rename = "type")]
    pub kind: String,
    /// Sorted by stream id for a stable wire shape.
    pub streams: Vec<StreamActivity>,
    /// First-seen stream ids of this window, sorted.
    pub new_streams: Vec<usize>,
    /// Stream ids with count > 0 this window, sorted.
    pub packets_available: Vec<usize>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamActivity {
    pub id: usize,
    /// Packet count DELTA of this window (not the stream total).
    pub count: u64,
    /// Latest packet timestamp (seconds) seen this window.
    pub last_time: u64,
}

/// Spawn the NATS → WebSocket bridge: subscribe to `topic`, decode each
/// message as an [`ActivityFrame`] and broadcast the re-serialized frame to
/// all WebSocket clients via `tx`. Frames arrive at ~1/s (the analyzer only
/// publishes non-idle windows), so this loop is effectively idle.
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

        loop {
            let Some(message) = subscriber.next().await else { break };
            let frame: ActivityFrame = match serde_json::from_slice(&message.payload) {
                Ok(frame) => frame,
                Err(e) => {
                    error!("Failed to decode activity frame from NATS ({} bytes): {}",
                        message.payload.len(), e);
                    continue;
                }
            };
            match serde_json::to_string(&frame) {
                Ok(json) => match tx.send(json) {
                    Ok(_) => debug!(
                        "Relayed activity frame: {} streams, {} new",
                        frame.streams.len(),
                        frame.new_streams.len()
                    ),
                    Err(_) => debug!("No WebSocket clients connected or channel full"),
                },
                Err(e) => error!("Failed to serialize activity frame: {}", e),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The relayed frame must be byte-compatible with the analyzer's
    /// publisher output (traffic-analyzer/src/ws_aggregator.rs) and with
    /// the frontend contract.
    #[test]
    fn frame_json_roundtrips_analyzer_shape() {
        let json = r#"{"type":"activity","streams":[{"id":123,"count":7,"last_time":1791464509},{"id":456,"count":1,"last_time":1791464510}],"new_streams":[123,456],"packets_available":[123,456]}"#;
        let frame: ActivityFrame = serde_json::from_str(json).unwrap();
        assert_eq!(frame.kind, "activity");
        assert_eq!(frame.streams.len(), 2);
        assert_eq!(frame.streams[0], StreamActivity { id: 123, count: 7, last_time: 1791464509 });
        assert_eq!(frame.new_streams, vec![123, 456]);
        // Relay must not reshape the frame.
        let out = serde_json::to_string(&frame).unwrap();
        let v1: serde_json::Value = serde_json::from_str(json).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v1, v2);
    }

    #[test]
    fn malformed_frame_is_rejected_not_panicking() {
        assert!(serde_json::from_str::<ActivityFrame>("{\"type\":\"activity\"}").is_err());
        assert!(serde_json::from_str::<ActivityFrame>("not json").is_err());
    }

    /// Live end-to-end bridge test against a scratch NATS broker: publish
    /// analyzer-shaped summary frames, assert the bridge relays them to the
    /// broadcaster unchanged.
    ///
    /// Ignored by default (needs a broker): run with
    /// `NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --lib ws_bridge_live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires a scratch NATS broker (NATS_TEST_URL, default nats://127.0.0.1:4222)"]
    async fn ws_bridge_live_relays_summary_frames() {
        use std::time::Duration;

        let url =
            std::env::var("NATS_TEST_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let client = async_nats::connect(&url).await.expect("scratch NATS reachable");

        let (tx, mut rx) = broadcast::channel::<String>(64);
        let topic = format!("ws-bridge-test-{}", std::process::id());
        let bridge = spawn_ws_bridge(client.clone(), Arc::new(tx), topic.clone());

        // Give the bridge a moment to register its subscription.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Publish two analyzer-shaped frames (one per simulated second).
        let frames = [
            r#"{"type":"activity","streams":[{"id":42,"count":25,"last_time":1001}],"new_streams":[42],"packets_available":[42]}"#,
            r#"{"type":"activity","streams":[{"id":42,"count":9,"last_time":1002},{"id":77,"count":1,"last_time":1002}],"new_streams":[77],"packets_available":[42,77]}"#,
        ];
        for f in frames {
            client.publish(topic.clone(), f.as_bytes().to_vec().into()).await.unwrap();
        }

        let mut got: Vec<serde_json::Value> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while got.len() < 2 {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(msg)) => got.push(serde_json::from_str(&msg).expect("valid JSON frame")),
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Err(_) => break, // deadline
            }
        }
        bridge.abort();

        assert_eq!(got.len(), 2, "both frames relayed");
        assert_eq!(got[0]["streams"][0]["id"], 42);
        assert_eq!(got[0]["streams"][0]["count"], 25);
        assert_eq!(got[1]["new_streams"], serde_json::json!([77]));
        assert_eq!(got[1]["packets_available"], serde_json::json!([42, 77]));
    }
}
