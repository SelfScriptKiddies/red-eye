use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_nats::{Client, ConnectOptions};
use log::{error, warn};
use tokio::sync::mpsc;

use crate::config::AgentConfig;

// WIRE FORMAT — agent → analyzer, raw packets topic (bincode, standard config):
//   (RAW_PACKET_WIRE_VERSION: u32, Vec<RawPacket>)
//   RawPacket = (tv_usec: i64, tv_sec: i64, ethernet_frame: Vec<u8>)
// One NATS message carries a BATCH of packets: up to `nats_batch_size`
// packets or `nats_batch_interval_ms` (whichever first). The envelope is
// ALWAYS a versioned array — single packets travel as one-element Vecs;
// there is deliberately no bare-tuple compatibility mode: agent and
// analyzer deploy together from this repo, so a clean break is safe.
// The decoder lives in traffic-analyzer/src/message_broker.rs and MUST
// mirror this constant and shape.
pub const RAW_PACKET_WIRE_VERSION: u32 = 2;

/// One raw packet queued for the batcher. Field order is part of the wire
/// contract: (tv_usec, tv_sec, frame).
pub type RawPacket = (i64, i64, Vec<u8>);

/// Capacity of the batching queue inside the NATS client. Bounded so a
/// stalled/slow broker degrades into counted packet drops, never into a
/// blocked capture pipeline (the sniffer's own queue sits in front of
/// this one and keeps the same guarantee for the capture thread itself).
const PACKET_QUEUE_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct NatsClient {
    client: Client,
    pub raw_packets_topic: String,
    pub agent_id: String,
    packet_tx: mpsc::Sender<RawPacket>,
    batch_drops: Arc<AtomicU64>,
}

impl NatsClient {
    pub async fn connect(config: &AgentConfig, agent_id: &str) -> anyhow::Result<Self> {
        let client = ConnectOptions::with_user_and_password(
            config.nats_login.clone(),
            config.nats_password.clone(),
        )
        .connect(&config.nats_host)
        .await?;

        let (packet_tx, packet_rx) = mpsc::channel::<RawPacket>(PACKET_QUEUE_CAPACITY);
        let batch_drops = Arc::new(AtomicU64::new(0));
        tokio::spawn(run_packet_batcher(
            client.clone(),
            config.nats_raw_packets_topic.clone(),
            packet_rx,
            config.nats_batch_size,
            config.nats_batch_interval_ms,
            batch_drops.clone(),
        ));

        Ok(Self {
            client,
            raw_packets_topic: config.nats_raw_packets_topic.clone(),
            agent_id: agent_id.to_string(),
            packet_tx,
            batch_drops,
        })
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Packets dropped because the batching queue was full (broker slow or
    /// down). Losing packets is acceptable degradation; blocking the
    /// capture pipeline is not.
    pub fn batch_queue_drops(&self) -> u64 {
        self.batch_drops.load(Ordering::Relaxed)
    }

    /// Queue one raw packet for batched publishing. Never blocks: a full
    /// queue drops the packet with a counter (warn every 1000th drop).
    /// Sync on purpose — callers are packet-forwarding loops that must not
    /// await a broker.
    pub fn send_packet(&self, packet: RawPacket) {
        match self.packet_tx.try_send(packet) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = self.batch_drops.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped.is_multiple_of(1000) {
                    warn!("NATS packet batching queue full, packets dropped so far: {}", dropped);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("Packet batcher task is gone, dropping packet");
            }
        }
    }

    pub async fn send_heartbeat(&self, status_json: Vec<u8>) {
        let topic = format!("agent_status.{}", self.agent_id);
        if let Err(e) = self.client.publish(topic, status_json.into()).await {
            error!("Failed to send heartbeat: {}", e);
        }
    }

    /// Publish a Docker containers listing reply (DockerListContainers
    /// command handler) — same pattern as the heartbeat topic.
    pub async fn send_docker_containers(&self, status_json: Vec<u8>) {
        let topic = format!("agent_docker.{}", self.agent_id);
        if let Err(e) = self.client.publish(topic, status_json.into()).await {
            error!("Failed to send docker containers: {}", e);
        }
    }

    pub async fn subscribe_commands(&self) -> anyhow::Result<async_nats::Subscriber> {
        let topic = format!("agent_commands.{}", self.agent_id);
        let sub = self.client.subscribe(topic).await?;
        Ok(sub)
    }
}

/// Encode the versioned batch envelope (see RAW_PACKET_WIRE_VERSION).
/// Public for tests; the wire contract is mirrored in the analyzer.
pub fn encode_packet_batch(packets: Vec<RawPacket>) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::encode_to_vec(
        (RAW_PACKET_WIRE_VERSION, packets),
        bincode::config::standard(),
    )?)
}

/// The batcher: collects packets and publishes one NATS message per batch
/// (K packets or `interval_ms`, whichever first). Publishing inside this
/// task is the only place the pipeline ever awaits the broker — backpressure
/// surfaces as a filling queue and counted drops upstream.
async fn run_packet_batcher(
    client: Client,
    topic: String,
    mut rx: mpsc::Receiver<RawPacket>,
    max_batch: usize,
    interval_ms: u64,
    drops: Arc<AtomicU64>,
) {
    let max_batch = max_batch.max(1);
    // interval (NOT sleep-in-select): a sleep would be recreated on every
    // loop iteration, so steady drip traffic below `max_batch` would keep
    // resetting it and partial batches would sit far longer than
    // `interval_ms`. interval() ticks on a fixed schedule regardless of
    // recv activity.
    let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // first tick fires immediately — consume it
    let mut buffer: Vec<RawPacket> = Vec::with_capacity(max_batch);

    loop {
        tokio::select! {
            biased;
            maybe = rx.recv() => {
                match maybe {
                    Some(pkt) => {
                        buffer.push(pkt);
                        if buffer.len() >= max_batch {
                            publish_batch(&client, &topic, &mut buffer, &drops).await;
                        }
                    }
                    None => {
                        // Queue closed (client dropped) — flush and exit.
                        publish_batch(&client, &topic, &mut buffer, &drops).await;
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                publish_batch(&client, &topic, &mut buffer, &drops).await;
            }
        }
    }
}

async fn publish_batch(
    client: &Client,
    topic: &str,
    buffer: &mut Vec<RawPacket>,
    drops: &Arc<AtomicU64>,
) {
    if buffer.is_empty() {
        return;
    }
    let count = buffer.len();
    // std::mem::take leaves an (empty) Vec behind so the caller's buffer is
    // reusable even on the error path.
    let payload = match encode_packet_batch(std::mem::take(buffer)) {
        Ok(p) => p,
        Err(e) => {
            // Encode of a Vec of tuples cannot realistically fail; if it
            // does, count the loss and move on (degradation, not crash).
            error!("Failed to encode packet batch ({} packets): {}", count, e);
            drops.fetch_add(count as u64, Ordering::Relaxed);
            return;
        }
    };
    if let Err(e) = client.publish(topic.to_string(), payload.into()).await {
        error!("Failed to send packet batch ({} packets) to NATS: {}", count, e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn envelope_roundtrip_matches_analyzer_decoder_shape() {
        // Encode here, decode with the SAME shape the analyzer uses
        // (u32 version, Vec<RawPacket>) — a regression in either side's
        // tuple order or version breaks this test.
        let packets: Vec<RawPacket> = vec![
            (111_111i64, 1_700_000_000i64, vec![0x00, 0x01, 0xFF, 0x0a]),
            (222_222i64, 1_700_000_001i64, vec![]),
        ];
        let bytes = encode_packet_batch(packets.clone()).unwrap();

        let ((version, decoded), _): ((u32, Vec<RawPacket>), usize) =
            bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(version, RAW_PACKET_WIRE_VERSION);
        assert_eq!(decoded, packets);
        // Version must match the analyzer's constant (kept in sync by
        // deploy-together policy + this comment: message_broker.rs).
        assert_eq!(RAW_PACKET_WIRE_VERSION, 2);
    }

    #[test]
    fn single_packet_is_a_one_element_envelope() {
        let bytes = encode_packet_batch(vec![(1i64, 2i64, vec![9u8])]).unwrap();
        let ((version, decoded), _): ((u32, Vec<RawPacket>), usize) =
            bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(version, RAW_PACKET_WIRE_VERSION);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], (1, 2, vec![9]));
    }

    /// Live measurement: 50k packets through the scratch broker, one
    /// message per packet vs batched (K=32) — messages/s and wire bytes.
    /// Run with:
    /// `NATS_TEST_URL=nats://127.0.0.1:4433 cargo test -p red-eye-agent nats_batching -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires a scratch NATS broker (NATS_TEST_URL, default nats://127.0.0.1:4222)"]
    async fn nats_batching_50k_single_vs_batched_live() {
        let url = std::env::var("NATS_TEST_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let client = async_nats::connect(&url).await.expect("scratch NATS reachable");
        let topic = format!("bench-raw-{}", std::process::id());
        let mut sub = client.subscribe(topic.clone()).await.unwrap();

        let make_packets = |n: usize| -> Vec<RawPacket> {
            (0..n)
                .map(|i| (i as i64, 1_700_000_000 + (i as i64 / 1000), vec![0xAB; 200]))
                .collect()
        };

        // Leg 1: one NATS message per packet (the pre-batching wire shape:
        // an envelope of exactly one packet, so both legs pay the same
        // envelope cost and the delta isolates message rate).
        let packets = make_packets(50_000);
        let t0 = std::time::Instant::now();
        for p in &packets {
            let msg = encode_packet_batch(vec![p.clone()]).unwrap();
            client.publish(topic.clone(), msg.into()).await.unwrap();
        }
        let mut received = 0usize;
        let mut wire_bytes = 0usize;
        while received < 50_000 {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(10), sub.next())
                .await
                .expect("timed out waiting for messages")
                .expect("subscriber alive");
            received += 1;
            wire_bytes += msg.payload.len();
        }
        let single_ms = t0.elapsed().as_secs_f64() * 1000.0;

        // Leg 2: batched, K=32 (what run_packet_batcher publishes).
        const K: usize = 32;
        let t0 = std::time::Instant::now();
        for chunk in packets.chunks(K) {
            let msg = encode_packet_batch(chunk.to_vec()).unwrap();
            client.publish(topic.clone(), msg.into()).await.unwrap();
        }
        let mut received_batches = 0usize;
        let mut batched_wire_bytes = 0usize;
        let expected_batches = 50_000usize.div_ceil(K);
        while received_batches < expected_batches {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(10), sub.next())
                .await
                .expect("timed out waiting for batches")
                .expect("subscriber alive");
            received_batches += 1;
            batched_wire_bytes += msg.payload.len();
        }
        let batched_ms = t0.elapsed().as_secs_f64() * 1000.0;

        eprintln!(
            "BENCH nats 50k packets (200 B payload each):\n  single-msg : {:8.1} ms total, {:8.0} msgs/s, {:7.2} MB wire\n  batched K={} : {:8.1} ms total, {:8.0} msgs/s, {:7.2} MB wire  (msg rate reduced {}x)",
            single_ms, 50_000.0 / (single_ms / 1000.0), wire_bytes as f64 / 1e6,
            K, batched_ms, expected_batches as f64 / (batched_ms / 1000.0), batched_wire_bytes as f64 / 1e6,
            32,
        );
    }

    /// Live: a partial batch (below K) must be flushed by the INTERVAL
    /// timer, not wait for more packets — guards the interval-vs-sleep
    /// distinction in run_packet_batcher.
    #[tokio::test]
    #[ignore = "requires a scratch NATS broker (NATS_TEST_URL, default nats://127.0.0.1:4222)"]
    async fn nats_partial_batch_flushes_on_timer_live() {
        use clap::Parser;
        use futures::StreamExt;

        let url = std::env::var("NATS_TEST_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let config = AgentConfig::parse_from([
            "red-eye-agent",
            "--nats-host", &url,
            "--nats-batch-size", "32",
            "--nats-batch-interval-ms", "10",
        ]);
        let nats = NatsClient::connect(&config, "batcher-test").await.unwrap();
        let mut sub = nats.inner().subscribe(config.nats_raw_packets_topic.clone()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // 3 packets (< K=32): must arrive as ONE batch within a few ticks.
        for i in 0..3i64 {
            nats.send_packet((i, 1_700_000_000, vec![0xCD; 64]));
        }
        let t0 = std::time::Instant::now();
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
            .await
            .expect("partial batch must flush on the interval timer, not stall")
            .expect("subscriber alive");
        let elapsed = t0.elapsed();

        let ((version, packets), _): ((u32, Vec<RawPacket>), usize) =
            bincode::decode_from_slice(&msg.payload, bincode::config::standard()).unwrap();
        assert_eq!(version, RAW_PACKET_WIRE_VERSION);
        assert_eq!(packets.len(), 3, "all three packets in the timed batch");
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "flushed by timer quickly, took {:?}",
            elapsed
        );
        assert_eq!(nats.batch_queue_drops(), 0, "no drops in this scenario");
    }
}
