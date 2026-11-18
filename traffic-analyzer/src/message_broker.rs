use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions};
use futures::StreamExt;
use log::{error, info, trace};
use prost::Message as _;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task;

use crate::batch_flusher::ShardedBatchSender;
use crate::cache::Cache;
use crate::configuration::ApplicationConfig;
use crate::db::DB;
use crate::ethernet::parse_packet;
use crate::metrics;
use crate::process_packet::{PacketContext, PacketProcessor};
use crate::wire::{PacketBatch, WireRawPacket};

// One NATS message carries a BATCH of packets (agent batches up to K packets or ~10 ms, whichever first)
// version 4: protobuf contract with batch-level agent attribution (agent_id)
pub const RAW_PACKET_WIRE_VERSION: u32 = 4;

/// One raw packet as consumed downstream, converted from the protobuf
/// type at the decode boundary: (tv_usec, tv_sec, ethernet_frame).
pub type RawPacket = (i64, i64, Vec<u8>);

/// Decoded batch: producing agent identity plus the packets.
pub struct DecodedBatch {
    pub agent_id: String,
    pub packets: Vec<RawPacket>,
}

/// Decode the versioned batch envelope
fn decode_raw_packet_batch(payload: &[u8]) -> Result<DecodedBatch, String> {
    let batch = PacketBatch::decode(payload).map_err(|e| format!("protobuf decode failed: {e}"))?;
    if batch.version != RAW_PACKET_WIRE_VERSION {
        return Err(format!(
            "unknown raw-packet wire version {} (expected {}) — analyzer/agent skew?",
            batch.version, RAW_PACKET_WIRE_VERSION
        ));
    }
    Ok(DecodedBatch {
        agent_id: batch.agent_id,
        packets: batch
            .packets
            .into_iter()
            .map(|p: WireRawPacket| (p.tv_usec as i64, p.tv_sec as i64, p.ethernet_frame))
            .collect(),
    })
}

pub async fn create_consumer(config: &ApplicationConfig) -> Result<Client> {
    let host = config.nats_host.clone();
    let client = ConnectOptions::with_user_and_password(
        config.nats_login.clone(),
        config.nats_password.clone(),
    )
    .connect(&host)
    .await
    .with_context(|| format!("Error connecting to NATS: {}", &host))?;

    Ok(client)
}

pub async fn consume_raw_packets(
    client: Client,
    cache: Arc<Cache>,
    config: Arc<ApplicationConfig>,
    db: DB,
    batch_tx: Arc<ShardedBatchSender>,
) -> Result<()> {
    let subject = config.nats_raw_packets_topic.clone();

    let mut subscriber = client.subscribe(subject.clone()).await?;
    info!("Starting to consume packets from topic {}", &subject);

    let concurrency = config.analyzer_concurrency;
    let packet_processor = Arc::new(PacketProcessor::new(cache.clone(), config, db, batch_tx));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    while let Some(message) = subscriber.next().await {
        // Get payload and clone it so it can live in a separate task
        let payload = message.payload.to_vec();

        // Unpack the versioned batch envelope
        // Malformed or skewed messages error and drop one batch.
        let batch: DecodedBatch = match decode_raw_packet_batch(&payload) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    "Error decoding packet batch ({} bytes): {}",
                    payload.len(),
                    e
                );
                continue;
            }
        };

        // Per-agent machine IP: direction detection depends on it, so a
        // batch from an unknown agent is dropped and counted rather than
        // misattributed through any global default.
        let machine_ip: IpAddr = match cache.agent_machine_ip(&batch.agent_id) {
            Some(ip) => ip,
            None => {
                let dropped = metrics::UNKNOWN_AGENT_DROPS
                    .fetch_add(batch.packets.len() as u64, Ordering::Relaxed)
                    + 1;
                let agent_id = batch.agent_id.clone();
                metrics::rate_limited_log("unknown-agent", dropped, || {
                    format!(
                        "dropping packets from unregistered agent '{}': waiting for its configuration snapshot",
                        agent_id
                    )
                });
                continue;
            }
        };

        trace!(
            "Decoded batch of {} packets ({} bytes) from agent '{}'",
            batch.packets.len(),
            payload.len(),
            batch.agent_id
        );

        for packet in batch.packets {
            // Processing packet in a separate task with cloned data
            let processor_clone = packet_processor.clone();
            let context = PacketContext {
                agent_id: batch.agent_id.clone(),
                machine_ip,
            };
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            task::spawn(async move {
                trace!(
                    "Processing packet of size {} bytes: {:?}",
                    packet.2.len(),
                    packet.2
                );
                // packet.0 = usec, packet.1 = sec
                let parsed_packet = parse_packet((packet.0 as u64, packet.1 as u64), packet.2);

                if let Some(packet) = parsed_packet {
                    processor_clone
                        .process_packet(packet, &context)
                        .await
                        .unwrap_or_else(|e| {
                            error!("Error processing packet: {}", e);
                        });
                }
                drop(permit);
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_bytes(version: u32, packets: Vec<WireRawPacket>) -> Vec<u8> {
        PacketBatch {
            version,
            agent_id: "agent-a".into(),
            packets,
        }
        .encode_to_vec()
    }

    #[test]
    fn decode_roundtrips_what_the_agent_encodes() {
        let wire_packets = vec![
            WireRawPacket {
                tv_sec: 1_700_000_000,
                tv_usec: 111_111,
                ethernet_frame: vec![0x01, 0x02, 0x03, 0xff],
            },
            WireRawPacket {
                tv_sec: 1_700_000_001,
                tv_usec: 0,
                ethernet_frame: vec![],
            },
        ];
        let bytes = batch_bytes(RAW_PACKET_WIRE_VERSION, wire_packets);

        let decoded = decode_raw_packet_batch(&bytes).unwrap();
        assert_eq!(decoded.agent_id, "agent-a");
        assert_eq!(
            decoded.packets,
            vec![
                (111_111, 1_700_000_000, vec![0x01, 0x02, 0x03, 0xff]),
                (0, 1_700_000_001, vec![]),
            ]
        );
    }

    #[test]
    fn wrong_version_is_a_clean_error() {
        // A version-2 bincode-era producer (or an empty proto3 payload,
        // which decodes as version 0) must be rejected by the version gate.
        assert!(decode_raw_packet_batch(&batch_bytes(2, vec![])).is_err());
        assert!(decode_raw_packet_batch(&[]).is_err());
    }

    #[test]
    fn garbage_and_lying_lengths_error_instead_of_allocating() {
        // Random garbage (the first bytes of the outdated bincode format that caused the 10.10.2026 crash loop).
        assert!(decode_raw_packet_batch(&[0xfc, 0x28, 0x5c, 0x22, 0x06, 0x00]).is_err());

        // A lying frame length: field 2 (packets, length-delimited) declares ~34 GB.
        // prost validates the length against the bytes that remain and return error - no oversized allocation is attempted.
        let mut lie = batch_bytes(RAW_PACKET_WIRE_VERSION, vec![]);
        lie.extend_from_slice(&[0x12, 0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode_raw_packet_batch(&lie).is_err());
    }
}
