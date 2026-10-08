use bincode;
use tokio::task;
use tokio::sync::Semaphore;
use std::sync::Arc;
use anyhow::{Context, Result};
use log::{trace, error, info};
use futures::StreamExt;
use async_nats::{
    ConnectOptions,
    Client
};

use crate::db::DB;
use crate::cache::Cache;
use crate::ethernet::parse_packet;
use crate::process_packet::PacketProcessor;
use crate::configuration::ApplicationConfig;
use crate::batch_flusher::ShardedBatchSender;

// WIRE FORMAT — agent → analyzer, raw packets topic (bincode, standard config):
//   (RAW_PACKET_WIRE_VERSION: u32, Vec<RawPacket>)
//   RawPacket = (tv_usec: i64, tv_sec: i64, ethernet_frame: Vec<u8>)
// One NATS message carries a BATCH of packets (agent batches up to K
// packets or ~10 ms, whichever first). The envelope is ALWAYS a
// versioned array — single packets travel as one-element Vecs; there is
// deliberately no bare-tuple compatibility mode: agent and analyzer
// deploy together from this repo, so a clean break is safe. The encoder
// lives in agent/src/nats.rs and MUST mirror this constant and shape.
pub const RAW_PACKET_WIRE_VERSION: u32 = 2;

/// One raw packet as it crosses the wire (bincode tuple). Field order is
/// part of the wire contract: (tv_usec, tv_sec, frame).
pub type RawPacket = (i64, i64, Vec<u8>);

pub async fn create_consumer(config: &ApplicationConfig) -> Result<Client> {
    let host = config.nats_host.clone();
    let client = ConnectOptions::with_user_and_password(
        config.nats_login.clone(),
        config.nats_password.clone()
    ).connect(&host).await
    .with_context(|| format!("Error connecting to NATS: {}", &host))?;

    Ok(client)
}

pub async fn consume_raw_packets(client: Client, cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB, batch_tx: Arc<ShardedBatchSender>) -> Result<()> {
    let subject = config.nats_raw_packets_topic.clone();

    let mut subscriber = client.subscribe(subject.clone()).await?;
    info!("Starting to consume packets from topic {}", &subject);

    let concurrency = config.analyzer_concurrency;
    let packet_processor = Arc::new(PacketProcessor::new(cache, config, db, batch_tx));
    let semaphore = Arc::new(Semaphore::new(concurrency));

    while let Some(message) = subscriber.next().await {
        // Get payload and clone it so it can live in a separate task
        let payload = message.payload.to_vec();

        // Unpack the versioned batch envelope (see RAW_PACKET_WIRE_VERSION
        // above). A decode failure drops ONE message — i.e. up to K packets,
        // the agent's batch — and is logged, never fatal.
        let (version, packets): (u32, Vec<RawPacket>) = match bincode::decode_from_slice(&payload, bincode::config::standard()) {
            Ok((u, _)) => u,
            Err(e) => {
                error!("Error decoding packet batch ({} bytes): {}", payload.len(), e);
                continue;
            }
        };
        if version != RAW_PACKET_WIRE_VERSION {
            error!("Unknown raw-packet wire version {} (expected {}) — is the analyzer older than the agent?", version, RAW_PACKET_WIRE_VERSION);
            continue;
        }
        trace!("Decoded batch of {} packets ({} bytes)", packets.len(), payload.len());

        for packet in packets {
            // Processing packet in a separate task with cloned data
            let processor_clone = packet_processor.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            task::spawn(async move {
                trace!("Processing packet of size {} bytes: {:?}", packet.2.len(), packet.2);
                // packet.0 = usec, packet.1 = sec
                let parsed_packet = parse_packet((packet.0 as u64, packet.1 as u64), packet.2);

                if let Some(packet) = parsed_packet {
                    processor_clone.process_packet(packet).await.unwrap_or_else(|e| {
                        error!("Error processing packet: {}", e);
                    });
                }
                drop(permit);
            });
        }
    }

    Ok(())
}
