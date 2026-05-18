use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use log::{info, error, debug};
use async_nats::Client;

use crate::db::DB;

/// A fully-processed packet ready for batch insertion.
pub struct ProcessedPacket {
    pub timestamp_seconds: u64,
    pub timestamp_microseconds: u64,
    pub direction: String,
    pub stream_id: usize,
    pub payload: Vec<u8>,
    pub triggers: Vec<(usize, usize, usize)>, // (filter_id, start_index, end_index)
    pub http_message: Option<HttpMessageData>,
    pub grpc_message: Option<GrpcMessageData>,
    pub nats_event: Option<NatsEventData>,
}

pub struct HttpMessageData {
    pub stream_id: usize,
    pub is_request: bool,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status_code: Option<u16>,
    pub http_version: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<i32>,
    pub body_offset: i32,
    pub body_length: i32,
    pub header_set_id: Option<i32>,
    pub timestamp: i64,
    pub l7_protocol: String,
    pub grpc: Option<GrpcMessageData>,
}

pub struct GrpcMessageData {
    pub stream_id: usize,
    pub service_name: Option<String>,
    pub method_name: Option<String>,
    pub is_request: bool,
    pub decoded_json: String,
    pub timestamp: i64,
}

pub struct NatsEventData {
    pub stream_id: usize,
    pub timestamp_seconds: u64,
    pub timestamp_microseconds: u64,
    pub direction: String,
}

pub fn create_batch_channel(buffer_size: usize) -> (mpsc::Sender<ProcessedPacket>, mpsc::Receiver<ProcessedPacket>) {
    mpsc::channel(buffer_size)
}

/// Background task that collects processed packets and flushes them in batches.
///
/// Flush triggers:
/// - Buffer reaches `batch_size` items, OR
/// - `flush_interval_ms` milliseconds elapsed since the first buffered item.
pub async fn run_batch_flusher(
    mut rx: mpsc::Receiver<ProcessedPacket>,
    db: DB,
    batch_size: usize,
    flush_interval_ms: u64,
    nats_client: Option<Client>,
    nats_ws_topic: String,
) {
    info!("Batch flusher started: batch_size={}, flush_interval={}ms", batch_size, flush_interval_ms);

    let flush_duration = Duration::from_millis(flush_interval_ms);
    let mut buffer: Vec<ProcessedPacket> = Vec::with_capacity(batch_size);
    let mut deadline: Option<Instant> = None;

    loop {
        let recv_result = if buffer.is_empty() {
            // No buffered items — just wait for the next packet (no timeout).
            match rx.recv().await {
                Some(pkt) => Some(pkt),
                None => {
                    // Channel closed — flush remaining and exit.
                    if !buffer.is_empty() {
                        flush_batch(&db, &mut buffer, &nats_client, &nats_ws_topic).await;
                    }
                    info!("Batch flusher: channel closed, shutting down");
                    return;
                }
            }
        } else {
            // We have buffered items — wait with a timeout.
            let remaining = deadline
                .map(|d| d.saturating_duration_since(Instant::now()))
                .unwrap_or(flush_duration);

            tokio::select! {
                biased;
                result = rx.recv() => {
                    match result {
                        Some(pkt) => Some(pkt),
                        None => {
                            flush_batch(&db, &mut buffer, &nats_client, &nats_ws_topic).await;
                            info!("Batch flusher: channel closed, shutting down");
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    // Timeout — flush what we have.
                    None
                }
            }
        };

        if let Some(pkt) = recv_result {
            if buffer.is_empty() {
                deadline = Some(Instant::now() + flush_duration);
            }
            buffer.push(pkt);
        }

        // Flush if buffer is full OR timeout expired.
        let should_flush = buffer.len() >= batch_size
            || (!buffer.is_empty() && deadline.map(|d| Instant::now() >= d).unwrap_or(false));

        if should_flush {
            flush_batch(&db, &mut buffer, &nats_client, &nats_ws_topic).await;
            deadline = None;
        }
    }
}

/// Flush a batch of processed packets to the database and send NATS events.
async fn flush_batch(
    db: &DB,
    buffer: &mut Vec<ProcessedPacket>,
    nats_client: &Option<Client>,
    nats_ws_topic: &str,
) {
    if buffer.is_empty() {
        return;
    }

    let count = buffer.len();
    debug!("Flushing batch of {} packets", count);

    // 1. Batch INSERT packets -> RETURNING id
    let packet_ids = match db.insert_packets_batch(buffer).await {
        Ok(ids) => ids,
        Err(e) => {
            error!("Batch packet insert failed: {:?}", e);
            buffer.clear();
            return;
        }
    };

    if packet_ids.len() != buffer.len() {
        error!("Batch insert returned {} ids for {} packets — mismatch", packet_ids.len(), buffer.len());
        buffer.clear();
        return;
    }

    // 2. Batch INSERT triggers
    let mut trigger_rows: Vec<(i32, usize, usize, usize)> = Vec::new();
    for (idx, pkt) in buffer.iter().enumerate() {
        let packet_id = packet_ids[idx];
        for &(filter_id, start, end) in &pkt.triggers {
            trigger_rows.push((packet_id, filter_id, start, end));
        }
    }
    if !trigger_rows.is_empty() {
        if let Err(e) = db.insert_triggers_batch(&trigger_rows).await {
            error!("Batch trigger insert failed: {:?}", e);
        }
    }

    // 3. Insert HTTP messages one by one (they need RETURNING id for gRPC)
    for (idx, pkt) in buffer.iter().enumerate() {
        let packet_id = packet_ids[idx];
        if let Some(ref http) = pkt.http_message {
            match db.insert_http_message(
                http.stream_id,
                packet_id,
                http.is_request,
                http.method.as_deref(),
                http.path.as_deref(),
                http.status_code,
                http.http_version.as_deref(),
                http.content_type.as_deref(),
                http.content_length,
                http.body_offset,
                http.body_length,
                http.header_set_id,
                http.timestamp,
            ).await {
                Ok(msg_id) => {
                    // Update l7_protocol
                    let _ = db.update_stream_l7_protocol(http.stream_id, &http.l7_protocol).await;

                    // Insert gRPC if present
                    if let Some(ref grpc) = http.grpc {
                        let _ = db.insert_grpc_message(
                            grpc.stream_id,
                            msg_id,
                            grpc.service_name.as_deref(),
                            grpc.method_name.as_deref(),
                            grpc.is_request,
                            &grpc.decoded_json,
                            grpc.timestamp,
                        ).await;
                    }
                }
                Err(e) => {
                    error!("Failed to insert HTTP message: {:?}", e);
                }
            }
        }
    }

    // 4. Bulk UPDATE stream SET last_packet_time (grouped by stream_id)
    let mut stream_max_ts: HashMap<usize, i64> = HashMap::new();
    for pkt in buffer.iter() {
        let ts = pkt.timestamp_seconds as i64;
        stream_max_ts
            .entry(pkt.stream_id)
            .and_modify(|existing| {
                if ts > *existing {
                    *existing = ts;
                }
            })
            .or_insert(ts);
    }
    for (stream_id, ts) in &stream_max_ts {
        if let Err(e) = db.update_stream_last_packet_time(*stream_id, *ts).await {
            error!("Failed to update last_packet_time for stream {}: {:?}", stream_id, e);
        }
    }

    // 5. pg_notify('refresh_streams')
    if let Err(e) = db.notify_refresh_streams().await {
        error!("pg_notify failed: {:?}", e);
    }

    // 6. Send NATS events for WebSocket
    if let Some(ref client) = nats_client {
        for (idx, pkt) in buffer.iter().enumerate() {
            if let Some(ref evt) = pkt.nats_event {
                let event = serde_json::json!({
                    "packet_id": packet_ids[idx],
                    "stream_id": evt.stream_id,
                    "timestamp_seconds": evt.timestamp_seconds,
                    "timestamp_microseconds": evt.timestamp_microseconds,
                    "direction": evt.direction,
                });
                if let Ok(payload) = serde_json::to_vec(&event) {
                    if let Err(e) = client.publish(nats_ws_topic.to_string(), payload.into()).await {
                        error!("Failed to send NATS event: {}", e);
                    }
                }
            }
        }
    }

    debug!("Flushed {} packets ({} triggers, {} streams updated)",
        count, trigger_rows.len(), stream_max_ts.len());
    buffer.clear();
}
