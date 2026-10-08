use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use log::{info, warn, error, debug};
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

    // 1. Batch INSERT packets -> RETURNING id.
    // Losing 1 packet is acceptable, losing the whole batch is not: retry once
    // after a short backoff before dropping (no DLQ / persistent queue).
    let packet_ids = match insert_packets_with_retry(db, buffer).await {
        Some(ids) => ids,
        None => {
            buffer.clear();
            return;
        }
    };

    // 2. Batch INSERT triggers
    //    Each tag row must carry its packet's CAPTURE timestamp (not flush
    //    time): the backend JOINs packets p ON p.timestamp = t.timestamp, so
    //    DB-clock timestamps detach tags from packets (lost flags).
    let trigger_rows = build_trigger_rows(&packet_ids, buffer);
    if !trigger_rows.is_empty() {
        if let Err(e) = db.insert_triggers_batch(&trigger_rows).await {
            error!("Batch trigger insert failed: {:?}", e);
        }
    }

    // 3. Batch INSERT HTTP messages (multi-row, RETURNING id), then batch the
    //    dependent gRPC inserts and l7-protocol UPDATEs.
    let http_rows: Vec<(i32, &HttpMessageData)> = buffer.iter().enumerate()
        .filter_map(|(idx, pkt)| pkt.http_message.as_ref().map(|http| (packet_ids[idx], http)))
        .collect();
    if !http_rows.is_empty() {
        match db.insert_http_messages_batch(&http_rows).await {
            Ok(msg_ids) if msg_ids.len() == http_rows.len() => {
                // gRPC inserts keyed by the returned http_message ids.
                let grpc_rows: Vec<(i32, &GrpcMessageData)> = http_rows.iter().enumerate()
                    .filter_map(|(i, (_, http))| http.grpc.as_ref().map(|grpc| (msg_ids[i], grpc)))
                    .collect();
                if let Err(e) = db.insert_grpc_messages_batch(&grpc_rows).await {
                    error!("Batch gRPC insert failed: {:?}", e);
                }

                // Dedupe l7-protocol UPDATEs to the highest protocol per stream.
                let rank = |p: &str| match p { "grpc" => 2, "http1" => 1, _ => 0 };
                let mut l7: HashMap<usize, &str> = HashMap::new();
                for (_, http) in &http_rows {
                    l7.entry(http.stream_id)
                        .and_modify(|cur| { if rank(&http.l7_protocol) > rank(cur) { *cur = http.l7_protocol.as_str(); } })
                        .or_insert(http.l7_protocol.as_str());
                }
                let l7_rows: Vec<(usize, String)> = l7.into_iter().map(|(s, p)| (s, p.to_string())).collect();
                if let Err(e) = db.update_stream_l7_protocol_batch(&l7_rows).await {
                    error!("Batch l7_protocol update failed: {:?}", e);
                }
            }
            Ok(msg_ids) => {
                error!("HTTP batch returned {} ids for {} messages — skipping grpc/l7", msg_ids.len(), http_rows.len());
            }
            Err(e) => {
                error!("Batch HTTP message insert failed: {:?}", e);
            }
        }
    }

    // 4. Single set-based UPDATE stream: last_packet_time (max) + packet_count (+= delta).
    let mut stream_stats: HashMap<usize, (i64, i64)> = HashMap::new(); // stream_id -> (max_ts, count)
    for pkt in buffer.iter() {
        let ts = pkt.timestamp_seconds as i64;
        let entry = stream_stats.entry(pkt.stream_id).or_insert((ts, 0));
        if ts > entry.0 { entry.0 = ts; }
        entry.1 += 1;
    }
    let stats_rows: Vec<(usize, i64, i64)> = stream_stats.iter().map(|(s, (ts, c))| (*s, *ts, *c)).collect();
    if let Err(e) = db.update_streams_stats_batch(&stats_rows).await {
        error!("Failed to update stream stats: {:?}", e);
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
        count, trigger_rows.len(), stats_rows.len());
    buffer.clear();
}

/// Build tag insert tuples (packet_id, filter_id, start, end, packet_timestamp)
/// for a flushed batch. `packet_ids[i]` is the DB id of `buffer[i]`, so each
/// trigger row pairs with ITS OWN packet's capture timestamp — this is what
/// keeps `JOIN packets p ON p.timestamp = t.timestamp` correct in the backend.
fn build_trigger_rows(packet_ids: &[i32], buffer: &[ProcessedPacket]) -> Vec<(i32, usize, usize, usize, i64)> {
    let mut rows = Vec::new();
    for (idx, pkt) in buffer.iter().enumerate() {
        let packet_id = packet_ids[idx];
        let timestamp = pkt.timestamp_seconds as i64;
        for &(filter_id, start, end) in &pkt.triggers {
            rows.push((packet_id, filter_id, start, end, timestamp));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(timestamp_seconds: u64, triggers: Vec<(usize, usize, usize)>) -> ProcessedPacket {
        ProcessedPacket {
            timestamp_seconds,
            timestamp_microseconds: 0,
            direction: "inbound".to_string(),
            stream_id: 1,
            payload: vec![],
            triggers,
            http_message: None,
            grpc_message: None,
            nats_event: None,
        }
    }

    #[test]
    fn trigger_rows_pair_each_packet_with_its_own_timestamp() {
        // Two packets with different capture seconds and multiple matches
        // each; ids are distinct from buffer positions to catch index mixups.
        let buffer = vec![
            packet(1_000, vec![(10, 0, 5), (11, 7, 9)]),
            packet(2_000, vec![(10, 1, 4), (12, 2, 8), (12, 20, 25)]),
        ];
        let packet_ids = [101, 202];

        let rows = build_trigger_rows(&packet_ids, &buffer);

        assert_eq!(rows.len(), 5);
        // Packet 1 (id=101, ts=1000): both triggers carry ts 1000
        assert_eq!(rows[0], (101, 10, 0, 5, 1_000));
        assert_eq!(rows[1], (101, 11, 7, 9, 1_000));
        // Packet 2 (id=202, ts=2000): all three triggers carry ts 2000
        assert_eq!(rows[2], (202, 10, 1, 4, 2_000));
        assert_eq!(rows[3], (202, 12, 2, 8, 2_000));
        assert_eq!(rows[4], (202, 12, 20, 25, 2_000));
        // Invariant: every row's timestamp equals the capture time of the
        // packet its id belongs to.
        for (pid, _, _, _, ts) in &rows {
            let expected = if *pid == 101 { 1_000 } else { 2_000 };
            assert_eq!(*ts, expected, "packet {} got wrong timestamp", pid);
        }
    }

    #[test]
    fn trigger_rows_empty_when_no_matches() {
        let buffer = vec![packet(1_000, vec![]), packet(1_001, vec![])];
        assert!(build_trigger_rows(&[1, 2], &buffer).is_empty());
    }
}

/// Insert the packet batch, retrying once after a short backoff on error or
/// id-count mismatch. Returns the packet ids, or None if the batch should be
/// dropped (logged at warn with the count lost).
async fn insert_packets_with_retry(db: &DB, buffer: &[ProcessedPacket]) -> Option<Vec<i32>> {
    match db.insert_packets_batch(buffer).await {
        Ok(ids) if ids.len() == buffer.len() => return Some(ids),
        Ok(ids) => error!("Batch insert returned {} ids for {} packets — retrying", ids.len(), buffer.len()),
        Err(e) => error!("Batch packet insert failed: {:?} — retrying", e),
    }

    tokio::time::sleep(Duration::from_millis(75)).await;

    match db.insert_packets_batch(buffer).await {
        Ok(ids) if ids.len() == buffer.len() => Some(ids),
        Ok(ids) => {
            warn!("Batch insert still mismatched after retry ({} ids for {} packets) — dropping {} packets",
                ids.len(), buffer.len(), buffer.len());
            None
        }
        Err(e) => {
            warn!("Batch packet insert failed after retry: {:?} — dropping {} packets", e, buffer.len());
            None
        }
    }
}
