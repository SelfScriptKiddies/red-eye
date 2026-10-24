use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

use crate::cache::Cache;
use crate::db::DB;
use crate::ws_aggregator::SharedWsAggregator;

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
    /// Dedup key + JSONB payload, resolved to a header_set id per flush
    /// batch (see db::insert_http_messages_batch). No DB call on the hot
    /// path, no in-memory id cache to go stale against cleanup GC.
    pub header_hash: Option<i64>,
    pub header_json: Option<String>,
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

/// Sender side of the sharded flusher pool. One bounded channel per flusher
/// task; a packet is routed by its stream id so all packets of a stream are
/// buffered and flushed by ONE flusher (per-stream order preserved, and a
/// stream's stats/last_packet_time updates never race themselves).
///
/// Shard rule: `stream_id % N`. Deterministic, and because stream ids are
/// sequential, concurrent connections round-robin across flushers.
///
/// Ordering note: packet PROCESSING tasks already run concurrently
/// (semaphore in message_broker), so packets of one stream may reach this
/// sender slightly out of capture order — same as before sharding; the
/// shard only guarantees they stay on one flusher.
pub struct ShardedBatchSender {
    senders: Vec<mpsc::Sender<ProcessedPacket>>,
}

/// Packets dropped because every flusher queue a packet maps to was full.
/// Losing a packet is acceptable degradation; blocking the NATS consumer
/// (or a processing task holding one of the concurrency permits) is not.
static FLUSHER_QUEUE_DROPS: AtomicU64 = AtomicU64::new(0);

pub fn flusher_queue_drops() -> u64 {
    FLUSHER_QUEUE_DROPS.load(Ordering::Relaxed)
}

impl ShardedBatchSender {
    pub fn shard_count(&self) -> usize {
        self.senders.len()
    }

    /// Which flusher queue a stream maps to (exposed for tests and for the
    /// sharding determinism guarantee).
    pub fn shard_for(&self, stream_id: usize) -> usize {
        stream_id % self.senders.len()
    }

    /// Route one processed packet to its flusher queue. Never blocks: a
    /// full queue drops the packet and counts it (warn every 1000th drop),
    /// keeping the NATS consumer and the processing permits flowing.
    pub fn send(&self, packet: ProcessedPacket) -> Result<(), String> {
        let shard = self.shard_for(packet.stream_id);
        match self.senders[shard].try_send(packet) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped = FLUSHER_QUEUE_DROPS.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped.is_multiple_of(1000) {
                    warn!(
                        "Flusher queue {} full — {} packets dropped so far",
                        shard, dropped
                    );
                }
                Ok(()) // counted degradation, not an error
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err("flusher queue closed (flusher task died)".to_string())
            }
        }
    }
}

/// Create N flusher tasks (N >= 1), each with its own bounded queue of
/// `queue_capacity` packets, sharing one DB pool, one cache and one WS
/// activity aggregator. Flush cadence and batch size are per flusher and
/// unchanged from the single-flusher design.
///
/// The `ws_aggregator` is the analyzer-side WebSocket activity aggregation:
/// flushers record per-packet events into it and
/// `ws_aggregator::spawn_ws_aggregate_publisher` publishes one summary
/// frame per second on the WS topic (replaces the old per-packet NATS
/// publishes — see ws_aggregator.rs for the wire format).
pub fn spawn_flushers(
    flushers: usize,
    queue_capacity: usize,
    db: DB,
    opts: FlusherOptions,
    cache: Arc<Cache>,
    ws_aggregator: SharedWsAggregator,
) -> ShardedBatchSender {
    let n = flushers.max(1);
    let (sender, rxs) = sharded_channels(n, queue_capacity);
    for (i, rx) in rxs.into_iter().enumerate() {
        let db = db.clone(); // PgPool is internally Arc — one shared pool
        let cache = cache.clone();
        let ws_aggregator = ws_aggregator.clone();
        tokio::spawn(async move {
            run_batch_flusher(i, n, rx, db, opts, cache, ws_aggregator).await;
        });
    }
    info!(
        "Spawned {} batch flusher(s) (batch_size={}, flush_interval={}ms, queue={} packets each)",
        n, opts.batch_size, opts.flush_interval_ms, queue_capacity
    );
    sender
}

/// Flush cadence knobs shared by every flusher (values from batch config).
#[derive(Clone, Copy)]
pub struct FlusherOptions {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

/// Create the flusher queues without spawning the consumers (shared by
/// spawn_flushers and the sharding tests).
fn sharded_channels(
    n: usize,
    queue_capacity: usize,
) -> (ShardedBatchSender, Vec<mpsc::Receiver<ProcessedPacket>>) {
    let mut senders = Vec::with_capacity(n);
    let mut receivers = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::channel::<ProcessedPacket>(queue_capacity);
        senders.push(tx);
        receivers.push(rx);
    }
    (ShardedBatchSender { senders }, receivers)
}

/// Background task that collects processed packets and flushes them in batches.
///
/// Flush triggers:
/// - Buffer reaches `batch_size` items, OR
/// - `flush_interval_ms` milliseconds elapsed since the first buffered item.
///
/// Also drains the TCP lifecycle dirty set (`cache.drain_closed_streams`)
/// each flush, so FIN/RST closes and SYN retirements become one batched
/// `UPDATE stream SET active = false` instead of per-packet UPDATEs.
///
/// `index`/`total` identify the flusher in logs (N of these run in
/// parallel, sharded by stream id — see [`spawn_flushers`]).
pub async fn run_batch_flusher(
    index: usize,
    total: usize,
    mut rx: mpsc::Receiver<ProcessedPacket>,
    db: DB,
    opts: FlusherOptions,
    cache: Arc<Cache>,
    ws_aggregator: SharedWsAggregator,
) {
    info!(
        "Batch flusher {}/{} started: batch_size={}, flush_interval={}ms",
        index + 1,
        total,
        opts.batch_size,
        opts.flush_interval_ms
    );

    let flush_duration = Duration::from_millis(opts.flush_interval_ms);
    let mut buffer: Vec<ProcessedPacket> = Vec::with_capacity(opts.batch_size);
    let mut deadline: Option<Instant> = None;

    loop {
        let recv_result = if buffer.is_empty() {
            // No buffered items — wait for the next packet, but wake up once
            // per flush interval anyway: FIN/RST-only packets never enter the
            // buffer (control-only packets carry no data row), so without
            // this wakeup deferred stream closes would sit in the dirty set
            // until unrelated data traffic arrives.
            tokio::select! {
                biased;
                result = rx.recv() => {
                    match result {
                        Some(pkt) => Some(pkt),
                        None => {
                            // Channel closed — flush remaining and exit.
                            if !buffer.is_empty() {
                                flush_batch(&db, &mut buffer, &ws_aggregator).await;
                            }
                            flush_lifecycle(&db, &cache).await;
                            info!("Batch flusher {}/{}: channel closed, shutting down", index + 1, total);
                            return;
                        }
                    }
                }
                _ = tokio::time::sleep(flush_duration) => {
                    flush_lifecycle(&db, &cache).await;
                    None
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
                            flush_batch(&db, &mut buffer, &ws_aggregator).await;
                            flush_lifecycle(&db, &cache).await;
                            info!("Batch flusher {}/{}: channel closed, shutting down", index + 1, total);
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
        let should_flush = buffer.len() >= opts.batch_size
            || (!buffer.is_empty() && deadline.map(|d| Instant::now() >= d).unwrap_or(false));

        if should_flush {
            flush_batch(&db, &mut buffer, &ws_aggregator).await;
            flush_lifecycle(&db, &cache).await;
            deadline = None;
        }
    }
}

/// Flush deferred TCP lifecycle state (FIN/RST closes, SYN retirements):
/// one idempotent set-based UPDATE for everything in the dirty set.
/// Called on every data-batch flush AND on the empty-buffer timeout, so
/// streams close within ~flush_interval even with no data traffic.
/// On failure the ids are re-queued for the next flush — a stream staying
/// active a bit longer is the pre-lifecycle behavior, not a failure.
async fn flush_lifecycle(db: &DB, cache: &Cache) {
    let closed = cache.drain_closed_streams();
    if closed.is_empty() {
        return;
    }
    if let Err(e) = db.set_streams_inactive_batch(&closed).await {
        error!(
            "Deferred stream close update failed ({} streams, retried next flush): {:?}",
            closed.len(),
            e
        );
        cache.requeue_closed_streams(&closed);
    }
}

/// Flush a batch of processed packets to the database and record WS activity.
async fn flush_batch(
    db: &DB,
    buffer: &mut Vec<ProcessedPacket>,
    ws_aggregator: &SharedWsAggregator,
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
    let http_rows: Vec<(i64, &HttpMessageData)> = buffer
        .iter()
        .enumerate()
        .filter_map(|(idx, pkt)| {
            pkt.http_message
                .as_ref()
                .map(|http| (packet_ids[idx], http))
        })
        .collect();
    if !http_rows.is_empty() {
        match db.insert_http_messages_batch(&http_rows).await {
            Ok(msg_ids) if msg_ids.len() == http_rows.len() => {
                // gRPC inserts keyed by the returned http_message ids.
                let grpc_rows: Vec<(i64, &GrpcMessageData)> = http_rows
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (_, http))| http.grpc.as_ref().map(|grpc| (msg_ids[i], grpc)))
                    .collect();
                if let Err(e) = db.insert_grpc_messages_batch(&grpc_rows).await {
                    error!("Batch gRPC insert failed: {:?}", e);
                }

                // Dedupe l7-protocol UPDATEs to the highest protocol per stream.
                let rank = |p: &str| match p {
                    "grpc" => 2,
                    "http1" => 1,
                    _ => 0,
                };
                let mut l7: HashMap<usize, &str> = HashMap::new();
                for (_, http) in &http_rows {
                    l7.entry(http.stream_id)
                        .and_modify(|cur| {
                            if rank(&http.l7_protocol) > rank(cur) {
                                *cur = http.l7_protocol.as_str();
                            }
                        })
                        .or_insert(http.l7_protocol.as_str());
                }
                let l7_rows: Vec<(usize, String)> =
                    l7.into_iter().map(|(s, p)| (s, p.to_string())).collect();
                if let Err(e) = db.update_stream_l7_protocol_batch(&l7_rows).await {
                    error!("Batch l7_protocol update failed: {:?}", e);
                }
            }
            Ok(msg_ids) => {
                error!(
                    "HTTP batch returned {} ids for {} messages — skipping grpc/l7",
                    msg_ids.len(),
                    http_rows.len()
                );
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
        if ts > entry.0 {
            entry.0 = ts;
        }
        entry.1 += 1;
    }
    let stats_rows: Vec<(usize, i64, i64)> = stream_stats
        .iter()
        .map(|(s, (ts, c))| (*s, *ts, *c))
        .collect();
    if let Err(e) = db.update_streams_stats_batch(&stats_rows).await {
        error!("Failed to update stream stats: {:?}", e);
    }

    // 5. Maintain stream_tag INCREMENTALLY: upsert the distinct
    //    (stream_id -> tag_name) pairs seen in this batch, so the streams
    //    list never recomputes the tag x packets join (replaces the old
    //    materialized view + its pg_notify refresh machinery).
    let filter_names = crate::filters::filter_names_snapshot();
    let stream_tag_pairs = build_stream_tag_pairs(buffer, &filter_names);
    if !stream_tag_pairs.is_empty() {
        if let Err(e) = db.insert_stream_tags_batch(&stream_tag_pairs).await {
            error!("Batch stream_tag upsert failed: {:?}", e);
        }
    }

    // 6. Record WebSocket activity into the shared aggregator. The
    // publisher task (ws_aggregator.rs) turns it into ONE NATS message per
    // second instead of the per-packet publishes this loop used to do.
    if let Ok(mut agg) = ws_aggregator.lock() {
        for pkt in buffer.iter() {
            if let Some(ref evt) = pkt.nats_event {
                agg.record(evt.stream_id, evt.timestamp_seconds);
            }
        }
    }

    debug!(
        "Flushed {} packets ({} triggers, {} streams updated)",
        count,
        trigger_rows.len(),
        stats_rows.len()
    );
    buffer.clear();
}

/// Build tag insert tuples (packet_id, filter_id, start, end, packet_timestamp)
/// for a flushed batch. `packet_ids[i]` is the DB id of `buffer[i]`, so each
/// trigger row pairs with ITS OWN packet's capture timestamp — this is what
/// keeps `JOIN packets p ON p.timestamp = t.timestamp` correct in the backend.
fn build_trigger_rows(
    packet_ids: &[i64],
    buffer: &[ProcessedPacket],
) -> Vec<(i64, usize, usize, usize, i64)> {
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

/// Build distinct (stream_id, tag_name) pairs for the stream_tag upsert.
/// `names` maps filter_id -> tag_name (the same tag_name.name strings the
/// old materialized view produced, so UI semantics don't change). Triggers
/// whose filter is no longer registered (removed mid-game) are skipped —
/// losing one tag pair is acceptable degradation.
fn build_stream_tag_pairs(
    buffer: &[ProcessedPacket],
    names: &HashMap<usize, String>,
) -> Vec<(usize, String)> {
    let mut seen: HashSet<(usize, &str)> = HashSet::new();
    let mut pairs: Vec<(usize, String)> = Vec::new();
    for pkt in buffer {
        for &(filter_id, _, _) in &pkt.triggers {
            if let Some(name) = names.get(&filter_id) {
                if seen.insert((pkt.stream_id, name.as_str())) {
                    pairs.push((pkt.stream_id, name.clone()));
                }
            }
        }
    }
    pairs
}

/// Insert the packet batch, retrying once after a short backoff on error or
/// id-count mismatch. Returns the packet ids, or None if the batch should be
/// dropped (logged at warn with the count lost).
async fn insert_packets_with_retry(db: &DB, buffer: &[ProcessedPacket]) -> Option<Vec<i64>> {
    match db.insert_packets_batch(buffer).await {
        Ok(ids) if ids.len() == buffer.len() => return Some(ids),
        Ok(ids) => error!(
            "Batch insert returned {} ids for {} packets — retrying",
            ids.len(),
            buffer.len()
        ),
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
            warn!(
                "Batch packet insert failed after retry: {:?} — dropping {} packets",
                e,
                buffer.len()
            );
            None
        }
    }
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

    #[test]
    fn stream_tag_pairs_dedup_across_packets_and_triggers() {
        // Same (stream, filter) firing on many packets and twice on one
        // packet must yield ONE pair; different streams and filters each
        // get their own pair.
        let buffer = vec![
            packet(1_000, vec![(10, 0, 5)]),
            packet(1_001, vec![(10, 1, 4), (10, 2, 8)]), // same filter again
            packet(1_002, vec![(11, 0, 3)]),             // different filter, same stream
        ];
        // packet() helper pins stream_id=1; patch the third packet to
        // another stream to cover the cross-stream case.
        let mut other_stream = packet(1_003, vec![(10, 0, 2)]);
        other_stream.stream_id = 2;

        let mut buffer = buffer;
        buffer.push(other_stream);

        let mut names = HashMap::new();
        names.insert(10usize, "flag_leak".to_string());
        names.insert(11usize, "exploit".to_string());

        let mut pairs = build_stream_tag_pairs(&buffer, &names);
        pairs.sort();

        assert_eq!(
            pairs,
            vec![
                (1, "exploit".to_string()),
                (1, "flag_leak".to_string()),
                (2, "flag_leak".to_string()),
            ]
        );
    }

    #[test]
    fn stream_tag_pairs_skip_unknown_filter_ids() {
        // Filter removed mid-game: no name mapping -> skip, no panic, no
        // placeholder garbage in stream_tag.
        let buffer = vec![packet(1_000, vec![(999, 0, 5), (10, 1, 2)])];
        let mut names = HashMap::new();
        names.insert(10usize, "flag_leak".to_string());

        let pairs = build_stream_tag_pairs(&buffer, &names);
        assert_eq!(pairs, vec![(1, "flag_leak".to_string())]);
    }

    #[test]
    fn stream_tag_pairs_empty_when_no_triggers() {
        let buffer = vec![packet(1_000, vec![])];
        let names = HashMap::new();
        assert!(build_stream_tag_pairs(&buffer, &names).is_empty());
    }

    #[test]
    fn sharding_is_deterministic_and_covers_all_shards() {
        // Same stream id must always map to the same shard (per-stream
        // order guarantee), and sequential stream ids must spread over all
        // shards (even load — connections are created sequentially).
        let (sender, _rxs) = sharded_channels(4, 8);
        assert_eq!(sender.shard_count(), 4);
        for stream_id in 0..100usize {
            assert_eq!(sender.shard_for(stream_id), stream_id % 4);
        }
        // Every shard receives traffic for sequential stream ids.
        let covered: HashSet<usize> = (0..40usize).map(|s| sender.shard_for(s)).collect();
        assert_eq!(
            covered,
            [0, 1, 2, 3].into_iter().collect(),
            "all shards used"
        );
    }

    #[tokio::test]
    async fn packets_of_one_stream_land_in_one_queue_in_send_order() {
        let (sender, mut rxs) = sharded_channels(3, 16);
        let mut other_stream = packet(2_000, vec![]);
        other_stream.stream_id = 2; // shard 2 — a different queue than 7
        let mut s1 = packet(1_000, vec![]);
        s1.stream_id = 7; // shard 1
        let mut s2 = packet(1_001, vec![]);
        s2.stream_id = 7;

        sender.send(s1).unwrap();
        sender.send(other_stream).unwrap();
        sender.send(s2).unwrap();

        let shard = sender.shard_for(7);
        // Drain all queues; stream 7's two packets must both be in its shard
        // queue, in send order (and only its own packets).
        let mut got7 = Vec::new();
        for (i, rx) in rxs.iter_mut().enumerate() {
            while let Ok(pkt) = rx.try_recv() {
                if i == shard {
                    got7.push(pkt.timestamp_seconds);
                }
            }
        }
        assert_eq!(got7, vec![1_000, 1_001]);
    }

    #[tokio::test]
    async fn full_queue_drops_and_counts_instead_of_blocking() {
        // Capacity 1, no consumer: further sends must return Ok (counted
        // degradation) and the drop counter must increase — never block.
        let (sender, _rxs) = sharded_channels(1, 1);
        let before = flusher_queue_drops();
        for _ in 0..5 {
            sender.send(packet(1_000, vec![])).unwrap();
        }
        assert!(
            flusher_queue_drops() >= before + 4,
            "4 of 5 packets dropped and counted"
        );
    }
}
