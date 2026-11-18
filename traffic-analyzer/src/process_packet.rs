use crate::batch_flusher::{
    GrpcMessageData, HttpMessageData, NatsEventData, ProcessedPacket, ShardedBatchSender,
};
use crate::cache::streams::StreamsCache;
use crate::cache::Cache;
use crate::configuration::updater::dto::TransportProto;
use crate::configuration::ApplicationConfig;
use crate::db::DB;
use crate::ethernet::{self, PacketInfo, Protocol};
use crate::grpc_parser;
use crate::http_parser;
use crate::metrics;
use crate::retention::RetentionTracker;
use log::debug;
use std::io::Error;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Inbound,
    Outbound,
}

/// Per-packet resolution inputs: the capturing agent and its effective
/// machine IP. A wildcard machine IP (0.0.0.0 / ::) matches services by
/// protocol+port only; a concrete value matches IP+port.
pub struct PacketContext {
    pub agent_id: String,
    pub machine_ip: IpAddr,
}

/// Outcome of service matching.
#[derive(Debug, PartialEq)]
enum ServiceMatch {
    Found(usize, Direction),
    /// Both endpoints match the same service under wildcard mode.
    Ambiguous,
    /// No service covers this packet: drop immediately, never buffer.
    Unknown,
}

/// Pure service resolution: transport protocol + port identify the
/// service; a concrete machine IP also requires the IP to match the local
/// endpoint, a wildcard one matches protocol+port on either side and
/// reports ambiguity when both sides match. Extracted for unit tests.
fn resolve_service_match<'a, I>(
    services: I,
    packet: &PacketInfo,
    machine_ip: IpAddr,
) -> ServiceMatch
where
    I: Iterator<Item = (usize, &'a crate::configuration::updater::dto::Service)>,
{
    for (id, service) in services {
        let transport = match service.protocol {
            TransportProto::Tcp => Protocol::TCP,
            TransportProto::Udp => Protocol::UDP,
        };
        if transport != packet.protocol {
            continue;
        }

        if !machine_ip.is_unspecified() {
            let src_is_local = machine_ip == packet.src_ip && service.port == packet.src_port;
            let dst_is_local = machine_ip == packet.dst_ip && service.port == packet.dst_port;
            if src_is_local {
                return ServiceMatch::Found(id, Direction::Outbound);
            }
            if dst_is_local {
                return ServiceMatch::Found(id, Direction::Inbound);
            }
        } else {
            let src_matches = service.port == packet.src_port;
            let dst_matches = service.port == packet.dst_port;
            match (src_matches, dst_matches) {
                (true, true) => return ServiceMatch::Ambiguous,
                (true, false) => return ServiceMatch::Found(id, Direction::Outbound),
                (false, true) => return ServiceMatch::Found(id, Direction::Inbound),
                (false, false) => continue,
            }
        }
    }
    ServiceMatch::Unknown
}

#[derive(Debug)]
pub struct PacketEvent {
    pub packet_id: i64,
    pub stream_id: usize,
    pub timestamp_seconds: u64,
    pub timestamp_microseconds: u64,
    pub direction: String,
}

pub struct PacketProcessor {
    cache: Arc<Cache>,
    config: Arc<ApplicationConfig>,
    db: DB,
    batch_tx: Arc<ShardedBatchSender>,
    /// Starred-segment tracker for the segment rotation decision.
    retention: Arc<RetentionTracker>,
}

impl PacketProcessor {
    pub fn new(
        cache: Arc<Cache>,
        config: Arc<ApplicationConfig>,
        db: DB,
        batch_tx: Arc<ShardedBatchSender>,
        retention: Arc<RetentionTracker>,
    ) -> Self {
        Self {
            cache,
            config,
            db,
            batch_tx,
            retention,
        }
    }

    /// Service matching by transport protocol + port, direction by the
    /// agent's machine IP. Returns the first match in id order for the
    /// concrete case; the wildcard case additionally detects ambiguity.
    fn find_service_and_direction(
        &self,
        packet: &PacketInfo,
        context: &PacketContext,
    ) -> ServiceMatch {
        let services = self.cache.services.read().unwrap();
        resolve_service_match(
            services.iter().map(|(id, s)| (*id, s)),
            packet,
            context.machine_ip,
        )
    }

    /// Direction-normalized 4-tuple: src is always the local machine's side,
    /// so both directions of one connection map onto the same tuple.
    fn normalized_tuple(packet: &PacketInfo, direction: &Direction) -> (String, u16, String, u16) {
        match direction {
            Direction::Outbound => (
                packet.src_ip.to_string(),
                packet.src_port,
                packet.dst_ip.to_string(),
                packet.dst_port,
            ),
            Direction::Inbound => (
                packet.dst_ip.to_string(),
                packet.dst_port,
                packet.src_ip.to_string(),
                packet.src_port,
            ),
        }
    }

    fn tuple_key(packet: &PacketInfo, direction: &Direction) -> String {
        let (src_ip, src_port, dst_ip, dst_port) = Self::normalized_tuple(packet, direction);
        StreamsCache::format_key(&src_ip, src_port, &dst_ip, dst_port)
    }

    /// Pure decision for the TCP SYN lifecycle: does a plain SYN on this
    /// tuple open a NEW stream?
    ///
    /// A plain SYN retransmission (SYN seen, no data yet, stream already
    /// cached) must reuse the stream the first SYN created; without this
    /// every dropped-SYN retransmit would fork a duplicate stream.
    /// A plain SYN after the tuple carried data (or after a close) opens a
    /// new connection — previously the tuple kept merging traffic from all
    /// its connections into one forever-growing stream.
    fn should_start_new_stream(is_syn: bool, syn_pending: bool, has_cached_stream: bool) -> bool {
        is_syn && !(syn_pending && has_cached_stream)
    }

    /// Would the incoming packet push this starred segment past its byte
    /// limit? Unstarred streams never rotate (tracker miss).
    fn rotation_needed(&self, stream_id: usize, incoming_bytes: usize) -> bool {
        self.retention.rotation_needed(stream_id, incoming_bytes)
    }

    /// Finalize the over-limit starred segment and assign this packet to a
    /// fresh unstarred continuation (same tuple, service, protocol, agent).
    /// The triggering packet is never dropped or split; on rotation failure
    /// the old segment keeps accepting packets (bounded over-limit growth).
    async fn rotate_segment(
        &self,
        old_id: usize,
        packet: &PacketInfo,
        direction: &Direction,
        service_id: usize,
        agent_id: &str,
    ) -> Result<usize, Error> {
        let (src_ip, src_port, dst_ip, dst_port) = Self::normalized_tuple(packet, direction);
        let outcome = self
            .db
            .rotate_star_segment(
                old_id,
                &src_ip,
                src_port,
                &dst_ip,
                dst_port,
                &packet.protocol,
                service_id,
                packet.timestamp_seconds as i64,
                Some(agent_id),
            )
            .await;
        match outcome {
            Ok(Some(new_id)) => {
                self.cache
                    .streams
                    .insert(&src_ip, src_port, &dst_ip, dst_port, new_id);
                let total = metrics::FAVORITE_ROTATIONS.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::rate_limited_log("favorite-rotation", total, || {
                    format!(
                        "starred segment {} hit the byte limit; continuation segment {} opened",
                        old_id, new_id
                    )
                });
                Ok(new_id)
            }
            Ok(None) => {
                // Unstarred concurrently: the old segment stays the target.
                Ok(old_id)
            }
            Err(e) => {
                let total = metrics::FAVORITE_ROTATION_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::rate_limited_log("favorite-rotation-failed", total, || {
                    format!(
                        "rotation of starred segment {} failed, keeping it active: {:?}",
                        old_id, e
                    )
                });
                Ok(old_id)
            }
        }
    }

    async fn find_stream_id(
        &self,
        packet: &PacketInfo,
        direction: &Direction,
        service_id: usize,
        agent_id: &str,
    ) -> Result<usize, Error> {
        let (src_ip, src_port, dst_ip, dst_port) = Self::normalized_tuple(packet, direction);
        let key = StreamsCache::format_key(&src_ip, src_port, &dst_ip, dst_port);

        // TCP lifecycle: a plain SYN starts a new connection on this tuple.
        if matches!(packet.protocol, Protocol::TCP)
            && ethernet::tcp_is_new_connection_syn(packet.tcp_flags)
        {
            let tuple = (src_ip, src_port, dst_ip, dst_port);
            return self
                .start_new_stream(&tuple, packet, service_id, agent_id)
                .await;
        }

        // Lock-free fast path: cached stream that needs no rotation. A
        // starred segment at its byte limit falls through to the locked
        // path below (the lock also serializes concurrent rotations).
        if let Some(id) = self.cache.streams.get_by_key(&key) {
            if !self.rotation_needed(id, packet.payload.len()) {
                return Ok(id);
            }
        }

        // Per-key lock for stream creation and rotation (prevents duplicates)
        let lock = self
            .cache
            .stream_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock.lock().await;

        // Re-check after lock: another task may have created or rotated the
        // segment for this tuple meanwhile.
        let mut stream_id = self.cache.streams.get_by_key(&key);

        // Try database
        if stream_id.is_none() {
            if let Some(id) = self
                .db
                .get_stream_id(&src_ip, src_port, &dst_ip, dst_port)
                .await
            {
                self.cache
                    .streams
                    .insert(&src_ip, src_port, &dst_ip, dst_port, id);
                stream_id = Some(id);
            }
        }

        // Create new stream
        let stream_id = match stream_id {
            Some(id) => id,
            None => {
                let id = self
                    .db
                    .create_stream(
                        &src_ip,
                        src_port,
                        &dst_ip,
                        dst_port,
                        &packet.protocol,
                        &packet.ip_version,
                        service_id,
                        packet.timestamp_seconds as i64,
                        Some(agent_id),
                    )
                    .await
                    .map_err(|e| Error::other(format!("Stream creation failed: {:?}", e)))?;

                self.cache
                    .streams
                    .insert(&src_ip, src_port, &dst_ip, dst_port, id);
                id
            }
        };

        // Rotation decision under the same lock: concurrent packets of the
        // tuple cannot both rotate; the loser re-reads the cache and lands
        // in the fresh continuation. All later packets take the fast path.
        if self.rotation_needed(stream_id, packet.payload.len()) {
            return self
                .rotate_segment(stream_id, packet, direction, service_id, agent_id)
                .await;
        }

        Ok(stream_id)
    }

    /// Open a new stream for a plain SYN on (possibly reused) tuple:
    /// retires the previously cached stream for the tuple (deferred
    /// active=false flush) and creates a fresh row so connections on a
    /// reused 4-tuple no longer merge into one stream.
    /// `tuple` is the direction-normalized (src_ip, src_port, dst_ip, dst_port).
    async fn start_new_stream(
        &self,
        tuple: &(String, u16, String, u16),
        packet: &PacketInfo,
        service_id: usize,
        agent_id: &str,
    ) -> Result<usize, Error> {
        let (src_ip, src_port, dst_ip, dst_port) = tuple;
        let key = StreamsCache::format_key(src_ip, *src_port, dst_ip, *dst_port);

        // Same per-key lock as the miss path: concurrent SYNs/data for one
        // tuple must not interleave retire/create/caches.
        let lock = self
            .cache
            .stream_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock.lock().await;

        // Double-check under the lock: a retransmitted plain SYN for a
        // connection that has not carried data yet reuses its stream.
        let syn_pending = self.cache.syn_pending(&key);
        let cached = self.cache.streams.get_by_key(&key);
        if !Self::should_start_new_stream(true, syn_pending, cached.is_some()) {
            return Ok(cached.expect("syn retransmission implies a cached stream"));
        }

        // Retire the old connection on this tuple: drop the cache entry and
        // queue active=false for the deferred batch flush.
        if let Some(old_id) = self.cache.streams.remove_by_key(&key) {
            self.cache.mark_stream_closed(old_id);
        }

        let stream_id = self
            .db
            .create_stream(
                src_ip,
                *src_port,
                dst_ip,
                *dst_port,
                &packet.protocol,
                &packet.ip_version,
                service_id,
                packet.timestamp_seconds as i64,
                Some(agent_id),
            )
            .await
            .map_err(|e| {
                // Degradation, not failure: the SYN's stream row is missing
                // but the next payload packet falls through to the normal
                // miss path and creates one.
                std::io::Error::other(format!("Stream creation on SYN failed: {:?}", e))
            })?;

        self.cache
            .streams
            .insert(src_ip, *src_port, dst_ip, *dst_port, stream_id);
        self.cache.mark_syn_pending(&key);
        Ok(stream_id)
    }

    pub async fn process_packet(
        &self,
        packet: PacketInfo,
        context: &PacketContext,
    ) -> Result<(), Error> {
        // Find service and direction. Unknown-service packets are dropped
        // immediately (never buffered): the loss is observable via the
        // aggregate counter and may trigger asynchronous reconciliation.
        let (service_id, direction) = match self.find_service_and_direction(&packet, context) {
            ServiceMatch::Found(service_id, direction) => (service_id, direction),
            ServiceMatch::Ambiguous => {
                let dropped =
                    metrics::AMBIGUOUS_DIRECTION_DROPS.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::rate_limited_log("ambiguous-direction", dropped, || {
                    format!(
                        "agent '{}': both endpoints match the same service under wildcard machine IP",
                        context.agent_id
                    )
                });
                return Ok(());
            }
            ServiceMatch::Unknown => {
                let dropped = metrics::UNKNOWN_SERVICE_DROPS.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::rate_limited_log("unknown-service", dropped, || {
                    format!(
                        "agent '{}': no service covers {} port {}",
                        context.agent_id, packet.dst_ip, packet.dst_port
                    )
                });
                return Ok(());
            }
        };

        debug!(
            "Processing packet: \nService ID: {:?}\nDirection: {:?}",
            service_id, direction
        );

        // TCP lifecycle (flags absent/UDP -> same behavior as before).
        let is_tcp = matches!(packet.protocol, Protocol::TCP);
        let is_close = is_tcp && ethernet::tcp_is_connection_close(packet.tcp_flags);

        // FIN/RST: retire the tuple's stream — deferred active=false flush,
        // no synchronous UPDATE on the hot path. The cache entry STAYS so
        // half-close data (other direction still sending) still attributes
        // to this stream; only a later plain SYN starts a new one.
        if is_close {
            let key = Self::tuple_key(&packet, &direction);
            if let Some(stream_id) = self.cache.streams.get_by_key(&key) {
                self.cache.mark_stream_closed(stream_id);
            }
            self.cache.clear_syn_pending(&key);
        }

        // Control-only packets (SYN/FIN/RST without payload): lifecycle only
        // — no packet row, no filters, no L7, no NATS event.
        if packet.payload.is_empty() {
            if is_tcp && ethernet::tcp_is_new_connection_syn(packet.tcp_flags) {
                let tuple = Self::normalized_tuple(&packet, &direction);
                self.start_new_stream(&tuple, &packet, service_id, &context.agent_id)
                    .await?;
            }
            return Ok(());
        }

        // Payload-bearing packet: this connection has data, so a later plain
        // SYN on the tuple is a new connection, not a retransmission.
        {
            let key = Self::tuple_key(&packet, &direction);
            self.cache.clear_syn_pending(&key);
        }

        let stream_id = self
            .find_stream_id(&packet, &direction, service_id, &context.agent_id)
            .await?;
        debug!("Stream ID: {:?}", stream_id);

        // Checking all filters (direction-aware, target-aware, OR
        // semantics: every matching filter emits its own tag) and running
        // the threshold gates for alert/block actions.
        let machine_ip = context.machine_ip;
        let filter_ctx = crate::filters::FilterContext {
            payload: &packet.payload,
            direction,
            agent_id: &context.agent_id,
            service_id: service_id as u64,
            stream_id: stream_id as u64,
            src_ip: packet.src_ip,
            dst_ip: packet.dst_ip,
            src_port: packet.src_port,
            dst_port: packet.dst_port,
            machine_ip,
            protocol: match packet.protocol {
                Protocol::TCP => "TCP",
                Protocol::UDP => "UDP",
            },
            timestamp_secs: packet.timestamp_seconds,
            now_ms: now_epoch_ms(),
        };
        let matches = crate::filters::apply_filters(&filter_ctx);
        debug!("Filter matches: {}", matches.len());

        // Flatten payload offsets into (filter_id, start_index, end_index)
        // and process threshold-gated actions (alerts and temporary
        // blocks). Each persisted packet match counts ONCE per filter
        // (matches are already one entry per filter).
        let mut trigger_tuples: Vec<(usize, usize, usize)> = Vec::new();
        for m in &matches {
            for t in &m.offsets {
                if m.offset_tag {
                    trigger_tuples.push((m.filter_id as usize, t.start_index, t.end_index));
                }
            }
            if let Some(threshold) = &m.threshold {
                process_threshold_match(&self.db, m, threshold, &filter_ctx, stream_id).await;
            }
        }

        let direction_str = match direction {
            Direction::Inbound => "inbound".to_string(),
            Direction::Outbound => "outbound".to_string(),
        };

        // Try L7 HTTP parsing (only if service has http_parse enabled)
        let should_parse_http = {
            let services = self.cache.services.read().unwrap();
            services
                .get(&service_id)
                .map(|s| s.http_parse)
                .unwrap_or(false)
        };

        let mut http_message: Option<HttpMessageData> = None;

        if should_parse_http {
            if let Some(parsed) = http_parser::try_parse_http(&packet.payload) {
                let content_type = parsed
                    .headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case("content-type"))
                    .map(|(_, v)| v.clone());
                let content_length = parsed
                    .headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse::<i32>().ok());
                let body_length = (packet.payload.len() as i32 - parsed.body_offset as i32).max(0);

                // Header dedup key + JSONB — resolved to a header_set id per
                // flush batch (no per-packet DB call, no stale id cache).
                let (header_hash, header_json) = if parsed.headers.is_empty() {
                    (None, None)
                } else {
                    let (h, j) = DB::hash_headers(&parsed.headers);
                    (Some(h), Some(j.to_string()))
                };

                // Detect gRPC
                let is_grpc = content_type
                    .as_deref()
                    .map(|ct| ct.starts_with("application/grpc"))
                    .unwrap_or(false);

                let mut grpc_data: Option<GrpcMessageData> = None;
                let l7_protocol;

                if is_grpc {
                    let body = &packet.payload[parsed.body_offset..];
                    let proto = {
                        let schemas = self.cache.proto_schemas.read().unwrap();
                        schemas.get(&service_id).cloned()
                    };
                    if let Some(grpc) = grpc_parser::try_parse_grpc_body(
                        body,
                        parsed.path.as_deref(),
                        parsed.is_request,
                        proto.as_ref(),
                    ) {
                        l7_protocol = "grpc".to_string();
                        grpc_data = Some(GrpcMessageData {
                            stream_id,
                            service_name: grpc.service_name,
                            method_name: grpc.method_name,
                            is_request: grpc.is_request,
                            decoded_json: grpc.decoded_json,
                            timestamp: packet.timestamp_seconds as i64,
                        });
                        debug!("gRPC parsed for batch");
                    } else {
                        l7_protocol = "http2".to_string();
                    }
                } else {
                    l7_protocol = "http1".to_string();
                }

                http_message = Some(HttpMessageData {
                    stream_id,
                    is_request: parsed.is_request,
                    method: parsed.method,
                    path: parsed.path,
                    status_code: parsed.status_code,
                    http_version: parsed.http_version,
                    content_type,
                    content_length,
                    body_offset: parsed.body_offset as i32,
                    body_length,
                    header_hash,
                    header_json,
                    timestamp: packet.timestamp_seconds as i64,
                    l7_protocol,
                    grpc: grpc_data,
                    user_agent: crate::user_agent::classify(&parsed.headers, parsed.is_request),
                });

                debug!(
                    "HTTP parsed for batch: {} headers (hash={:?})",
                    parsed.headers.len(),
                    header_hash
                );
            }
        } // should_parse_http

        // Build NATS event data
        let nats_event = Some(NatsEventData {
            stream_id,
            timestamp_seconds: packet.timestamp_seconds,
            timestamp_microseconds: packet.timestamp_microseconds,
            direction: direction_str.clone(),
        });

        // Build the ProcessedPacket and send to batch channel
        let processed = ProcessedPacket {
            timestamp_seconds: packet.timestamp_seconds,
            timestamp_microseconds: packet.timestamp_microseconds,
            direction: direction_str,
            stream_id,
            payload: packet.payload,
            triggers: trigger_tuples,
            http_message,
            grpc_message: None, // gRPC is nested inside http_message
            nats_event,
        };

        // Route to this stream's flusher queue. Never blocks (a full queue
        // drops the packet with a counter inside) — blocking here would hold
        // one of the analyzer's concurrency permits and backpressure the
        // NATS consumer.
        self.batch_tx
            .send(processed)
            .map_err(|e| Error::other(format!("Batch channel send failed: {}", e)))?;

        Ok(())
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Threshold gate for one (packet, filter) match. Tag rows are emitted
/// unconditionally by the caller; this only gates the alert or block
/// action, counting each persisted packet match once per filter.
async fn process_threshold_match(
    db: &DB,
    m: &crate::filters::FilterMatch,
    threshold: &crate::filters::spec::ThresholdCfg,
    ctx: &crate::filters::FilterContext<'_>,
    stream_id: usize,
) {
    use crate::filters::spec::{Action, GroupBy};
    use crate::thresholds::{record_hit, CounterKey, ThresholdOutcome};

    let now = now_epoch_secs();
    let remote_ip = crate::filters::remote_ip_of(ctx);
    let key = match threshold.group_by {
        GroupBy::RemoteIp => CounterKey::RemoteIp {
            filter_id: m.filter_id,
            agent_id: ctx.agent_id.to_string(),
            ip: remote_ip.to_string(),
        },
        GroupBy::Stream => CounterKey::Stream {
            filter_id: m.filter_id,
            agent_id: ctx.agent_id.to_string(),
            stream_id: ctx.stream_id,
        },
        GroupBy::Service => CounterKey::Service {
            filter_id: m.filter_id,
            agent_id: ctx.agent_id.to_string(),
            service_id: ctx.service_id,
        },
    };

    match record_hit(
        key,
        threshold.count,
        threshold.window_secs,
        threshold.cooldown_secs,
        now,
    ) {
        ThresholdOutcome::Below | ThresholdOutcome::Suppressed => return,
        ThresholdOutcome::Fire => {}
    }

    let (kind, block_until): (&str, Option<i64>) = match m.action {
        Action::Alert => ("alert", None),
        Action::Block => {
            // A threshold block NEVER blocks the local machine address.
            if remote_ip == ctx.machine_ip {
                metrics::THRESHOLD_BLOCK_REJECTS.fetch_add(1, Ordering::Relaxed);
                metrics::rate_limited_log(
                    "threshold-block-local",
                    metrics::THRESHOLD_BLOCK_REJECTS.load(Ordering::Relaxed),
                    || {
                        format!(
                            "filter {} threshold block rejected: {} is the agent's machine IP",
                            m.filter_id, remote_ip
                        )
                    },
                );
                return;
            }
            // Validate the filter is still registered with the same
            // revision (removed or updated filters must not mint blocks).
            if crate::filters::filter_revision(m.filter_id) != Some(m.revision) {
                metrics::THRESHOLD_BLOCK_REJECTS.fetch_add(1, Ordering::Relaxed);
                return;
            }
            let until = (now + u64::from(threshold.block_secs)) as i64;
            // Bounded retry: one attempt after a short backoff, then drop
            // with a counter (degradation, never ingestion failure).
            let ip_str = remote_ip.to_string();
            let mut result = db
                .insert_active_block(m.filter_id as i64, ctx.agent_id, &ip_str, until)
                .await;
            if result.is_err() {
                tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                result = db
                    .insert_active_block(m.filter_id as i64, ctx.agent_id, &ip_str, until)
                    .await;
            }
            if let Err(e) = result {
                metrics::THRESHOLD_BLOCK_REJECTS.fetch_add(1, Ordering::Relaxed);
                metrics::rate_limited_log(
                    "threshold-block-write",
                    metrics::THRESHOLD_BLOCK_REJECTS.load(Ordering::Relaxed),
                    || format!("filter {} active_block insert failed: {:?}", m.filter_id, e),
                );
                return;
            }
            debug!(
                "Filter {} threshold block: {} banned on agent {} for {}s",
                m.filter_id, remote_ip, ctx.agent_id, threshold.block_secs
            );
            ("block", Some(until))
        }
        // Tag and flag filters cannot carry thresholds (validated at the
        // backend); a mismatch here is inert.
        _ => return,
    };

    crate::filter_events::queue_event(crate::filter_events::FilterEventRow {
        filter_id: m.filter_id as i64,
        agent_id: ctx.agent_id.to_string(),
        service_id: Some(ctx.service_id as i64),
        stream_id: Some(stream_id as i64),
        remote_ip: Some(remote_ip.to_string()),
        kind: kind.to_string(),
        window_secs: Some(threshold.window_secs as i32),
        match_count: Some(threshold.count as i32),
    });
    // block_until is recorded in active_block; the event row keeps the
    // audit trail.
    let _ = block_until;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::updater::dto::{Service as DtoService, TransportProto};
    use std::net::IpAddr;

    fn service(id: usize, port: u16, protocol: TransportProto) -> (usize, DtoService) {
        (
            id,
            DtoService {
                name: format!("svc-{id}"),
                port,
                protocol,
                chunked_transfer_encoding: false,
                decrypt_tls: false,
                inflate_websockets: false,
                merge_adjacent_packets: false,
                urldecode: false,
                http_parse: false,
            },
        )
    }

    fn packet(
        src: &str,
        src_port: u16,
        dst: &str,
        dst_port: u16,
        protocol: Protocol,
    ) -> PacketInfo {
        PacketInfo {
            payload: vec![1, 2, 3],
            timestamp_seconds: 0,
            timestamp_microseconds: 0,
            src_ip: src.parse::<IpAddr>().unwrap(),
            src_port,
            dst_ip: dst.parse::<IpAddr>().unwrap(),
            dst_port,
            protocol,
            ip_version: crate::ethernet::IpVersion::IPv4,
            stream_id: None,
            tcp_flags: 0,
        }
    }

    fn services() -> Vec<(usize, DtoService)> {
        vec![
            service(1, 80, TransportProto::Tcp),
            service(2, 53, TransportProto::Udp),
        ]
    }

    #[test]
    fn tcp_and_udp_coexist_on_the_same_port() {
        let services = vec![
            service(1, 8080, TransportProto::Tcp),
            service(2, 8080, TransportProto::Udp),
        ];
        let tcp_packet = packet("10.0.0.1", 40000, "10.0.0.5", 8080, Protocol::TCP);
        let udp_packet = packet("10.0.0.1", 40000, "10.0.0.5", 8080, Protocol::UDP);

        let tcp_match = resolve_service_match(
            services.iter().map(|(id, s)| (*id, s)),
            &tcp_packet,
            "10.0.0.5".parse().unwrap(),
        );
        let udp_match = resolve_service_match(
            services.iter().map(|(id, s)| (*id, s)),
            &udp_packet,
            "10.0.0.5".parse().unwrap(),
        );
        assert_eq!(tcp_match, ServiceMatch::Found(1, Direction::Inbound));
        assert_eq!(udp_match, ServiceMatch::Found(2, Direction::Inbound));
    }

    #[test]
    fn protocol_mismatch_is_unknown_not_tcp_fallback() {
        // A UDP packet must never match a TCP-only service set: the loss
        // degrades to a counted unknown-service drop instead of a silent
        // reinterpretation.
        let services = vec![service(1, 53, TransportProto::Tcp)];
        let udp_packet = packet("10.0.0.1", 40000, "10.0.0.5", 53, Protocol::UDP);
        assert_eq!(
            resolve_service_match(
                services.iter().map(|(id, s)| (*id, s)),
                &udp_packet,
                "10.0.0.5".parse().unwrap(),
            ),
            ServiceMatch::Unknown
        );
    }

    #[test]
    fn concrete_machine_ip_requires_endpoint_match() {
        let local: IpAddr = "10.0.0.5".parse().unwrap();
        let response = packet("10.0.0.5", 80, "10.0.0.9", 40000, Protocol::TCP);
        let request = packet("10.0.0.9", 40000, "10.0.0.5", 80, Protocol::TCP);

        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &response, local),
            ServiceMatch::Found(1, Direction::Outbound)
        );
        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &request, local),
            ServiceMatch::Found(1, Direction::Inbound)
        );

        // Same ports, but neither endpoint is this machine.
        let other = packet("10.1.0.1", 80, "10.1.0.9", 40000, Protocol::TCP);
        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &other, local),
            ServiceMatch::Unknown
        );
    }

    #[test]
    fn wildcard_machine_ip_matches_by_protocol_and_port() {
        let wildcard: IpAddr = "0.0.0.0".parse().unwrap();
        let pkt = packet("172.16.0.9", 40000, "172.16.0.5", 80, Protocol::TCP);
        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &pkt, wildcard),
            ServiceMatch::Found(1, Direction::Inbound)
        );

        // Outbound under wildcard: service port on the source side.
        let out = packet("172.16.0.5", 80, "172.16.0.9", 40000, Protocol::TCP);
        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &out, wildcard),
            ServiceMatch::Found(1, Direction::Outbound)
        );

        // IPv6 wildcard behaves the same.
        let pkt6 = packet("fd00::9", 40000, "fd00::5", 80, Protocol::TCP);
        assert_eq!(
            resolve_service_match(
                services().iter().map(|(id, s)| (*id, s)),
                &pkt6,
                "::".parse().unwrap()
            ),
            ServiceMatch::Found(1, Direction::Inbound)
        );
    }

    #[test]
    fn wildcard_both_endpoints_matching_is_ambiguous_and_dropped() {
        let wildcard: IpAddr = "0.0.0.0".parse().unwrap();
        // src port 80 and dst port 80: direction cannot be decided.
        let pkt = packet("10.0.0.9", 80, "10.0.0.10", 80, Protocol::TCP);
        assert_eq!(
            resolve_service_match(services().iter().map(|(id, s)| (*id, s)), &pkt, wildcard),
            ServiceMatch::Ambiguous
        );
    }

    #[test]
    fn normalized_tuple_puts_local_machine_on_src_side_for_both_directions() {
        use std::net::Ipv4Addr;

        // The analyzed machine hosts the service on port 80 (that is what
        // find_service_and_direction matches on): client requests are
        // Inbound (dst = local:80), server responses are Outbound
        // (src = local:80).
        let local: std::net::IpAddr = Ipv4Addr::new(10, 0, 0, 5).into();
        let remote: std::net::IpAddr = Ipv4Addr::new(10, 0, 0, 9).into();

        let response_packet = PacketInfo {
            // local:80 -> remote:40000
            payload: vec![1, 2, 3],
            timestamp_seconds: 0,
            timestamp_microseconds: 0,
            src_ip: local,
            src_port: 80,
            dst_ip: remote,
            dst_port: 40000,
            protocol: Protocol::TCP,
            ip_version: crate::ethernet::IpVersion::IPv4,
            stream_id: None,
            tcp_flags: 0,
        };

        let request_packet = PacketInfo {
            // remote:40000 -> local:80
            src_ip: remote,
            src_port: 40000,
            dst_ip: local,
            dst_port: 80,
            ..response_packet.clone()
        };

        let out = PacketProcessor::normalized_tuple(&response_packet, &Direction::Outbound);
        let inc = PacketProcessor::normalized_tuple(&request_packet, &Direction::Inbound);

        // Both directions of one connection share one tuple (and thus one
        // stream): src is the local machine's service side.
        assert_eq!(out, inc);
        assert_eq!(out.0, local.to_string());
        assert_eq!(out.1, 80);
        assert_eq!(out.2, remote.to_string());
        assert_eq!(out.3, 40000);

        // ...and therefore share one cache key — what a FIN from either
        // direction must invalidate.
        assert_eq!(
            PacketProcessor::tuple_key(&response_packet, &Direction::Outbound),
            PacketProcessor::tuple_key(&request_packet, &Direction::Inbound),
        );
    }

    #[test]
    fn agent_registry_rejects_stale_snapshots() {
        let cache = Cache::new(60);
        cache.set_agent_network("a1", "10.0.0.5".parse().unwrap(), 5);
        assert_eq!(
            cache.agent_machine_ip("a1"),
            Some("10.0.0.5".parse().unwrap())
        );

        // Stale revision must not roll back a newer value.
        cache.set_agent_network("a1", "10.9.9.9".parse().unwrap(), 4);
        assert_eq!(
            cache.agent_machine_ip("a1"),
            Some("10.0.0.5".parse().unwrap())
        );

        // Newer revision applies; unknown agents resolve to None.
        cache.set_agent_network("a1", "10.0.0.7".parse().unwrap(), 6);
        assert_eq!(
            cache.agent_machine_ip("a1"),
            Some("10.0.0.7".parse().unwrap())
        );
        assert_eq!(cache.agent_machine_ip("ghost"), None);
    }

    #[test]
    fn first_syn_on_fresh_tuple_opens_new_stream() {
        // No cached stream for the tuple yet: the SYN must create one.
        assert!(PacketProcessor::should_start_new_stream(true, false, false));
    }

    #[test]
    fn syn_after_data_opens_new_stream_and_invalidates_cache() {
        // Tuple reused by a NEW connection (old stream cached, data already
        // flowed so syn_pending was cleared): must NOT reuse the old stream.
        assert!(PacketProcessor::should_start_new_stream(true, false, true));
    }

    #[test]
    fn syn_after_close_opens_new_stream() {
        // FIN/RST cleared syn_pending: a following plain SYN is a new
        // connection even though the closed stream may still be cached.
        assert!(PacketProcessor::should_start_new_stream(true, false, true));
    }

    #[test]
    fn retransmitted_syn_reuses_pending_stream() {
        // SYN seen, no data yet, stream cached: this is a retransmission —
        // reusing the stream avoids forking duplicates per dropped SYN.
        assert!(!PacketProcessor::should_start_new_stream(true, true, true));
    }

    #[test]
    fn syn_pending_without_cached_stream_still_creates() {
        // syn_pending set but the cache entry is gone (e.g. TTL eviction
        // raced the handshake): create rather than lose the connection.
        assert!(PacketProcessor::should_start_new_stream(true, true, false));
    }

    #[test]
    fn non_syn_packets_never_take_the_new_stream_path() {
        assert!(!PacketProcessor::should_start_new_stream(
            false, false, false
        ));
        assert!(!PacketProcessor::should_start_new_stream(false, true, true));
    }
}
