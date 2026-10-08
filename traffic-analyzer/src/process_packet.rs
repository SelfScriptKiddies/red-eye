use std::io::Error;
use crate::ethernet::{self, PacketInfo, Protocol};
use crate::filters::apply_filters;
use crate::http_parser;
use crate::grpc_parser;
use crate::batch_flusher::{ProcessedPacket, HttpMessageData, GrpcMessageData, NatsEventData, ShardedBatchSender};
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::cache::Cache;
use crate::cache::streams::StreamsCache;
use crate::configuration::ApplicationConfig;
use crate::db::DB;

#[derive(Debug, PartialEq)]
pub enum Direction {
    Inbound,
    Outbound
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
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB, batch_tx: Arc<ShardedBatchSender>) -> Self {
        Self {
            cache,
            config,
            db,
            batch_tx,
        }
    }

    fn compare_ip(&self, filter_ip: &String, got_ip: &String) -> bool {
        filter_ip == got_ip || filter_ip == "0.0.0.0"
    }

    fn find_service_and_direction(&self, packet: &PacketInfo) -> Option<(usize, Direction)> {
        let services = self.cache.services.read().unwrap();
        for (id, service) in services.iter() {
            if self.compare_ip(&self.config.machine_ip, &packet.src_ip.to_string()) && service.port == packet.src_port {
                return Some((*id, Direction::Outbound));
            }

            if self.compare_ip(&self.config.machine_ip, &packet.dst_ip.to_string()) && service.port == packet.dst_port {
                return Some((*id, Direction::Inbound));
            }
        }
        None
    }

    /// Direction-normalized 4-tuple: src is always the local machine's side,
    /// so both directions of one connection map onto the same tuple.
    fn normalized_tuple(packet: &PacketInfo, direction: &Direction) -> (String, u16, String, u16) {
        match direction {
            Direction::Outbound => (
                packet.src_ip.to_string(), packet.src_port,
                packet.dst_ip.to_string(), packet.dst_port,
            ),
            Direction::Inbound => (
                packet.dst_ip.to_string(), packet.dst_port,
                packet.src_ip.to_string(), packet.src_port,
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

    async fn find_stream_id(&self, packet: &PacketInfo, direction: &Direction, service_id: usize) -> Result<usize, Error> {
        let (src_ip, src_port, dst_ip, dst_port) = Self::normalized_tuple(packet, direction);
        let key = StreamsCache::format_key(&src_ip, src_port, &dst_ip, dst_port);

        // TCP lifecycle: a plain SYN starts a new connection on this tuple.
        if matches!(packet.protocol, Protocol::TCP)
            && ethernet::tcp_is_new_connection_syn(packet.tcp_flags)
        {
            let tuple = (src_ip, src_port, dst_ip, dst_port);
            return self.start_new_stream(&tuple, packet, service_id).await;
        }

        // Lock-free lookup
        if let Some(id) = self.cache.streams.get_by_key(&key) {
            return Ok(id);
        }

        // Per-key lock for stream creation (prevents duplicates)
        let lock = self.cache.stream_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock.lock().await;

        // Double-check after lock
        if let Some(stream_id) = self.cache.streams.get_by_key(&key) {
            return Ok(stream_id);
        }

        // Try database
        if let Some(stream_id) = self.db.get_stream_id(&src_ip, src_port, &dst_ip, dst_port).await {
            self.cache.streams.insert(&src_ip, src_port, &dst_ip, dst_port, stream_id);
            return Ok(stream_id);
        }

        // Create new stream
        let stream_id = self.db.create_stream(&src_ip, src_port, &dst_ip, dst_port, &packet.protocol, &packet.ip_version, service_id, packet.timestamp_seconds as i64).await
            .map_err(|e| Error::other(format!("Stream creation failed: {:?}", e)))?;

        self.cache.streams.insert(&src_ip, src_port, &dst_ip, dst_port, stream_id);
        Ok(stream_id)
    }

    /// Open a new stream for a plain SYN on (possibly reused) tuple:
    /// retires the previously cached stream for the tuple (deferred
    /// active=false flush) and creates a fresh row so connections on a
    /// reused 4-tuple no longer merge into one stream.
    /// `tuple` is the direction-normalized (src_ip, src_port, dst_ip, dst_port).
    async fn start_new_stream(&self, tuple: &(String, u16, String, u16), packet: &PacketInfo, service_id: usize) -> Result<usize, Error> {
        let (src_ip, src_port, dst_ip, dst_port) = tuple;
        let key = StreamsCache::format_key(src_ip, *src_port, dst_ip, *dst_port);

        // Same per-key lock as the miss path: concurrent SYNs/data for one
        // tuple must not interleave retire/create/caches.
        let lock = self.cache.stream_locks
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

        let stream_id = self.db.create_stream(src_ip, *src_port, dst_ip, *dst_port, &packet.protocol, &packet.ip_version, service_id, packet.timestamp_seconds as i64).await
            .map_err(|e| {
                // Degradation, not failure: the SYN's stream row is missing
                // but the next payload packet falls through to the normal
                // miss path and creates one.
                std::io::Error::other(format!("Stream creation on SYN failed: {:?}", e))
            })?;

        self.cache.streams.insert(src_ip, *src_port, dst_ip, *dst_port, stream_id);
        self.cache.mark_syn_pending(&key);
        Ok(stream_id)
    }

    pub async fn process_packet(&self, packet: PacketInfo) -> Result<(), Error> {
        // Find service and direction
        let (service_id, direction) = self.find_service_and_direction(&packet)
                                                            .ok_or(Error::other("Service not found"))?;

        debug!("Processing packet: \nService ID: {:?}\nDirection: {:?}", service_id, direction);

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
                self.start_new_stream(&tuple, &packet, service_id).await?;
            }
            return Ok(());
        }

        // Payload-bearing packet: this connection has data, so a later plain
        // SYN on the tuple is a new connection, not a retransmission.
        {
            let key = Self::tuple_key(&packet, &direction);
            self.cache.clear_syn_pending(&key);
        }

        let stream_id = self.find_stream_id(&packet, &direction, service_id).await?;
        debug!("Stream ID: {:?}", stream_id);

        // Checking all of the filters (direction-aware: a filter tagged
        // 'in'/'out' only fires on packets flowing that way)
        let triggers = apply_filters(&packet.payload, &direction);
        debug!("Triggers: {:?}", triggers);

        // Flatten triggers into (filter_id, start_index, end_index)
        let trigger_tuples: Vec<(usize, usize, usize)> = triggers
            .iter()
            .flat_map(|(filter_id, arr)| {
                arr.iter().map(move |t| (*filter_id, t.start_index, t.end_index))
            })
            .collect();

        let direction_str = match direction {
            Direction::Inbound => "inbound".to_string(),
            Direction::Outbound => "outbound".to_string(),
        };

        // Try L7 HTTP parsing (only if service has http_parse enabled)
        let should_parse_http = {
            let services = self.cache.services.read().unwrap();
            services.get(&service_id).map(|s| s.http_parse).unwrap_or(false)
        };

        let mut http_message: Option<HttpMessageData> = None;

        if should_parse_http {
        if let Some(parsed) = http_parser::try_parse_http(&packet.payload) {
            let content_type = parsed.headers.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case("content-type"))
                .map(|(_, v)| v.clone());
            let content_length = parsed.headers.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, v)| v.parse::<i32>().ok());
            let body_length = (packet.payload.len() as i32 - parsed.body_offset as i32).max(0);

            // Deduplicate headers
            let header_set_id = if !parsed.headers.is_empty() {
                match self.db.get_or_create_header_set(&parsed.headers, &self.cache.header_sets).await {
                    Ok(id) => Some(id),
                    Err(e) => {
                        error!("Failed to store header set: {:?}", e);
                        None
                    }
                }
            } else {
                None
            };

            // Detect gRPC
            let is_grpc = content_type.as_deref()
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
                header_set_id,
                timestamp: packet.timestamp_seconds as i64,
                l7_protocol,
                grpc: grpc_data,
            });

            debug!("HTTP parsed for batch: {} headers (set_id={:?})",
                parsed.headers.len(), header_set_id
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
        self.batch_tx.send(processed)
            .map_err(|e| Error::other(format!("Batch channel send failed: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!PacketProcessor::should_start_new_stream(false, false, false));
        assert!(!PacketProcessor::should_start_new_stream(false, true, true));
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

        let response_packet = PacketInfo { // local:80 -> remote:40000
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

        let request_packet = PacketInfo { // remote:40000 -> local:80
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
}
