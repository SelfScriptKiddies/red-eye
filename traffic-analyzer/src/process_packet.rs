use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use crate::http_parser;
use crate::grpc_parser;
use crate::batch_flusher::{ProcessedPacket, HttpMessageData, GrpcMessageData, NatsEventData};
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
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
    pub packet_id: i32,
    pub stream_id: usize,
    pub timestamp_seconds: u64,
    pub timestamp_microseconds: u64,
    pub direction: String,
}

 pub struct PacketProcessor {
    cache: Arc<Cache>,
    config: Arc<ApplicationConfig>,
    db: DB,
    batch_tx: mpsc::Sender<ProcessedPacket>,
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB, batch_tx: mpsc::Sender<ProcessedPacket>) -> Self {
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

    async fn find_stream_id(&self, packet: &PacketInfo, direction: &Direction, service_id: usize) -> Result<usize, Error> {
        let (src_ip, dst_ip, src_port, dst_port) = match direction {
            Direction::Outbound => (
                &packet.src_ip, &packet.dst_ip, packet.src_port, packet.dst_port,
            ),
            Direction::Inbound => (
                &packet.dst_ip, &packet.src_ip, packet.dst_port, packet.src_port,
            ),
        };

        let key = StreamsCache::format_key(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port);

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
        if let Some(stream_id) = self.db.get_stream_id(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port).await {
            self.cache.streams.insert(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, stream_id);
            return Ok(stream_id);
        }

        // Create new stream
        let stream_id = self.db.create_stream(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, &packet.protocol, &packet.ip_version, service_id).await
            .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Stream creation failed: {:?}", e)))?;

        self.cache.streams.insert(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, stream_id);
        Ok(stream_id)
    }

    pub async fn process_packet(&self, packet: PacketInfo) -> Result<(), Error> {
        // Find service and direction
        let (service_id, direction) = self.find_service_and_direction(&packet)
                                                            .ok_or(Error::new(std::io::ErrorKind::Other, "Service not found"))?;

        debug!("Processing packet: \nService ID: {:?}\nDirection: {:?}", service_id, direction);

        let stream_id = self.find_stream_id(&packet, &direction, service_id).await?;
        debug!("Stream ID: {:?}", stream_id);

        // Checking all of the filters
        let triggers = apply_filters(&packet.payload);
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
                match self.db.get_or_create_header_set(&parsed.headers).await {
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

        self.batch_tx.send(processed).await
            .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Batch channel send failed: {}", e)))?;

        Ok(())
    }
}
