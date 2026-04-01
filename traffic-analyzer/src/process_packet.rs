use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use crate::http_parser;
use crate::grpc_parser;
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::Mutex;
use async_nats::Client;
use bincode::{config, Encode};
use crate::cache::Cache;
use crate::cache::streams::StreamsCache;
use crate::configuration::ApplicationConfig;
use crate::db::DB;

#[derive(Debug, PartialEq)]
pub enum Direction {
    Inbound,
    Outbound
}

#[derive(Debug, Encode)]
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
    nats_client: Option<Client>,
    nats_topic: String,
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB, nats_client: Option<Client>) -> Self {
        let nats_topic = config.nats_websocket_packets_topic.clone();
        Self { 
            cache, 
            config, 
            db,
            nats_client,
            nats_topic,
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
        // Swap src and dst if direction is inbound
        let (src_ip, dst_ip, src_port, dst_port) = match direction {
            Direction::Outbound => (
                &packet.src_ip,
                &packet.dst_ip,
                packet.src_port,
                packet.dst_port,
            ),
            Direction::Inbound => (
                &packet.dst_ip,
                &packet.src_ip,
                packet.dst_port,
                packet.src_port,
            ),
        };
        
        let key = StreamsCache::format_key(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port);

        // Fast lookup without lock (read-only check)
        {
            let mut stream_cache = self.cache.streams.lock().await;
            if let Some(id) = stream_cache.get_by_key(&key) {
                return Ok(id);
            }
        }

        // Get or create a per-key lock to prevent race conditions for this specific stream
        // This allows different streams to be processed concurrently while preventing
        // duplicate creation for the same stream
        let lock = self.cache.stream_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        
        let _guard = lock.lock().await;

        // Double-check after acquiring lock (another thread might have created it)
        {
            let mut stream_cache = self.cache.streams.lock().await;
            if let Some(stream_id) = stream_cache.get_by_key(&key) {
                return Ok(stream_id);
            }
        }

        // If not found in cache, try database
        if let Some(stream_id) = self.db.get_stream_id(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port).await {
            // Insert into cache for future lookups
            let mut stream_cache = self.cache.streams.lock().await;
            stream_cache.insert(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, stream_id);
            return Ok(stream_id);
        }
        
        // If not found, create new stream
        let stream_id = self.db.create_stream(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, &packet.protocol, &packet.ip_version, service_id).await
                                        .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Stream creation failed: {:?}", e)))?;
        
        // Insert into cache
        let mut stream_cache = self.cache.streams.lock().await;
        stream_cache.insert(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, stream_id);
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

        // Inserting packet into database
        let packet_id = self.db.insert_packet(&packet, stream_id, &direction).await
            .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Packet insertion failed: {:?}", e)))?;
        debug!("Packet ID: {:?}", packet_id);

        // Inserting triggers into database (batched)
        let trigger_rows: Vec<(i32, usize, usize, usize)> = triggers
            .iter()
            .flat_map(|(filter_id, arr)| {
                arr.iter().map(move |t| (packet_id, *filter_id, t.start_index, t.end_index))
            })
            .collect();
        if !trigger_rows.is_empty() {
            debug!("Inserting {} triggers", trigger_rows.len());
            self.db.insert_triggers_batch(&trigger_rows).await
                .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Trigger batch insert failed: {:?}", e)))?;
        }

        // Try L7 HTTP parsing (only if service has http_parse enabled)
        let should_parse_http = {
            let services = self.cache.services.read().unwrap();
            services.get(&service_id).map(|s| s.http_parse).unwrap_or(false)
        };

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

            match self.db.insert_http_message(
                stream_id,
                packet_id,
                parsed.is_request,
                parsed.method.as_deref(),
                parsed.path.as_deref(),
                parsed.status_code,
                parsed.http_version.as_deref(),
                content_type.as_deref(),
                content_length,
                parsed.body_offset as i32,
                body_length,
                header_set_id,
                packet.timestamp_seconds as i64,
            ).await {
                Ok(msg_id) => {
                    // Detect gRPC and parse protobuf body
                    let is_grpc = content_type.as_deref()
                        .map(|ct| ct.starts_with("application/grpc"))
                        .unwrap_or(false);

                    if is_grpc {
                        let body = &packet.payload[parsed.body_offset..];
                        if let Some(grpc) = grpc_parser::try_parse_grpc_body(
                            body,
                            parsed.path.as_deref(),
                            parsed.is_request,
                        ) {
                            // Store decoded gRPC in grpc_message table
                            let _ = self.db.insert_grpc_message(
                                stream_id,
                                msg_id,
                                grpc.service_name.as_deref(),
                                grpc.method_name.as_deref(),
                                grpc.is_request,
                                &grpc.decoded_json,
                                packet.timestamp_seconds as i64,
                            ).await;
                            let _ = self.db.update_stream_l7_protocol(stream_id, "grpc").await;
                            debug!("gRPC parsed: {}/{}",
                                grpc.service_name.as_deref().unwrap_or("?"),
                                grpc.method_name.as_deref().unwrap_or("?"));
                        } else {
                            let _ = self.db.update_stream_l7_protocol(stream_id, "http2").await;
                        }
                    } else {
                        let _ = self.db.update_stream_l7_protocol(stream_id, "http1").await;
                    }

                    debug!("HTTP {} parsed: {} headers (set_id={:?})",
                        if parsed.is_request { "request" } else { "response" },
                        parsed.headers.len(), header_set_id
                    );
                }
                Err(e) => {
                    error!("Failed to insert HTTP message: {:?}", e);
                }
            }
        }
        } // should_parse_http

        // Send event to NATS for real-time updates
        if let Some(ref client) = self.nats_client {
            let event = PacketEvent {
                packet_id,
                stream_id,
                timestamp_seconds: packet.timestamp_seconds,
                timestamp_microseconds: packet.timestamp_microseconds,
                direction: match direction {
                    Direction::Inbound => "inbound".to_string(),
                    Direction::Outbound => "outbound".to_string(),
                },
            };
            
            match bincode::encode_to_vec(&event, config::standard()) {
                Ok(payload) => {
                    let topic = self.nats_topic.clone();
                    let client_clone = client.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client_clone.publish(topic, payload.into()).await {
                            error!("Failed to send packet event to NATS: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to encode packet event: {}", e);
                }
            }
        }

        Ok(())
    }
}

