use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use log::{debug, trace};
use std::sync::Arc;
use crate::cache::Cache;
use crate::configuration::updater::dto::FilterDirection;
use crate::configuration::ApplicationConfig;
use crate::db::DB;

#[derive(Debug, PartialEq)]
pub enum Direction {
    Inbound,
    Outbound
}

 pub struct PacketProcessor {
    cache: Arc<Cache>,
    config: Arc<ApplicationConfig>,
    db: DB
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB) -> Self {
        Self { cache, config, db }
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
            

        // First trying local cache
        // TODO: check the error
        if let Some(stream_id) = self.cache.streams.lock().unwrap().get(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port) {
            return Ok(stream_id);
        }

        // If not found, trying to find in database
        if let Some(stream_id) = self.db.get_stream_id(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port).await {
            return Ok(stream_id);
        }
        
        // If not found, creating new stream
        let stream_id = self.db.create_stream(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, &packet.protocol, &packet.ip_version, service_id).await
                                        .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Stream creation failed: {:?}", e)))?;
        
        // And insert into cache
        self.cache.streams.lock().unwrap().insert(&src_ip.to_string(), src_port, &dst_ip.to_string(), dst_port, stream_id);
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

        // Inserting triggers into database
        for (filter_id, triggers_array) in triggers {
            debug!("Inserting {} triggers for filter ID: {:?}", triggers_array.len(), filter_id);
            for trigger in triggers_array {
                self.db.insert_trigger(packet_id, filter_id, trigger.start_index, trigger.end_index).await
                    .map_err(|e| Error::new(std::io::ErrorKind::Other, format!("Trigger insertion failed: {:?}", e)))?;
            }
        }

        Ok(())
    }
}

