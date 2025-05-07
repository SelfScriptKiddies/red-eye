use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use log::{debug, trace};
use std::sync::Arc;
use crate::cache::Cache;
use crate::configuration::updater::dto::FilterDirection;
use crate::configuration::ApplicationConfig;

#[derive(Debug)]
enum Direction {
    Inbound,
    Outbound
}

 pub struct PacketProcessor {
    cache: Arc<Cache>,
    config: Arc<ApplicationConfig>
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>, config: Arc<ApplicationConfig>) -> Self {
        Self { cache, config }
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
    
    fn find_stream_id(&self, packet: &PacketInfo) -> Result<usize, Error> {
        // TODO: Implement this
        Ok(0)
    }

    pub fn process_packet(&self, packet: PacketInfo) -> Result<(), Error> {
        // Find service and direction
        let (service_id, direction) = self.find_service_and_direction(&packet)
                                                            .ok_or(Error::new(std::io::ErrorKind::Other, "Service not found"))?;

        debug!("Processing packet: \nService ID: {:?}\nDirection: {:?}", service_id, direction);
        
        let triggers = apply_filters(&packet.payload);
        trace!("Triggers: {:?}", triggers);
        Ok(())
    }
}

