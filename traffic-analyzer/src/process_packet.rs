use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use log::{debug, trace};
use std::sync::Arc;
use crate::cache::Cache;

 pub struct PacketProcessor {
    pub cache: Arc<Cache>
 }

impl PacketProcessor {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }

    pub fn process_packet(&self, packet: PacketInfo) -> Result<(), Error> {
        debug!("Parsed packet: {:?}", packet);
        let triggers = apply_filters(&packet.payload);
        trace!("Triggers: {:?}", triggers);
        Ok(())
    }
}

