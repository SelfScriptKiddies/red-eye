use std::io::Error;
use crate::ethernet::PacketInfo;
use crate::filters::apply_filters;
use log::{debug, trace};


pub fn process_packet(packet: PacketInfo) -> Result<(), Error> {
    debug!("Parsed packet: {:?}", packet);
    let triggers = apply_filters(&packet.payload);
    trace!("Triggers: {:?}", triggers);
    Ok(())
}