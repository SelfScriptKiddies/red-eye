use log::{debug, error};
use pnet::packet::Packet as PnetPacket;
use pnet::packet::{ethernet::{EthernetPacket, EtherTypes}, 
                  ip::IpNextHeaderProtocols,
                  ipv4::Ipv4Packet, 
                  ipv6::Ipv6Packet,
                  tcp::{TcpPacket, TcpFlags}, 
                  udp::UdpPacket};
use std::net::IpAddr;

/// Structure for storing packet information
#[derive(Debug, Clone)]
pub struct PacketInfo {
    pub payload: Vec<u8>,
    pub timestamp: u64,  // timestamp in microseconds
    pub src_ip: Option<IpAddr>,
    pub src_port: Option<u16>,
    pub dst_ip: Option<IpAddr>,
    pub dst_port: Option<u16>,
}

/// Processes a pcap packet, extracting the payload and metadata
pub fn parse_packet(packet: Vec<u8>) -> Option<PacketInfo> {
    // Convert timestamp
    // let timestamp = (packet.header.ts.tv_sec as u64) * 1_000_000 + (packet.header.ts.tv_usec as u64);
    let timestamp = 0;
    
    // Get Ethernet frame
    if let Some(ethernet_packet) = EthernetPacket::new(&packet) {
        match ethernet_packet.get_ethertype() {
            EtherTypes::Ipv4 => {
                return process_ipv4_packet(ethernet_packet.payload(), timestamp);
            },
            EtherTypes::Ipv6 => {
                return process_ipv6_packet(ethernet_packet.payload(), timestamp);
            },
            _ => {
                debug!("Неподдерживаемый тип Ethernet-пакета: {:?}", ethernet_packet.get_ethertype());
                return None;
            }
        }
    } else {
        error!("Не удалось создать Ethernet-пакет");
        return None;
    }
}

/// Processing IPv4 packet
fn process_ipv4_packet(packet_data: &[u8], timestamp: u64) -> Option<PacketInfo> {
    if let Some(ipv4_packet) = Ipv4Packet::new(packet_data) {
        let src_ip = IpAddr::V4(ipv4_packet.get_source());
        let dst_ip = IpAddr::V4(ipv4_packet.get_destination());
        
        match ipv4_packet.get_next_level_protocol() {
            IpNextHeaderProtocols::Tcp => {
                return process_tcp_packet(ipv4_packet.payload(), timestamp, src_ip, dst_ip);
            },
            IpNextHeaderProtocols::Udp => {
                return process_udp_packet(ipv4_packet.payload(), timestamp, src_ip, dst_ip);
            },
            _ => {
                debug!("Неподдерживаемый протокол следующего уровня: {:?}", 
                       ipv4_packet.get_next_level_protocol());
                return None;
            }
        }
    } else {
        error!("Не удалось создать IPv4-пакет");
        return None;
    }
}

/// Processing IPv6 packet
fn process_ipv6_packet(packet_data: &[u8], timestamp: u64) -> Option<PacketInfo> {
    if let Some(ipv6_packet) = Ipv6Packet::new(packet_data) {
        let src_ip = IpAddr::V6(ipv6_packet.get_source());
        let dst_ip = IpAddr::V6(ipv6_packet.get_destination());
        
        match ipv6_packet.get_next_header() {
            IpNextHeaderProtocols::Tcp => {
                return process_tcp_packet(ipv6_packet.payload(), timestamp, src_ip, dst_ip);
            },
            IpNextHeaderProtocols::Udp => {
                return process_udp_packet(ipv6_packet.payload(), timestamp, src_ip, dst_ip);
            },
            _ => {
                debug!("Неподдерживаемый протокол следующего уровня IPv6: {:?}", 
                       ipv6_packet.get_next_header());
                return None;
            }
        }
    } else {
        error!("Не удалось создать IPv6-пакет");
        return None;
    }
}

/// Processing TCP packet
fn process_tcp_packet(packet_data: &[u8], timestamp: u64, src_ip: IpAddr, dst_ip: IpAddr) -> Option<PacketInfo> {
    if let Some(tcp_packet) = TcpPacket::new(packet_data) {
        let src_port = tcp_packet.get_source();
        let dst_port = tcp_packet.get_destination();
        let payload = tcp_packet.payload().to_vec();

        // Check for RST or FIN/ACK flags
        if tcp_packet.get_flags() & TcpFlags::RST != 0 || 
           (tcp_packet.get_flags() & TcpFlags::FIN != 0 && tcp_packet.get_flags() & TcpFlags::ACK != 0) {
            unimplemented!("Stream closing not implemented yet");
        }

        // Return information only if there is a payload
        if !payload.is_empty() {
            return Some(PacketInfo {
                payload,
                timestamp,
                src_ip: Some(src_ip),
                src_port: Some(src_port),
                dst_ip: Some(dst_ip),
                dst_port: Some(dst_port),
            });
        }
    } else {
        error!("Не удалось создать TCP-пакет");
    }
    None
}

/// Processing UDP packet
fn process_udp_packet(packet_data: &[u8], timestamp: u64, src_ip: IpAddr, dst_ip: IpAddr) -> Option<PacketInfo> {
    if let Some(udp_packet) = UdpPacket::new(packet_data) {
        let src_port = udp_packet.get_source();
        let dst_port = udp_packet.get_destination();
        let payload = udp_packet.payload().to_vec();
        
        // Return information only if there is a payload
        if !payload.is_empty() {
            return Some(PacketInfo {
                payload,
                timestamp,
                src_ip: Some(src_ip),
                src_port: Some(src_port),
                dst_ip: Some(dst_ip),
                dst_port: Some(dst_port),
            });
        }
    } else {
        error!("Не удалось создать UDP-пакет");
    }
    None
}
