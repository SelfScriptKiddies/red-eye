use log::{debug, error};
use pnet::packet::Packet as PnetPacket;
use pnet::packet::{ethernet::{EthernetPacket, EtherTypes}, 
                  ip::IpNextHeaderProtocols,
                  ipv4::Ipv4Packet, 
                  ipv6::Ipv6Packet,
                  tcp::{TcpPacket, TcpFlags}, 
                  udp::UdpPacket};
use std::net::IpAddr;
use strum_macros::IntoStaticStr;

#[derive(Debug, Clone, IntoStaticStr)]
pub enum Protocol {
    TCP,
    UDP
}

#[derive(Debug, Clone, IntoStaticStr)]
pub enum IpVersion {
    IPv4,
    IPv6
}

/// Structure for storing packet information
#[derive(Debug, Clone)]
pub struct PacketInfo {
    pub payload: Vec<u8>,
    pub timestamp: u64,  // timestamp in microseconds
    pub src_ip: IpAddr,
    pub src_port: u16,
    pub dst_ip: IpAddr,
    pub dst_port: u16,
    pub protocol: Protocol,
    pub ip_version: IpVersion,
    pub stream_id: Option<usize>,
    pub stream_closing: bool,
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
                debug!("Unsupported Ethernet packet type: {:?}", ethernet_packet.get_ethertype());
                return None;
            }
        }
    } else {
        error!("Failed to create Ethernet packet");
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
                return process_tcp_packet(ipv4_packet.payload(), timestamp, src_ip, dst_ip, IpVersion::IPv4);
            },
            IpNextHeaderProtocols::Udp => {
                return process_udp_packet(ipv4_packet.payload(), timestamp, src_ip, dst_ip, IpVersion::IPv4);
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
                return process_tcp_packet(ipv6_packet.payload(), timestamp, src_ip, dst_ip, IpVersion::IPv6);
            },
            IpNextHeaderProtocols::Udp => {
                return process_udp_packet(ipv6_packet.payload(), timestamp, src_ip, dst_ip, IpVersion::IPv6);
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
fn process_tcp_packet(packet_data: &[u8], timestamp: u64, src_ip: IpAddr, dst_ip: IpAddr, ip_version: IpVersion) -> Option<PacketInfo> {
    if let Some(tcp_packet) = TcpPacket::new(packet_data) {
        let src_port = tcp_packet.get_source();
        let dst_port = tcp_packet.get_destination();
        let payload = tcp_packet.payload().to_vec();
        let mut closing = false;

        // Check for RST or FIN/ACK flags
        if tcp_packet.get_flags() & TcpFlags::RST != 0 || 
           (tcp_packet.get_flags() & TcpFlags::FIN != 0 && tcp_packet.get_flags() & TcpFlags::ACK != 0) {
            closing = true;
        }

        // Return information only if there is a payload
        if !payload.is_empty() {
            return Some(PacketInfo {
                payload,
                timestamp,
                src_ip: src_ip,
                src_port: src_port,
                dst_ip: dst_ip,
                dst_port: dst_port,
                protocol: Protocol::TCP,
                ip_version: ip_version,
                stream_id: None,
                stream_closing: closing,
            });
        }
    } else {
        error!("Не удалось создать TCP-пакет");
    }
    None
}

/// Processing UDP packet
fn process_udp_packet(packet_data: &[u8], timestamp: u64, src_ip: IpAddr, dst_ip: IpAddr, ip_version: IpVersion) -> Option<PacketInfo> {
    if let Some(udp_packet) = UdpPacket::new(packet_data) {
        let src_port = udp_packet.get_source();
        let dst_port = udp_packet.get_destination();
        let payload = udp_packet.payload().to_vec();
        
        // Return information only if there is a payload
        if !payload.is_empty() {
            return Some(PacketInfo {
                payload,
                timestamp,
                src_ip: src_ip,
                src_port: src_port,
                dst_ip: dst_ip,
                dst_port: dst_port,
                protocol: Protocol::UDP,
                ip_version: ip_version,
                stream_id: None,
                stream_closing: false,
            });
        }
    } else {
        error!("Не удалось создать UDP-пакет");
    }
    None
}
