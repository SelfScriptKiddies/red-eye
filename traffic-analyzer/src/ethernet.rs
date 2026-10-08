use log::{debug, error};
use pcap::Packet as PcapPacket;
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

/// Raw TCP header flags (pnet TcpFlags bit mask). Always 0 for UDP.
/// Kept per-packet so the stream lifecycle can react to SYN/FIN/RST
/// without re-parsing; absent flags (0) degrade to the pre-lifecycle
/// tuple+activity behavior.
pub type TcpFlagsBits = u8;

#[derive(Debug, Clone)]
pub struct PacketInfo {
    pub payload: Vec<u8>,
    pub timestamp_seconds: u64,  // seconds
    pub timestamp_microseconds: u64, // microseconds
    pub src_ip: IpAddr,
    pub src_port: u16,
    pub dst_ip: IpAddr,
    pub dst_port: u16,
    pub protocol: Protocol,
    pub ip_version: IpVersion,
    pub stream_id: Option<usize>,
    pub tcp_flags: TcpFlagsBits,
}

/// Plain SYN (SYN without ACK) opens a TCP connection: the initiator's
/// first packet. SYN+ACK is a handshake reply, NOT a new connection —
/// treating it as one would split every handshake into two streams
/// (direction normalization maps both directions onto the same tuple key).
pub fn tcp_is_new_connection_syn(flags: TcpFlagsBits) -> bool {
    (flags & TcpFlags::SYN != 0) && (flags & TcpFlags::ACK == 0)
}

/// FIN or RST closes (or aborts) a TCP connection. Either direction of
/// the close maps to the same normalized tuple.
pub fn tcp_is_connection_close(flags: TcpFlagsBits) -> bool {
    flags & (TcpFlags::FIN | TcpFlags::RST) != 0
}

/// Processes a pcap packet, extracting the payload and metadata
pub fn parse_packet(timestamp: (u64, u64), packet: Vec<u8>) -> Option<PacketInfo> {
    // Convert timestamp
    // let timestamp = 0;
    
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
fn process_ipv4_packet(packet_data: &[u8], timestamp: (u64, u64)) -> Option<PacketInfo> {
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
fn process_ipv6_packet(packet_data: &[u8], timestamp: (u64, u64)) -> Option<PacketInfo> {
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
fn process_tcp_packet(packet_data: &[u8], timestamp: (u64, u64), src_ip: IpAddr, dst_ip: IpAddr, ip_version: IpVersion) -> Option<PacketInfo> {
    if let Some(tcp_packet) = TcpPacket::new(packet_data) {
        let src_port = tcp_packet.get_source();
        let dst_port = tcp_packet.get_destination();
        let payload = tcp_packet.payload().to_vec();
        let flags = tcp_packet.get_flags();

        // Data packets always pass through. Control packets (plain SYN that
        // opens a connection, FIN/RST that closes one) pass even without
        // payload so the stream lifecycle can observe them; the processor
        // drops them before the data path. Pure ACKs, SYN+ACK and other
        // empty non-control packets are still skipped (same behavior as
        // before this existed).
        let is_control = tcp_is_new_connection_syn(flags) || tcp_is_connection_close(flags);

        if !payload.is_empty() || is_control {
            return Some(PacketInfo {
                payload,
                timestamp_seconds: timestamp.1,
                timestamp_microseconds: timestamp.0,
                src_ip: src_ip,
                src_port: src_port,
                dst_ip: dst_ip,
                dst_port: dst_port,
                protocol: Protocol::TCP,
                ip_version: ip_version,
                stream_id: None,
                tcp_flags: flags,
            });
        }
    } else {
        error!("Не удалось создать TCP-пакет");
    }
    None
}

/// Processing UDP packet
fn process_udp_packet(packet_data: &[u8], timestamp: (u64, u64), src_ip: IpAddr, dst_ip: IpAddr, ip_version: IpVersion) -> Option<PacketInfo> {
    if let Some(udp_packet) = UdpPacket::new(packet_data) {
        let src_port = udp_packet.get_source();
        let dst_port = udp_packet.get_destination();
        let payload = udp_packet.payload().to_vec();
        
        // Return information only if there is a payload
        if !payload.is_empty() {
            return Some(PacketInfo {
                payload,
                timestamp_seconds: timestamp.1,
                timestamp_microseconds: timestamp.0,
                src_ip: src_ip,
                src_port: src_port,
                dst_ip: dst_ip,
                dst_port: dst_port,
                protocol: Protocol::UDP,
                ip_version: ip_version,
                stream_id: None,
                tcp_flags: 0,
            });
        }
    } else {
        error!("Не удалось создать UDP-пакет");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Raw TCP flag bits (RFC 9293): FIN=0x01, SYN=0x02, RST=0x04, ACK=0x10

    #[test]
    fn plain_syn_is_new_connection() {
        assert!(tcp_is_new_connection_syn(0x02));
    }

    #[test]
    fn syn_ack_is_not_new_connection() {
        // Treating SYN+ACK as a new connection would split every handshake
        // into two streams: both directions normalize onto one tuple key.
        assert!(!tcp_is_new_connection_syn(0x02 | 0x10));
    }

    #[test]
    fn ack_fin_rst_plain_are_not_new_connection() {
        assert!(!tcp_is_new_connection_syn(0x10));
        assert!(!tcp_is_new_connection_syn(0x01));
        assert!(!tcp_is_new_connection_syn(0x04));
        assert!(!tcp_is_new_connection_syn(0x00));
    }

    #[test]
    fn fin_and_rst_close_connection() {
        assert!(tcp_is_connection_close(0x01));        // FIN
        assert!(tcp_is_connection_close(0x01 | 0x10)); // FIN+ACK
        assert!(tcp_is_connection_close(0x04));        // RST
        assert!(tcp_is_connection_close(0x04 | 0x10)); // RST+ACK
    }

    #[test]
    fn syn_ack_data_do_not_close_connection() {
        assert!(!tcp_is_connection_close(0x02));
        assert!(!tcp_is_connection_close(0x02 | 0x10));
        assert!(!tcp_is_connection_close(0x10));
        assert!(!tcp_is_connection_close(0x00));
    }

    #[test]
    fn control_only_packets_are_emitted_from_parse() {
        use pnet::packet::tcp::MutableTcpPacket;

        // Build a minimal Ethernet + IPv4 + TCP frame with a plain SYN and
        // no payload; parse_packet must return it (with flags), while a pure
        // ACK with no payload must be dropped as before.
        let build = |flags: u8| -> Vec<u8> {
            let tcp_len = 20;
            let mut buf = vec![0u8; 14 + 20 + tcp_len];
            // Ethernet: IPv4 ethertype
            buf[12] = 0x08; buf[13] = 0x00;
            // IPv4 header
            buf[14] = 0x45;                       // version + IHL
            buf[16] = ((20 + tcp_len) >> 8) as u8; // total length
            buf[17] = (20 + tcp_len) as u8;
            buf[23] = 0x06;                       // protocol = TCP
            // src/dst IP are left 0.0.0.0/0.0.0.0 — fine for parsing
            let mut tcp = MutableTcpPacket::new(&mut buf[14 + 20..]).unwrap();
            tcp.set_source(12345);
            tcp.set_destination(80);
            tcp.set_data_offset(5);
            tcp.set_flags(flags);
            buf
        };

        // Plain SYN, no payload -> emitted
        let syn = parse_packet((0, 1_700_000_000), build(0x02)).expect("plain SYN must be parsed");
        assert_eq!(syn.src_port, 12345);
        assert_eq!(syn.tcp_flags, 0x02);
        assert!(tcp_is_new_connection_syn(syn.tcp_flags));
        assert!(syn.payload.is_empty());

        // FIN+ACK, no payload -> emitted
        let fin = parse_packet((0, 1_700_000_000), build(0x11)).expect("FIN+ACK must be parsed");
        assert!(tcp_is_connection_close(fin.tcp_flags));
        assert!(fin.payload.is_empty());

        // Pure ACK, no payload -> dropped (unchanged pre-existing behavior)
        assert!(parse_packet((0, 1_700_000_000), build(0x10)).is_none());
    }
}
