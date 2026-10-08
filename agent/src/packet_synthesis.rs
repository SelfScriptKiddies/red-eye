use std::net::IpAddr;

use harpoon_core::capture::CapturedPacket;

/// Rewrite localhost/unspecified IPs to the machine's real IP so the analyzer
/// can match the packet to the correct service.
fn rewrite_ip(ip: IpAddr, machine_ip: IpAddr) -> IpAddr {
    if ip.is_loopback() || ip.is_unspecified() {
        machine_ip
    } else {
        ip
    }
}

/// Synthesizes a raw Ethernet frame from a CapturedPacket so that the analyzer's
/// parse_packet() can process it without any changes.
///
/// TODO: Currently always generates TCP headers (protocol=6). UDP packets from
/// harpoon UDP proxy rules will have incorrect transport headers. Fixing this
/// requires adding protocol info to harpoon-core's CapturedPacket type.
pub fn synthesize_raw_frame(packet: &CapturedPacket, machine_ip: IpAddr) -> Vec<u8> {
    let src_ip = rewrite_ip(packet.src.ip(), machine_ip);
    let dst_ip = rewrite_ip(packet.dst.ip(), machine_ip);
    match (src_ip, dst_ip) {
        (IpAddr::V4(src_ip), IpAddr::V4(dst_ip)) => {
            let transport_header_len: usize = 20; // TCP header, no options
            let ip_total_len = (20 + transport_header_len + packet.payload.len()) as u16;
            let mut frame =
                Vec::with_capacity(14 + 20 + transport_header_len + packet.payload.len());

            // Ethernet header (14 bytes)
            frame.extend_from_slice(&[0u8; 6]); // dst MAC
            frame.extend_from_slice(&[0u8; 6]); // src MAC
            frame.extend_from_slice(&[0x08, 0x00]); // EtherType: IPv4

            // IPv4 header (20 bytes)
            frame.push(0x45); // Version 4, IHL 5
            frame.push(0x00); // DSCP/ECN
            frame.extend_from_slice(&ip_total_len.to_be_bytes());
            frame.extend_from_slice(&[0x00, 0x00]); // Identification
            frame.extend_from_slice(&[0x40, 0x00]); // Don't Fragment
            frame.push(64); // TTL
            frame.push(6); // Protocol: TCP
            frame.extend_from_slice(&[0x00, 0x00]); // Checksum (skip)
            frame.extend_from_slice(&src_ip.octets());
            frame.extend_from_slice(&dst_ip.octets());

            // TCP header (20 bytes)
            frame.extend_from_slice(&packet.src.port().to_be_bytes());
            frame.extend_from_slice(&packet.dst.port().to_be_bytes());
            frame.extend_from_slice(&[0x00; 4]); // Sequence number
            frame.extend_from_slice(&[0x00; 4]); // Ack number
            frame.push(0x50); // Data offset: 5 words (20 bytes)
            frame.push(0x18); // Flags: PSH + ACK
            frame.extend_from_slice(&[0xFF, 0xFF]); // Window
            frame.extend_from_slice(&[0x00, 0x00]); // Checksum
            frame.extend_from_slice(&[0x00, 0x00]); // Urgent pointer

            // Payload
            frame.extend_from_slice(&packet.payload);

            frame
        }
        (IpAddr::V6(src_ip), IpAddr::V6(dst_ip)) => {
            let payload_len = (20 + packet.payload.len()) as u16; // TCP header + payload
            let mut frame = Vec::with_capacity(14 + 40 + 20 + packet.payload.len());

            // Ethernet header
            frame.extend_from_slice(&[0u8; 6]); // dst MAC
            frame.extend_from_slice(&[0u8; 6]); // src MAC
            frame.extend_from_slice(&[0x86, 0xDD]); // EtherType: IPv6

            // IPv6 header (40 bytes)
            frame.push(0x60); // Version 6
            frame.extend_from_slice(&[0x00, 0x00, 0x00]); // Traffic Class + Flow Label
            frame.extend_from_slice(&payload_len.to_be_bytes());
            frame.push(6); // Next Header: TCP
            frame.push(64); // Hop Limit
            frame.extend_from_slice(&src_ip.octets()); // 16 bytes
            frame.extend_from_slice(&dst_ip.octets()); // 16 bytes

            // TCP header (20 bytes)
            frame.extend_from_slice(&packet.src.port().to_be_bytes());
            frame.extend_from_slice(&packet.dst.port().to_be_bytes());
            frame.extend_from_slice(&[0x00; 4]); // Sequence
            frame.extend_from_slice(&[0x00; 4]); // Ack
            frame.push(0x50);
            frame.push(0x18); // PSH + ACK
            frame.extend_from_slice(&[0xFF, 0xFF]); // Window
            frame.extend_from_slice(&[0x00, 0x00]); // Checksum
            frame.extend_from_slice(&[0x00, 0x00]); // Urgent

            frame.extend_from_slice(&packet.payload);

            frame
        }
        // Mixed v4/v6 — shouldn't happen, fall back to v4 with zeros
        _ => Vec::new(),
    }
}

/// Build the raw packet tuple for the NATS batching queue, in the same
/// shape the sniffer produces: (tv_usec: i64, tv_sec: i64,
/// raw_ethernet_bytes: Vec<u8>). The bincode envelope encoding happens per
/// batch inside the NATS client (see nats.rs RAW_PACKET_WIRE_VERSION).
pub fn encode_for_nats(packet: &CapturedPacket, machine_ip: IpAddr) -> Option<(i64, i64, Vec<u8>)> {
    let raw = synthesize_raw_frame(packet, machine_ip);
    if raw.is_empty() {
        return None;
    }

    let tv_sec = (packet.timestamp_ms / 1000) as i64;
    let tv_usec = ((packet.timestamp_ms % 1000) * 1000) as i64;

    Some((tv_usec, tv_sec, raw))
}
