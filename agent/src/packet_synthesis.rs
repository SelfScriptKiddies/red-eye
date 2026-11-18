use std::net::IpAddr;

use harpoon_core::capture::CapturedPacket;

use crate::updates::TransportProto;

/// Wildcard machine IPs (0.0.0.0 / ::) mean "preserve observed endpoint
/// addresses": rewriting them into the unspecified address would produce
/// an invalid flow tuple on the analyzer side.
fn rewrite_ip(ip: IpAddr, machine_ip: IpAddr) -> IpAddr {
    if machine_ip.is_unspecified() {
        ip
    } else if ip.is_loopback() || ip.is_unspecified() {
        machine_ip
    } else {
        ip
    }
}

/// Build a UDP header (8 bytes). Length covers header + payload; UDP over
/// IPv4 tolerates a zero checksum field, so it stays consistent without a
/// pseudo-header pass the analyzer never performs.
fn push_udp_header(frame: &mut Vec<u8>, src_port: u16, dst_port: u16, payload_len: usize) {
    let udp_len = (8 + payload_len) as u16;
    frame.extend_from_slice(&src_port.to_be_bytes());
    frame.extend_from_slice(&dst_port.to_be_bytes());
    frame.extend_from_slice(&udp_len.to_be_bytes());
    frame.extend_from_slice(&[0x00, 0x00]); // Checksum (IPv4 allows zero)
}

/// Build a TCP header (20 bytes, no options).
fn push_tcp_header(frame: &mut Vec<u8>, src_port: u16, dst_port: u16) {
    frame.extend_from_slice(&src_port.to_be_bytes());
    frame.extend_from_slice(&dst_port.to_be_bytes());
    frame.extend_from_slice(&[0x00; 4]); // Sequence number
    frame.extend_from_slice(&[0x00; 4]); // Ack number
    frame.push(0x50); // Data offset: 5 words (20 bytes)
    frame.push(0x18); // Flags: PSH + ACK
    frame.extend_from_slice(&[0xFF, 0xFF]); // Window
    frame.extend_from_slice(&[0x00, 0x00]); // Checksum
    frame.extend_from_slice(&[0x00, 0x00]); // Urgent pointer
}

/// Synthesizes a raw Ethernet frame from a CapturedPacket so that the analyzer's
/// parse_packet() can process it without any changes. `protocol` selects the
/// transport header: a captured UDP proxy unit must travel with a UDP header,
/// never a hardcoded TCP one.
pub fn synthesize_raw_frame(
    packet: &CapturedPacket,
    machine_ip: IpAddr,
    protocol: TransportProto,
) -> Vec<u8> {
    let src_ip = rewrite_ip(packet.src.ip(), machine_ip);
    let dst_ip = rewrite_ip(packet.dst.ip(), machine_ip);
    match (src_ip, dst_ip) {
        (IpAddr::V4(src_ip), IpAddr::V4(dst_ip)) => {
            let transport_header_len: usize = match protocol {
                TransportProto::Tcp => 20,
                TransportProto::Udp => 8,
            };
            // Payload is capped so the IPv4 total length and the UDP
            // length field stay inside 16 bits and remain consistent with
            // the actual bytes written.
            let max_payload = u16::MAX as usize - 20 - transport_header_len;
            let payload = &packet.payload[..packet.payload.len().min(max_payload)];
            let ip_total_len = (20 + transport_header_len + payload.len()) as u16;
            let mut frame = Vec::with_capacity(14 + 20 + transport_header_len + payload.len());

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
            frame.push(match protocol {
                TransportProto::Tcp => 6,
                TransportProto::Udp => 17,
            });
            frame.extend_from_slice(&[0x00, 0x00]); // Checksum (skip)
            frame.extend_from_slice(&src_ip.octets());
            frame.extend_from_slice(&dst_ip.octets());

            match protocol {
                TransportProto::Tcp => {
                    push_tcp_header(&mut frame, packet.src.port(), packet.dst.port())
                }
                TransportProto::Udp => push_udp_header(
                    &mut frame,
                    packet.src.port(),
                    packet.dst.port(),
                    payload.len(),
                ),
            }

            frame.extend_from_slice(payload);
            frame
        }
        (IpAddr::V6(src_ip), IpAddr::V6(dst_ip)) => {
            let transport_header_len: usize = match protocol {
                TransportProto::Tcp => 20,
                TransportProto::Udp => 8,
            };
            let max_payload = u16::MAX as usize - transport_header_len;
            let payload = &packet.payload[..packet.payload.len().min(max_payload)];
            let payload_len = (transport_header_len + payload.len()) as u16;
            let mut frame = Vec::with_capacity(14 + 40 + transport_header_len + payload.len());

            // Ethernet header
            frame.extend_from_slice(&[0u8; 6]); // dst MAC
            frame.extend_from_slice(&[0u8; 6]); // src MAC
            frame.extend_from_slice(&[0x86, 0xDD]); // EtherType: IPv6

            // IPv6 header (40 bytes)
            frame.push(0x60); // Version 6
            frame.extend_from_slice(&[0x00, 0x00, 0x00]); // Traffic Class + Flow Label
            frame.extend_from_slice(&payload_len.to_be_bytes());
            frame.push(match protocol {
                TransportProto::Tcp => 6,
                TransportProto::Udp => 17,
            }); // Next Header
            frame.push(64); // Hop Limit
            frame.extend_from_slice(&src_ip.octets()); // 16 bytes
            frame.extend_from_slice(&dst_ip.octets()); // 16 bytes

            match protocol {
                TransportProto::Tcp => {
                    push_tcp_header(&mut frame, packet.src.port(), packet.dst.port())
                }
                TransportProto::Udp => push_udp_header(
                    &mut frame,
                    packet.src.port(),
                    packet.dst.port(),
                    payload.len(),
                ),
            }

            frame.extend_from_slice(payload);
            frame
        }
        // Mixed v4/v6 — shouldn't happen, fall back to empty (dropped)
        _ => Vec::new(),
    }
}

/// Build the raw packet tuple for the NATS batching queue, in the same
/// shape the sniffer produces: (tv_usec: i64, tv_sec: i64,
/// raw_ethernet_bytes: Vec<u8>). The protobuf envelope encoding happens per
/// batch inside the NATS client (see nats.rs RAW_PACKET_WIRE_VERSION).
pub fn encode_for_nats(
    packet: &CapturedPacket,
    machine_ip: IpAddr,
    protocol: TransportProto,
) -> Option<(i64, i64, Vec<u8>)> {
    let raw = synthesize_raw_frame(packet, machine_ip, protocol);
    if raw.is_empty() {
        return None;
    }

    let tv_sec = (packet.timestamp_ms / 1000) as i64;
    let tv_usec = ((packet.timestamp_ms % 1000) * 1000) as i64;

    Some((tv_usec, tv_sec, raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn captured(payload: &[u8]) -> CapturedPacket {
        CapturedPacket {
            timestamp_ms: 1_700_000_000_123,
            rule_name: "test".into(),
            direction: harpoon_core::capture::PacketDirection::ClientToServer,
            src: "10.0.0.9:40000".parse::<SocketAddr>().unwrap(),
            dst: "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            payload_len: payload.len(),
            payload: bytes::Bytes::copy_from_slice(payload),
            filter_matched: None,
            was_dropped: false,
        }
    }

    fn ipv4_fields(frame: &[u8]) -> (usize, u8, usize) {
        // (total_len_field, protocol_byte, offset of the L4 header)
        (
            u16::from_be_bytes([frame[16], frame[17]]) as usize,
            frame[23],
            14 + 20,
        )
    }

    #[test]
    fn tcp_synthesis_unchanged_shape() {
        let frame = synthesize_raw_frame(
            &captured(b"hello"),
            "10.0.0.5".parse().unwrap(),
            TransportProto::Tcp,
        );
        assert!(!frame.is_empty());
        let (total_len, proto, l4) = ipv4_fields(&frame);
        assert_eq!(proto, 6);
        assert_eq!(total_len as usize, 20 + 20 + 5);
        // Payload rides after the 20-byte TCP header.
        assert_eq!(&frame[l4 + 20..l4 + 25], b"hello");
        // observed src 10.0.0.9 is preserved; the loopback dst is rewritten
        // to the machine IP.
        assert_eq!(&frame[26..30], &[10, 0, 0, 9]);
        assert_eq!(&frame[30..34], &[10, 0, 0, 5]);
        // ports ride in the TCP header
        assert_eq!(&frame[l4..l4 + 2], &40000u16.to_be_bytes());
        assert_eq!(&frame[l4 + 2..l4 + 4], &8080u16.to_be_bytes());
    }

    #[test]
    fn udp_synthesis_emits_valid_udp_header() {
        let frame = synthesize_raw_frame(
            &captured(b"hi"),
            "10.0.0.5".parse().unwrap(),
            TransportProto::Udp,
        );
        assert!(!frame.is_empty());
        let (total_len, proto, l4) = ipv4_fields(&frame);
        // IP protocol 17 = UDP, never the hardcoded TCP 6.
        assert_eq!(proto, 17);
        assert_eq!(total_len as usize, 20 + 8 + 2);

        let udp_len = u16::from_be_bytes([frame[l4 + 4], frame[l4 + 5]]);
        assert_eq!(
            udp_len as usize,
            8 + 2,
            "UDP length must cover header + payload"
        );
        assert_eq!(&frame[l4..l4 + 2], &40000u16.to_be_bytes());
        assert_eq!(&frame[l4 + 2..l4 + 4], &8080u16.to_be_bytes());
        assert_eq!(&frame[l4 + 8..l4 + 10], b"hi");
        assert_eq!(
            frame.len(),
            14 + total_len as usize,
            "wire bytes must match the declared IP length"
        );
    }

    #[test]
    fn udp_length_is_consistent_for_large_payloads() {
        let payload = vec![0xAB; 200];
        let frame = synthesize_raw_frame(
            &captured(&payload),
            "10.0.0.5".parse().unwrap(),
            TransportProto::Udp,
        );
        let (total_len, _, l4) = ipv4_fields(&frame);
        let udp_len = u16::from_be_bytes([frame[l4 + 4], frame[l4 + 5]]);
        assert_eq!(udp_len as usize, 8 + 200);
        assert_eq!(total_len as usize, 20 + udp_len as usize);
        assert_eq!(frame.len(), 14 + total_len as usize);
    }

    #[test]
    fn wildcard_machine_ip_preserves_observed_endpoints() {
        let packet = captured(b"x");
        let frame = synthesize_raw_frame(&packet, "0.0.0.0".parse().unwrap(), TransportProto::Udp);
        let l4 = 14 + 20;
        // src stays the observed 10.0.0.9; dst stays the observed loopback
        // instead of being replaced by the unspecified address.
        assert_eq!(&frame[26..30], &[10, 0, 0, 9]);
        assert_eq!(&frame[30..34], &[127, 0, 0, 1]);
        assert_eq!(&frame[l4 + 8..], b"x");

        let frame6 = synthesize_raw_frame(&packet, "::".parse().unwrap(), TransportProto::Tcp);
        assert!(!frame6.is_empty());
        assert_eq!(&frame6[26..30], &[10, 0, 0, 9]);
    }

    #[test]
    fn oversized_payload_is_truncated_with_consistent_lengths() {
        let payload = vec![0u8; 80_000];
        let frame = synthesize_raw_frame(
            &captured(&payload),
            "10.0.0.5".parse().unwrap(),
            TransportProto::Udp,
        );
        let (total_len, _, l4) = ipv4_fields(&frame);
        assert!(total_len <= u16::MAX as usize);
        let udp_len = u16::from_be_bytes([frame[l4 + 4], frame[l4 + 5]]);
        assert_eq!(total_len as usize, 20 + udp_len as usize);
        assert_eq!(frame.len(), 14 + total_len as usize);
    }

    #[test]
    fn encode_for_nats_keeps_timestamps() {
        let encoded = encode_for_nats(
            &captured(b"abc"),
            "10.0.0.5".parse().unwrap(),
            TransportProto::Udp,
        )
        .unwrap();
        assert_eq!(encoded.0, 123_000);
        assert_eq!(encoded.1, 1_700_000_000);
        assert!(!encoded.2.is_empty());
    }
}
