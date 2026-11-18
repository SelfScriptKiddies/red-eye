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

    /// Build the passive-capture view of the SAME logical flow the
    /// harpoon proxy observed: a client frame arriving at the machine
    /// (dst = machine ip, dst port = original service port) and the
    /// response leaving it. The analyzer derives direction from the
    /// machine IP and normalizes the tuple with the local side first
    /// (traffic-analyzer process_packet.rs: normalized_tuple) - the
    /// synthesized frames must produce identical direction + tuple.
    fn passive_equivalent_frame(
        src_ip: [u8; 4],
        src_port: u16,
        dst_ip: [u8; 4],
        dst_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        let l4 = 8 + payload.len();
        let total = 20 + l4;
        let mut frame = vec![0u8; 14 + total];
        frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes());
        frame[14] = 0x45;
        frame[16..18].copy_from_slice(&(total as u16).to_be_bytes());
        frame[23] = 17; // UDP
        frame[26..30].copy_from_slice(&src_ip);
        frame[30..34].copy_from_slice(&dst_ip);
        frame[34..36].copy_from_slice(&src_port.to_be_bytes());
        frame[36..38].copy_from_slice(&dst_port.to_be_bytes());
        frame[38..40].copy_from_slice(&(l4 as u16).to_be_bytes());
        frame[42..42 + payload.len()].copy_from_slice(payload);
        frame
    }

    /// Extract (src, dst) from a synthesized or passive frame the way the
    /// analyzer's parser does, plus the UDP length field.
    fn endpoints(frame: &[u8]) -> ([u8; 4], u16, [u8; 4], u16) {
        let src = [frame[26], frame[27], frame[28], frame[29]];
        let dst = [frame[30], frame[31], frame[32], frame[33]];
        let src_port = u16::from_be_bytes([frame[34], frame[35]]);
        let dst_port = u16::from_be_bytes([frame[36], frame[37]]);
        (src, src_port, dst, dst_port)
    }

    #[test]
    fn synthesized_frames_match_passive_direction_and_tuple() {
        let machine_ip: IpAddr = "10.0.0.5".parse().unwrap();

        // Harpoon capture: client 10.0.0.9:40000 -> target 127.0.0.1:80
        // (ClientToServer; target endpoint is rewritten to the machine IP
        // because the service runs on the local machine).
        let request = captured(b"GET /");
        let syn = synthesize_raw_frame(&request, machine_ip, TransportProto::Udp);
        let (s_ip, s_port, d_ip, d_port) = endpoints(&syn);
        assert_eq!(d_port, 8080, "target port preserved for attribution");
        assert_eq!(&s_ip, &[10, 0, 0, 9]);
        assert_eq!(
            &d_ip,
            &[10, 0, 0, 5],
            "loopback target rewritten to machine IP"
        );

        // Passive view of the same request on the wire.
        let passive =
            passive_equivalent_frame([10, 0, 0, 9], s_port, [10, 0, 0, 5], d_port, b"GET /");

        // Direction: the endpoint matching the machine IP is the local
        // side; here dst == machine => Inbound for both frames.
        let direction_of = |frame: &[u8]| {
            let (_s, _, d, _) = endpoints(frame);
            if d == [10, 0, 0, 5] {
                "inbound"
            } else {
                "outbound"
            }
        };
        // Normalized tuple: local side first (analyzer semantics).
        let normalized = |frame: &[u8]| {
            let (s, sp, d, dp) = endpoints(frame);
            if d == [10, 0, 0, 5] {
                (d, dp, s, sp)
            } else {
                (s, sp, d, dp)
            }
        };
        assert_eq!(direction_of(&syn), direction_of(&passive));
        assert_eq!(normalized(&syn), normalized(&passive));

        // Server-to-client capture: harpoon records src = target (the
        // service endpoint), dst = client.
        let mut response = captured(b"HTTP");
        response.direction = harpoon_core::capture::PacketDirection::ServerToClient;
        response.src = "127.0.0.1:8080".parse().unwrap();
        response.dst = "10.0.0.9:40000".parse().unwrap();
        let syn_reply = synthesize_raw_frame(&response, machine_ip, TransportProto::Udp);
        assert_eq!(direction_of(&syn_reply), "outbound");
        assert_eq!(
            normalized(&syn_reply),
            normalized(&passive_equivalent_frame(
                [10, 0, 0, 5],
                8080,
                [10, 0, 0, 9],
                s_port,
                b"HTTP"
            ))
        );
    }

    /// The synthesized frame's UDP length and IP total length always match
    /// the payload actually published (truncation included).
    #[test]
    fn synthesized_udp_lengths_consistent_with_published_bytes() {
        for size in [0usize, 1, 64, 1400, 65_000, 80_000] {
            let payload = vec![0xAB; size];
            let frame = synthesize_raw_frame(
                &captured(&payload),
                "10.0.0.5".parse().unwrap(),
                TransportProto::Udp,
            );
            let total = u16::from_be_bytes([frame[16], frame[17]]) as usize;
            let udp_len = u16::from_be_bytes([frame[14 + 20 + 4], frame[14 + 20 + 5]]) as usize;
            assert_eq!(udp_len, 8 + frame.len() - 14 - 20 - 8);
            assert_eq!(total, frame.len() - 14);
            assert_eq!(frame.len() - 14, total);
        }
    }
}
