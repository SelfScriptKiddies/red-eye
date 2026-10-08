use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use log::{debug, error, info, trace, warn};
use pnet::datalink::{self, Channel, Config as DatalinkConfig};
use pnet::packet::ethernet::{EtherTypes, EthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocol;
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::Ipv4Packet;
use pnet::packet::ipv6::Ipv6Packet;
use pnet::packet::tcp::TcpPacket;
use pnet::packet::udp::UdpPacket;
use pnet::packet::vlan::VlanPacket;
use pnet::packet::Packet;

use crate::config::AgentConfig;
use crate::nats::NatsClient;
use crate::updates::PortsCache;

/// Shared sniffer health, reported via heartbeat.
#[derive(Debug, Default)]
pub struct SnifferHealth {
    /// Whether the sniffer capture thread is currently running.
    active: AtomicBool,
    /// Packets dropped because the capture -> NATS queue was full.
    queue_drops: AtomicU64,
}

/// Capacity of the capture -> NATS queue. When it is full, packets are
/// dropped (and counted) instead of ever blocking the capture thread:
/// losing packets is acceptable, stalling capture is not.
const PACKET_QUEUE_CAPACITY: usize = 4096;

pub type SharedSnifferHealth = Arc<SnifferHealth>;

impl SnifferHealth {
    pub fn active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::Relaxed);
    }

    pub fn queue_drops(&self) -> u64 {
        self.queue_drops.load(Ordering::Relaxed)
    }
}

/// Reject an empty sniffer port list. Empty must never fall back to
/// "capture everything": the agent's own NATS connection would be mirrored
/// back into the analyzer, creating a feedback loop.
fn validate_ports(ports: &[u16]) -> Result<(), &'static str> {
    if ports.is_empty() {
        Err("port list is empty; refusing to capture all traffic because \
             that would feed the agent's own NATS connection back into the analyzer")
    } else {
        Ok(())
    }
}

pub struct SnifferEngine {
    active: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    nats: NatsClient,
    interface: String,
    ports: PortsCache,
    health: SharedSnifferHealth,
}

impl SnifferEngine {
    pub fn new(
        config: &AgentConfig,
        nats: NatsClient,
        ports_cache: PortsCache,
        health: SharedSnifferHealth,
    ) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
            nats,
            interface: config.interface.clone(),
            ports: ports_cache,
            health,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        // An empty port list must never mean "capture everything" (it would
        // loop the agent's own NATS traffic back into the analyzer). Disable
        // the sniffer module instead and keep the rest of the agent alive;
        // a later port update restarts it.
        let initial_ports: Vec<u16> = self.ports.read().unwrap().iter().cloned().collect();
        if let Err(reason) = validate_ports(&initial_ports) {
            self.health.set_active(false);
            error!(
                "Sniffer disabled: {} (waiting for a port update to re-enable)",
                reason
            );
            return Err(anyhow!(reason));
        }

        let device_name = self.interface.clone();

        let interface = datalink::interfaces()
            .into_iter()
            .find(|i| i.name == device_name)
            .ok_or_else(|| anyhow!("Interface '{}' not found", device_name))?;

        let config = DatalinkConfig {
            promiscuous: true,
            read_timeout: Some(Duration::from_millis(1000)),
            read_buffer_size: 2 * 1024 * 1024,
            ..Default::default()
        };

        let mut rx = match datalink::channel(&interface, config) {
            Ok(Channel::Ethernet(_tx, rx)) => rx,
            Ok(_) => return Err(anyhow!("Unsupported channel type for {}", device_name)),
            Err(e) => return Err(anyhow!("Failed to open channel on {}: {}", device_name, e)),
        };

        let ports_desc = initial_ports
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        info!(
            "Sniffer started on {} with filter: {} (AF_PACKET, promiscuous)",
            self.interface, ports_desc
        );

        let nats = self.nats.clone();
        let ports = self.ports.clone();
        let health = Arc::clone(&self.health);
        // Bounded async channel: the capture thread only ever try_sends
        // (drop-with-counter on full), and the consumer awaits recv() so it
        // never blocks a tokio worker thread.
        let (tx, mut packet_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(PACKET_QUEUE_CAPACITY);
        self.active.store(true, Ordering::SeqCst);
        self.health.set_active(true);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            eprintln!("[SNIFFER] Capture thread spawned!");
            info!("Capture thread started, waiting for packets...");
            let mut ports_snapshot: Vec<u16> =
                ports.read().unwrap().iter().cloned().collect();
            let mut pkt_count: u64 = 0;

            while active.load(Ordering::SeqCst) {
                // Refresh ports cache periodically
                pkt_count += 1;
                if pkt_count.is_multiple_of(1000) {
                    if let Ok(guard) = ports.try_read() {
                        ports_snapshot = guard.iter().cloned().collect();
                    }
                }

                let frame = match rx.next() {
                    Ok(frame) => frame,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::TimedOut
                            || e.kind() == std::io::ErrorKind::WouldBlock
                        {
                            continue;
                        }
                        warn!("Capture error: {}", e);
                        break;
                    }
                };

                if !packet_matches_filter(frame, &ports_snapshot) {
                    continue;
                }

                let (tv_sec, tv_usec) = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(d) => (d.as_secs() as i64, d.subsec_micros() as i32),
                    Err(_) => (0, 0),
                };

                let payload = match bincode::encode_to_vec(
                    (tv_usec, tv_sec, frame.to_vec()),
                    bincode::config::standard(),
                ) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!("Serialization error: {}", e);
                        continue;
                    }
                };

                let payload_len = payload.len();
                // Never block the capture thread: a full queue means we are
                // producing faster than NATS can absorb - drop and count.
                match tx.try_send(payload) {
                    Ok(_) => trace!("Packet captured, {} bytes", payload_len),
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        let dropped = health.queue_drops.fetch_add(1, Ordering::Relaxed) + 1;
                        if dropped.is_multiple_of(1000) {
                            warn!(
                                "Packet queue full, packets dropped so far: {} (last: {} bytes)",
                                dropped, payload_len
                            );
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        warn!("Receiver dropped, stopping capture");
                        break;
                    }
                }
            }
            info!("Capture thread stopped");
            drop(tx);
        }));

        tokio::spawn(async move {
            // Same send path as before; recv() is now async and no longer
            // starves the tokio runtime on single-vCPU hosts.
            while let Some(packet_data) = packet_rx.recv().await {
                nats.send_packet(packet_data).await;
            }
            trace!("NATS sender task finished");
        });

        Ok(())
    }

    pub fn stop(&mut self) -> Option<()> {
        debug!("Stopping sniffer...");
        self.active.store(false, Ordering::SeqCst);
        self.health.set_active(false);
        self.handle.take()?.join().ok()
    }
}

/// Match TCP/UDP packets against a port filter.
/// An empty filter matches nothing: callers must validate the port list
/// before starting capture (see [`validate_ports`]) — empty must never mean
/// "capture everything", or the agent's own NATS traffic would be fed back
/// into the analyzer.
fn packet_matches_filter(frame: &[u8], ports: &[u16]) -> bool {
    let eth = match EthernetPacket::new(frame) {
        Some(e) => e,
        None => return false,
    };
    match_l3(eth.get_ethertype(), eth.payload(), ports)
}

fn match_l3(ethertype: pnet::packet::ethernet::EtherType, payload: &[u8], ports: &[u16]) -> bool {
    match ethertype {
        EtherTypes::Ipv4 => {
            let ipv4 = match Ipv4Packet::new(payload) {
                Some(p) => p,
                None => return false,
            };
            match_l4(ipv4.get_next_level_protocol(), ipv4.payload(), ports)
        }
        EtherTypes::Ipv6 => {
            let ipv6 = match Ipv6Packet::new(payload) {
                Some(p) => p,
                None => return false,
            };
            match_l4(ipv6.get_next_header(), ipv6.payload(), ports)
        }
        EtherTypes::Vlan => {
            let vlan = match VlanPacket::new(payload) {
                Some(v) => v,
                None => return false,
            };
            match_l3(vlan.get_ethertype(), vlan.payload(), ports)
        }
        _ => false,
    }
}

fn match_l4(proto: IpNextHeaderProtocol, payload: &[u8], ports: &[u16]) -> bool {
    let (src, dst) = if proto == IpNextHeaderProtocols::Tcp {
        match TcpPacket::new(payload) {
            Some(t) => (t.get_source(), t.get_destination()),
            None => return false,
        }
    } else if proto == IpNextHeaderProtocols::Udp {
        match UdpPacket::new(payload) {
            Some(u) => (u.get_source(), u.get_destination()),
            None => return false,
        }
    } else {
        return false;
    };

    if ports.is_empty() {
        // Defense in depth: the port snapshot can briefly become empty
        // between a config sync and the sniffer restart — during that
        // window capture nothing rather than everything.
        return false;
    }
    ports.contains(&src) || ports.contains(&dst)
}

pub async fn run_sniffer_loop(
    nats: NatsClient,
    config: &AgentConfig,
    ports_cache: PortsCache,
    mut restart_rx: tokio::sync::watch::Receiver<()>,
    health: SharedSnifferHealth,
) {
    let mut sniffer = SnifferEngine::new(config, nats.clone(), ports_cache.clone(), health.clone());

    match sniffer.start().await {
        Ok(_) => info!("Sniffer engine started"),
        Err(e) => warn!("Failed to start sniffer (will retry on port change): {}", e),
    }

    loop {
        // SIGTERM/SIGINT are handled centrally (see lib.rs spawn_signal_handler);
        // this loop only reacts to sniffer restarts.
        if restart_rx.changed().await.is_err() {
            info!("Restart notifier dropped, shutting down sniffer");
            sniffer.stop();
            break;
        }

        info!("Ports changed, restarting sniffer...");
        sniffer.stop();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        sniffer = SnifferEngine::new(config, nats.clone(), ports_cache.clone(), health.clone());
        match sniffer.start().await {
            Ok(_) => info!("Sniffer restarted with updated ports"),
            Err(e) => log::error!("Failed to restart sniffer: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_port_list_is_rejected() {
        assert!(validate_ports(&[]).is_err());
        assert!(validate_ports(&[80]).is_ok());
        assert!(validate_ports(&[80, 443]).is_ok());
    }

    #[test]
    fn empty_port_list_rejection_explains_why() {
        let err = validate_ports(&[]).unwrap_err();
        assert!(
            err.contains("NATS"),
            "error should explain the feedback-loop danger: {}",
            err
        );
    }

    #[test]
    fn sniffer_health_tracks_active_state() {
        let health = SnifferHealth::default();
        assert!(!health.active());
        health.set_active(true);
        assert!(health.active());
        assert_eq!(health.queue_drops(), 0);
    }

    /// Minimal Ethernet + IPv4 + TCP frame (enough header context for the
    /// pnet parsers used by the filter).
    fn tcp_frame(src_port: u16, dst_port: u16) -> Vec<u8> {
        let mut frame = vec![0u8; 14 + 20 + 20];
        frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes()); // ethertype: IPv4
        frame[14] = 0x45; // version 4, IHL 5
        frame[16..18].copy_from_slice(&40u16.to_be_bytes()); // total length
        frame[23] = 6; // IPv4 protocol: TCP
        frame[34..36].copy_from_slice(&src_port.to_be_bytes());
        frame[36..38].copy_from_slice(&dst_port.to_be_bytes());
        frame
    }

    #[test]
    fn empty_filter_matches_nothing() {
        // Even if an empty snapshot reaches the matcher (e.g. right after a
        // config sync that emptied the port set), it must not capture all.
        let frame = tcp_frame(1234, 80);
        assert!(!packet_matches_filter(&frame, &[]));
    }

    #[test]
    fn filter_matches_src_or_dst_port() {
        let frame = tcp_frame(1234, 80);
        assert!(packet_matches_filter(&frame, &[80]));
        assert!(packet_matches_filter(&frame, &[1234]));
        assert!(!packet_matches_filter(&frame, &[443]));
    }
}
