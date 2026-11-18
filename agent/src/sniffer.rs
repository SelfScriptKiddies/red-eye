use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
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

use crate::nats::NatsClient;
use crate::updates::{ExcludedPortsCache, PortsCache, SharedConfig, TransportProto};

/// Shared sniffer health, reported via heartbeat.
#[derive(Debug, Default)]
pub struct SnifferHealth {
    /// Whether the sniffer capture thread is currently running.
    active: AtomicBool,
    /// True when passive capture is idle because the passive port set is
    /// empty (no services, or every key is proxied): a healthy waiting
    /// state, not an error.
    idle_no_services: AtomicBool,
    /// Packets dropped because the capture -> NATS queue was full.
    queue_drops: AtomicU64,
    /// Generation of the passive port set the capture thread last loaded:
    /// transitions wait for this to advance before trusting an exclusion.
    applied_generation: AtomicU64,
    /// Frames seen for keys that are effectively proxied. Any increment is
    /// a capture-source state bug (passive must be excluded) and is
    /// surfaced via diagnostics, never silently dropped.
    capture_source_conflicts: AtomicU64,
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

    pub fn idle_no_services(&self) -> bool {
        self.idle_no_services.load(Ordering::Relaxed)
    }

    pub fn set_idle_no_services(&self, idle: bool) {
        self.idle_no_services.store(idle, Ordering::Relaxed);
    }

    pub fn queue_drops(&self) -> u64 {
        self.queue_drops.load(Ordering::Relaxed)
    }

    pub fn applied_generation(&self) -> u64 {
        self.applied_generation.load(Ordering::SeqCst)
    }

    pub fn capture_source_conflicts(&self) -> u64 {
        self.capture_source_conflicts.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn set_applied_generation_test_only(&self, generation: u64) {
        self.set_applied_generation(generation);
    }

    fn set_applied_generation(&self, generation: u64) {
        self.applied_generation.store(generation, Ordering::SeqCst);
    }

    fn add_capture_source_conflict(&self) {
        let conflicts = self
            .capture_source_conflicts
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        // Rate-limited visibility: a conflict is a bug, not normal traffic.
        if conflicts == 1 || conflicts.is_multiple_of(1000) {
            warn!(
                "Capture-source conflict: passive sniffer captured a frame for a \
                 proxied key (total: {}) - exclusion must never be lost",
                conflicts
            );
        }
    }
}

/// A capture channel opened and validated but not yet forwarding. Opening
/// is the step that fails (missing interface, permission denied), so it
/// runs BEFORE the working capture is retired: on failure the previous
/// capture stays active (prepare-before-retire handoff).
struct PreparedCapture {
    interface: String,
    rx: Box<dyn pnet::datalink::DataLinkReceiver>,
}

pub struct SnifferEngine {
    active: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    nats: NatsClient,
    /// Interface the current capture is bound to.
    interface: String,
    ports: PortsCache,
    excluded_ports: ExcludedPortsCache,
    ports_generation: Arc<AtomicU64>,
    /// Port-set refreshes are triggered through the same watch that
    /// carries interface changes; the capture thread reacts without a
    /// restart.
    restart_rx: tokio::sync::watch::Receiver<()>,
    health: SharedSnifferHealth,
}

impl SnifferEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        nats: NatsClient,
        interface: String,
        ports_cache: PortsCache,
        excluded_ports: ExcludedPortsCache,
        ports_generation: Arc<AtomicU64>,
        restart_rx: tokio::sync::watch::Receiver<()>,
        health: SharedSnifferHealth,
    ) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
            nats,
            interface,
            ports: ports_cache,
            excluded_ports,
            ports_generation,
            restart_rx,
            health,
        }
    }

    pub fn current_interface(&self) -> &str {
        &self.interface
    }

    pub fn is_running(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Look up the interface and open the capture channel without
    /// forwarding anything yet.
    fn prepare(&self, interface_name: &str) -> Result<PreparedCapture> {
        let interface = datalink::interfaces()
            .into_iter()
            .find(|i| i.name == interface_name)
            .ok_or_else(|| anyhow!("Interface '{}' not found", interface_name))?;

        // Loopback is a known Linux trap for AF_PACKET captures: the kernel
        // taps every loopback transmission TWICE (egress + ingress), so the
        // sniffer receives, publishes and the analyzer stores EVERY packet
        // duplicated (byte-identical payloads, µs timestamps a few µs
        // apart). This is a kernel property, not a bug we can filter at
        // capture time (pnet discards the sockaddr_ll packet type) — warn
        // loudly and let the operator pick a real interface.
        if interface.is_loopback() {
            warn!(
                "Interface '{}' is loopback: every packet will be captured TWICE \
                 (Linux AF_PACKET loopback double-tap) and stored duplicated in Red Eye. \
                 Use a non-loopback interface for clean captures.",
                interface_name
            );
        }

        let config = DatalinkConfig {
            promiscuous: true,
            read_timeout: Some(Duration::from_millis(1000)),
            read_buffer_size: 2 * 1024 * 1024,
            ..Default::default()
        };

        let rx = match datalink::channel(&interface, config) {
            Ok(Channel::Ethernet(_tx, rx)) => rx,
            Ok(_) => return Err(anyhow!("Unsupported channel type for {}", interface_name)),
            Err(e) => {
                return Err(anyhow!(
                    "Failed to open channel on {}: {}",
                    interface_name,
                    e
                ))
            }
        };

        Ok(PreparedCapture {
            interface: interface_name.to_string(),
            rx,
        })
    }

    /// Open a capture on the configured interface (prepare + start).
    pub async fn start(&mut self) -> Result<()> {
        let prepared = self.prepare(&self.interface)?;
        self.start_prepared(prepared)
    }

    /// Start the forwarding loop on an already-opened channel.
    fn start_prepared(&mut self, prepared: PreparedCapture) -> Result<()> {
        // Generation read BEFORE the port set: the writer swaps the set
        // first and bumps the generation second, so applied_generation >= g
        // implies the loaded snapshot includes every exclusion of g.
        let initial_generation = self.ports_generation.load(Ordering::SeqCst);
        let ports: Vec<(TransportProto, u16)> =
            self.ports.read().unwrap().iter().cloned().collect();
        let ports_desc = ports
            .iter()
            .map(|(p, port)| format!("{}{}", port, p.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        info!(
            "Sniffer started on {} with filter: {} (AF_PACKET, promiscuous)",
            prepared.interface, ports_desc
        );

        let nats = self.nats.clone();
        let ports = self.ports.clone();
        let excluded_ports = self.excluded_ports.clone();
        let ports_generation = self.ports_generation.clone();
        let mut restart_rx = self.restart_rx.clone();
        let health = Arc::clone(&self.health);
        // Bounded async channel: the capture thread only ever try_sends
        // (drop-with-counter on full), and the consumer awaits recv() so it
        // never blocks a tokio worker thread. Payloads are raw tuples —
        // protobuf envelope encoding happens once per BATCH inside the NATS client's
        // batcher, not per packet in the capture thread.
        let (tx, mut packet_rx) =
            tokio::sync::mpsc::channel::<crate::nats::RawPacket>(PACKET_QUEUE_CAPACITY);
        self.active.store(true, Ordering::SeqCst);
        self.health.set_active(true);
        self.health.set_idle_no_services(false);
        self.interface = prepared.interface.clone();
        self.health.set_applied_generation(initial_generation);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            info!("Capture thread started on {}", prepared.interface);
            let mut rx = prepared.rx;
            let mut ports_snapshot: Vec<(TransportProto, u16)> =
                ports.read().unwrap().iter().cloned().collect();
            let mut excluded_snapshot: Vec<(TransportProto, u16)> =
                excluded_ports.read().unwrap().iter().cloned().collect();
            let mut pkt_count: u64 = 0;

            while active.load(Ordering::SeqCst) {
                pkt_count += 1;
                // Refresh the passive/excluded snapshots when a config
                // change is signalled (port set) or periodically. No
                // restart: the matcher set swap is atomic under the lock,
                // so unrelated keys keep capturing without a gap.
                let change_signalled = restart_rx.has_changed().unwrap_or(false);
                let periodic = pkt_count.is_multiple_of(1000);
                if change_signalled || periodic {
                    // On a lock miss the signal stays pending and the next
                    // iteration retries; the generation is published only
                    // after both snapshots are loaded together.
                    if let (Ok(ports_guard), Ok(excluded_guard)) =
                        (ports.try_read(), excluded_ports.try_read())
                    {
                        ports_snapshot = ports_guard.iter().cloned().collect();
                        excluded_snapshot = excluded_guard.iter().cloned().collect();
                        health.set_applied_generation(ports_generation.load(Ordering::SeqCst));
                        if change_signalled {
                            restart_rx.mark_changed();
                        }
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

                if let Some(conflict) = ports_snapshot_match_excluded(frame, &excluded_snapshot) {
                    // A frame for a proxied key must never reach the
                    // passive sniffer: count it, never publish it.
                    health.add_capture_source_conflict();
                    trace!(
                        "Excluded frame not published (conflicting key {}/{})",
                        conflict.1,
                        conflict.0.as_str()
                    );
                    continue;
                }

                if !ports_snapshot_match(frame, &ports_snapshot) {
                    continue;
                }

                let (tv_sec, tv_usec) = match SystemTime::now().duration_since(UNIX_EPOCH) {
                    Ok(d) => (d.as_secs() as i64, d.subsec_micros() as i32),
                    Err(_) => (0, 0),
                };

                let packet = (tv_usec as i64, tv_sec, frame.to_vec());
                let payload_len = packet.2.len();
                // Never block the capture thread: a full queue means we are
                // producing faster than NATS can absorb - drop and count.
                match tx.try_send(packet) {
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
            // Forward into the NATS client's batching queue (which does the
            // per-batch protobuf encode + publish); recv() stays async so the
            // runtime is never starved on single-vCPU hosts.
            while let Some(packet) = packet_rx.recv().await {
                nats.send_packet(packet);
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

/// Snapshot the port set as a small Vec for the capture thread's periodic
/// refresh; matching checks membership linearly (sets hold at most dozens
/// of entries).
type PortSet = Vec<(TransportProto, u16)>;

/// Extract the (protocol, src_port, dst_port) triple of a frame, if it is
/// a TCP/UDP frame.
fn frame_transport(frame: &[u8]) -> Option<(TransportProto, u16, u16)> {
    let eth = EthernetPacket::new(frame)?;
    let (transport, src, dst) = match eth.get_ethertype() {
        EtherTypes::Ipv4 => {
            let ipv4 = Ipv4Packet::new(eth.payload())?;
            match_transport(ipv4.get_next_level_protocol(), ipv4.payload())?
        }
        EtherTypes::Ipv6 => {
            let ipv6 = Ipv6Packet::new(eth.payload())?;
            match_transport(ipv6.get_next_header(), ipv6.payload())?
        }
        EtherTypes::Vlan => {
            let vlan = VlanPacket::new(eth.payload())?;
            match vlan.get_ethertype() {
                EtherTypes::Ipv4 => {
                    let ipv4 = Ipv4Packet::new(vlan.payload())?;
                    match_transport(ipv4.get_next_level_protocol(), ipv4.payload())?
                }
                EtherTypes::Ipv6 => {
                    let ipv6 = Ipv6Packet::new(vlan.payload())?;
                    match_transport(ipv6.get_next_header(), ipv6.payload())?
                }
                _ => return None,
            }
        }
        _ => return None,
    };
    Some((transport, src, dst))
}

fn match_transport(
    proto: IpNextHeaderProtocol,
    payload: &[u8],
) -> Option<(TransportProto, u16, u16)> {
    if proto == IpNextHeaderProtocols::Tcp {
        let t = TcpPacket::new(payload)?;
        Some((TransportProto::Tcp, t.get_source(), t.get_destination()))
    } else if proto == IpNextHeaderProtocols::Udp {
        let u = UdpPacket::new(payload)?;
        Some((TransportProto::Udp, u.get_source(), u.get_destination()))
    } else {
        None
    }
}

/// Match TCP/UDP packets against a (protocol, port) filter. Test-only:
/// production matching goes through ports_snapshot_match / the excluded
/// conflict check. An empty filter matches nothing: empty must never mean
/// "capture everything", or the agent's own NATS traffic would be fed
/// back into the analyzer.
#[cfg(test)]
fn packet_matches_filter(frame: &[u8], ports: &HashSet<(TransportProto, u16)>) -> bool {
    let Some((transport, src, dst)) = frame_transport(frame) else {
        return false;
    };
    if ports.is_empty() {
        // Defense in depth: empty means disabled, never "capture all".
        return false;
    }
    ports.contains(&(transport, src)) || ports.contains(&(transport, dst))
}

/// Returns the conflicting (protocol, port) key when the frame belongs to
/// a key excluded from passive capture (effectively proxied).
fn ports_snapshot_match_excluded(
    frame: &[u8],
    excluded: &PortSet,
) -> Option<(TransportProto, u16)> {
    let (transport, src, dst) = frame_transport(frame)?;
    excluded
        .iter()
        .find(|(proto, port)| *proto == transport && (*port == src || *port == dst))
        .copied()
}

fn ports_snapshot_match(frame: &[u8], ports: &PortSet) -> bool {
    let Some((transport, src, dst)) = frame_transport(frame) else {
        return false;
    };
    if ports.is_empty() {
        // Defense in depth: the port snapshot can briefly become empty
        // during a config sync - capture nothing rather than everything.
        return false;
    }
    ports.contains(&(transport, src)) || ports.contains(&(transport, dst))
}

/// Sniffer lifecycle: reacts to restart signals from applied snapshots and
/// proxy transitions. Port-set-only changes never restart the capture
/// thread (the matcher set is refreshed in place, so enabling one service
/// does not disturb unrelated keys). Interface changes are prepared before
/// the working capture is retired. An empty passive set disables capture
/// while the agent stays healthy and idle.
#[allow(clippy::too_many_arguments)]
pub async fn run_sniffer_loop(
    nats: NatsClient,
    ports_cache: PortsCache,
    excluded_ports: ExcludedPortsCache,
    ports_generation: Arc<AtomicU64>,
    mut restart_rx: tokio::sync::watch::Receiver<()>,
    health: SharedSnifferHealth,
    config: SharedConfig,
    last_sync_error: Arc<RwLock<Option<String>>>,
    acker: Arc<crate::updates::RevisionAcker>,
) {
    let initial = config.read().unwrap().interface.clone();
    let mut sniffer = SnifferEngine::new(
        nats.clone(),
        initial,
        ports_cache.clone(),
        excluded_ports.clone(),
        ports_generation.clone(),
        restart_rx.clone(),
        health.clone(),
    );

    match sniffer.start().await {
        Ok(_) => {
            info!("Sniffer engine started");
            confirm_passive_applied(&ports_generation, &health, &acker).await;
        }
        Err(e) => {
            warn!(
                "Failed to start sniffer (waiting for a config update): {}",
                e
            );
            *last_sync_error.write().unwrap() =
                Some(e.to_string().chars().take(300).collect::<String>());
        }
    }

    loop {
        // SIGTERM/SIGINT are handled centrally (see lib.rs spawn_signal_handler);
        // this loop only reacts to sniffer restarts.
        if restart_rx.changed().await.is_err() {
            info!("Restart notifier dropped, shutting down sniffer");
            sniffer.stop();
            break;
        }

        let desired_interface = config.read().unwrap().interface.clone();
        let ports_now: HashSet<(TransportProto, u16)> = ports_cache.read().unwrap().clone();

        // Empty passive set: sniffer disabled, agent healthy and idle
        // while heartbeat, proxy capture and update processing continue.
        if ports_now.is_empty() {
            if sniffer.is_running() {
                info!("Passive port set is empty, disabling sniffer (idle)");
                sniffer.stop();
            }
            health.set_idle_no_services(true);
            // Nothing to capture: the passive side is trivially applied.
            acker.confirm_passive();
            continue;
        }
        health.set_idle_no_services(false);

        let port_change_only = sniffer.current_interface() == desired_interface;
        if port_change_only {
            // Port-set change on the same interface: NO restart. The
            // capture thread refreshes its matcher set from the generation
            // watch within milliseconds; unrelated keys keep capturing and
            // established state is untouched.
            debug!("Passive port set changed, refreshing in place");
            confirm_passive_applied(&ports_generation, &health, &acker).await;
        } else {
            // Interface change: open the new capture BEFORE retiring the
            // working one. On failure the old capture stays active and the
            // revision stays unapplied (the backend re-delivers).
            info!(
                "Interface change {} -> {}: preparing new capture",
                sniffer.current_interface(),
                desired_interface
            );
            let mut replacement = SnifferEngine::new(
                nats.clone(),
                desired_interface.clone(),
                ports_cache.clone(),
                excluded_ports.clone(),
                ports_generation.clone(),
                restart_rx.clone(),
                health.clone(),
            );
            match replacement.prepare(&desired_interface) {
                Ok(prepared) => {
                    sniffer.stop();
                    match replacement.start_prepared(prepared) {
                        Ok(_) => {
                            sniffer = replacement;
                            info!("Interface handoff complete");
                            confirm_passive_applied(&ports_generation, &health, &acker).await;
                        }
                        Err(e) => {
                            error!("Prepared capture failed to start: {}", e);
                            *last_sync_error.write().unwrap() =
                                Some(e.to_string().chars().take(300).collect());
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Interface handoff to '{}' failed, keeping '{}' active: {}",
                        desired_interface,
                        sniffer.current_interface(),
                        e
                    );
                    *last_sync_error.write().unwrap() =
                        Some(format!("interface '{}': {}", desired_interface, e));
                }
            }
        }
    }
}

/// The passive side of the pending revision counts as applied once the
/// capture thread has loaded the latest port-set generation. Bounded: a
/// stale generation means the sniffer is unhealthy; the revision stays
/// unacknowledged and the backend re-delivers.
async fn confirm_passive_applied(
    ports_generation: &Arc<AtomicU64>,
    health: &SnifferHealth,
    acker: &Arc<crate::updates::RevisionAcker>,
) {
    let target = ports_generation.load(Ordering::SeqCst);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while health.applied_generation() < target {
        if std::time::Instant::now() >= deadline {
            warn!(
                "Sniffer did not confirm port set generation {} in time; revision stays unacknowledged",
                target
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    acker.confirm_passive();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_matches_nothing() {
        // Even if an empty snapshot reaches the matcher, it must not
        // capture all: empty means disabled, never "capture everything".
        let frame = tcp_frame(1234, 80);
        assert!(!packet_matches_filter(&frame, &HashSet::new()));
    }

    #[test]
    fn tcp_ports_match_tcp_packets() {
        let mut ports = HashSet::new();
        ports.insert((TransportProto::Tcp, 80));
        assert!(packet_matches_filter(&tcp_frame(1234, 80), &ports));
        assert!(packet_matches_filter(&tcp_frame(80, 1234), &ports));
        assert!(!packet_matches_filter(&tcp_frame(1234, 81), &ports));
    }

    #[test]
    fn tcp_port_does_not_match_udp_packet_on_same_port() {
        // TCP and UDP services on the same port are independent: a UDP
        // packet must not match a TCP-only port set.
        let mut ports = HashSet::new();
        ports.insert((TransportProto::Tcp, 53));
        assert!(!packet_matches_filter(&udp_frame(1234, 53), &ports));

        ports.insert((TransportProto::Udp, 53));
        assert!(packet_matches_filter(&udp_frame(1234, 53), &ports));
        assert!(packet_matches_filter(&udp_frame(53, 1234), &ports));
    }

    #[test]
    fn excluded_keys_are_detected_as_conflicts_not_captured() {
        // A proxied key's frame must be recognized as a conflict so the
        // capture thread counts it instead of publishing it.
        let excluded: PortSet = vec![(TransportProto::Tcp, 80)];
        assert_eq!(
            ports_snapshot_match_excluded(&tcp_frame(1234, 80), &excluded),
            Some((TransportProto::Tcp, 80))
        );
        assert_eq!(
            ports_snapshot_match_excluded(&tcp_frame(80, 1234), &excluded),
            Some((TransportProto::Tcp, 80))
        );
        assert_eq!(
            ports_snapshot_match_excluded(&tcp_frame(1234, 81), &excluded),
            None
        );
        // Protocol independence: a UDP frame for the same numeric port is
        // NOT a conflict of a TCP exclusion.
        assert_eq!(
            ports_snapshot_match_excluded(&udp_frame(1234, 80), &excluded),
            None
        );

        let health = SnifferHealth::default();
        health.add_capture_source_conflict();
        assert_eq!(health.capture_source_conflicts(), 1);
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

    /// Minimal Ethernet + IPv4 + UDP frame.
    fn udp_frame(src_port: u16, dst_port: u16) -> Vec<u8> {
        let mut frame = vec![0u8; 14 + 20 + 8];
        frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes()); // ethertype: IPv4
        frame[14] = 0x45; // version 4, IHL 5
        frame[16..18].copy_from_slice(&28u16.to_be_bytes()); // total length
        frame[23] = 17; // IPv4 protocol: UDP
        frame[34..36].copy_from_slice(&src_port.to_be_bytes());
        frame[36..38].copy_from_slice(&dst_port.to_be_bytes());
        frame[38..40].copy_from_slice(&8u16.to_be_bytes()); // UDP length
        frame
    }

    #[test]
    fn sniffer_health_tracks_active_state() {
        let health = SnifferHealth::default();
        assert!(!health.active());
        assert!(!health.idle_no_services());
        health.set_active(true);
        health.set_idle_no_services(true);
        assert!(health.active());
        assert!(health.idle_no_services());
        assert_eq!(health.queue_drops(), 0);
        health.set_applied_generation(7);
        assert_eq!(health.applied_generation(), 7);
    }

    #[test]
    fn empty_port_list_rejection_is_gone_in_favor_of_idle_state() {
        // Empty service sets are a healthy idle state now (see
        // run_sniffer_loop); the matcher side still refuses to capture
        // everything when the set is empty.
        assert!(!packet_matches_filter(&tcp_frame(1, 2), &HashSet::new()));
    }
}
