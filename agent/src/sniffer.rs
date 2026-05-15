use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use log::{debug, info, trace, warn};
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

pub struct SnifferEngine {
    active: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    nats: NatsClient,
    interface: String,
    ports: PortsCache,
}

impl SnifferEngine {
    pub fn new(config: &AgentConfig, nats: NatsClient, ports_cache: PortsCache) -> Self {
        Self {
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
            nats,
            interface: config.interface.clone(),
            ports: ports_cache,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let device_name = self.interface.clone();

        let interface = datalink::interfaces()
            .into_iter()
            .find(|i| i.name == device_name)
            .ok_or_else(|| anyhow!("Interface '{}' not found", device_name))?;

        let config = DatalinkConfig {
            promiscuous: true,
            read_timeout: Some(Duration::from_millis(1000)),
            read_buffer_size: 65536,
            ..Default::default()
        };

        let mut rx = match datalink::channel(&interface, config) {
            Ok(Channel::Ethernet(_tx, rx)) => rx,
            Ok(_) => return Err(anyhow!("Unsupported channel type for {}", device_name)),
            Err(e) => return Err(anyhow!("Failed to open channel on {}: {}", device_name, e)),
        };

        let initial_ports: Vec<u16> = self.ports.read().unwrap().iter().cloned().collect();
        let ports_desc = if initial_ports.is_empty() {
            "tcp or udp (all)".to_string()
        } else {
            initial_ports
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        info!(
            "Sniffer started on {} with filter: {} (AF_PACKET, promiscuous)",
            self.interface, ports_desc
        );

        let nats = self.nats.clone();
        let ports = self.ports.clone();
        let (tx, mpsc_rx) = mpsc::channel::<Vec<u8>>();
        self.active.store(true, Ordering::SeqCst);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            eprintln!("[SNIFFER] Capture thread spawned!");
            info!("Capture thread started, waiting for packets...");
            while active.load(Ordering::SeqCst) {
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

                let ports_snapshot: Vec<u16> =
                    ports.read().unwrap().iter().cloned().collect();
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
                if tx.send(payload).is_err() {
                    warn!("Receiver dropped, stopping capture");
                    break;
                }

                info!("Packet captured, {} bytes", payload_len);
            }
            info!("Capture thread stopped");
            drop(tx);
        }));

        tokio::spawn(async move {
            while let Ok(packet_data) = mpsc_rx.recv() {
                nats.send_packet(packet_data).await;
            }
            trace!("NATS sender task finished");
        });

        Ok(())
    }

    pub fn stop(&mut self) -> Option<()> {
        debug!("Stopping sniffer...");
        self.active.store(false, Ordering::SeqCst);
        self.handle.take()?.join().ok()
    }
}

/// Match TCP/UDP packets against a port filter.
/// Empty filter = all TCP/UDP traffic passes.
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
        return true;
    }
    ports.contains(&src) || ports.contains(&dst)
}

pub async fn run_sniffer_loop(
    nats: NatsClient,
    config: &AgentConfig,
    ports_cache: PortsCache,
    mut restart_rx: tokio::sync::watch::Receiver<()>,
) {
    let mut sniffer = SnifferEngine::new(config, nats.clone(), ports_cache.clone());

    match sniffer.start().await {
        Ok(_) => info!("Sniffer engine started"),
        Err(e) => warn!("Failed to start sniffer (will retry on port change): {}", e),
    }

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
                sniffer.stop();
                break;
            }
            result = restart_rx.changed() => {
                if result.is_err() {
                    info!("Restart notifier dropped, shutting down");
                    sniffer.stop();
                    break;
                }

                info!("Ports changed, restarting sniffer...");
                sniffer.stop();
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                sniffer = SnifferEngine::new(config, nats.clone(), ports_cache.clone());
                match sniffer.start().await {
                    Ok(_) => info!("Sniffer restarted with updated ports"),
                    Err(e) => log::error!("Failed to restart sniffer: {}", e),
                }
            }
        }
    }
}
