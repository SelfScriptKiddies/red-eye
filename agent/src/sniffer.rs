use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

use log::{debug, error, info, trace, warn};
use pcap::{Capture, Error};

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

    pub async fn start(&mut self) -> Result<(), Error> {
        let device_name = self.interface.clone();

        let mut cap = Capture::from_device(device_name.as_str())?
            .promisc(true)
            .snaplen(65535)
            .timeout(1000)
            .open()?
            .setnonblock()?;

        let initial_ports: Vec<u16> = self.ports.read().unwrap().iter().cloned().collect();
        let ports_filter = if initial_ports.is_empty() {
            "tcp or udp".to_string()
        } else {
            initial_ports
                .iter()
                .map(|port| format!("tcp port {} or udp port {}", port, port))
                .collect::<Vec<_>>()
                .join(" or ")
        };

        cap.filter(&ports_filter, true).unwrap_or_else(|e| {
            warn!("Failed to set filter on {}: {}", self.interface, e);
        });

        info!(
            "Sniffer started on {} with filter: {}",
            self.interface, ports_filter
        );

        let nats = self.nats.clone();
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        self.active.store(true, Ordering::SeqCst);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            let mut capture = cap;
            debug!("Capture thread started");
            while active.load(Ordering::SeqCst) {
                let packet = match capture.next_packet() {
                    Ok(packet) => packet,
                    Err(pcap::Error::NoMorePackets) | Err(pcap::Error::TimeoutExpired) => {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => {
                        warn!("Capture error: {}", e);
                        break;
                    }
                };

                let payload = match bincode::encode_to_vec(
                    (
                        packet.header.ts.tv_usec,
                        packet.header.ts.tv_sec,
                        packet.to_vec(),
                    ),
                    bincode::config::standard(),
                ) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!("Serialization error: {}", e);
                        continue;
                    }
                };

                if tx.send(payload).is_err() {
                    warn!("Receiver dropped, stopping capture");
                    break;
                }

                trace!("Packet captured");
            }
            info!("Capture thread stopped");
            drop(tx);
        }));

        tokio::spawn(async move {
            while let Ok(packet_data) = rx.recv() {
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

/// Main sniffer loop with restart support on port changes
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
                    Err(e) => error!("Failed to restart sniffer: {}", e),
                }
            }
        }
    }
}
