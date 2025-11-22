use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use log::{debug, info, trace, warn};
use pcap::{Capture, Error};
use crate::configuration::ApplicationConfig;
use crate::nats::NATSManager;

pub struct PacketCatcher {
    active: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    broker_manager: NATSManager,
    interface: String,
    ports: Vec<u16>
}

impl PacketCatcher {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let broker_manager = NATSManager::new(config).await;

        Self {
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
            broker_manager: broker_manager,
            interface: config.interface.clone(),
            ports: config.ports.0.clone()
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Error>{
        let device_name = self.interface.clone();

        let mut cap = Capture::from_device(&*device_name)?
            .promisc(true)  // Режим промискуитета
            .snaplen(65535) // Максимальная длина захватываемых пакетов
            .timeout(1000)  // Таймаут в миллисекундах
            .open()?
            .setnonblock()?;

        let ports_filter = self.ports.iter()
            .map(|port| format!("tcp port {} or udp port {}", port, port))
            .collect::<Vec<_>>()
            .join(" or ");

        cap.filter(&ports_filter, true).unwrap_or_else(|e| {
            warn!("Failed to set filter on {}: {}", self.interface, e);
        });

        info!("Started capturing on interface {} with filter: {}", self.interface, ports_filter);

        let broker_manager = self.broker_manager.clone();
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        self.active.store(true, Ordering::SeqCst);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            let mut capture = cap;
            debug!("Capturing the packets started!");
            while active.load(Ordering::SeqCst) {
                let packet: pcap::Packet<'_> = match capture.next_packet() {
                    Ok(packet) => packet,
                    Err(pcap::Error::NoMorePackets) | Err(pcap::Error::TimeoutExpired) => {
                        std::thread::sleep(std::time::Duration::from_millis(5)); // Little timeout to avoid 100% CPU usage
                        continue;
                    }
                    Err(e) => {
                        warn!("Got error while gathering packets: {}", e);
                        break;
                    }
                };

                let payload = match bincode::encode_to_vec((packet.header.ts.tv_usec, packet.header.ts.tv_sec, packet.to_vec()), bincode::config::standard()) {
                    Ok(bytes) => bytes,
                    Err(e) => { 
                        warn!("Error serializing: {}", e);
                        continue;
                    }
                };

                if tx.send(payload).is_err() {
                    warn!("Receiver dropped, stopping capture thread.");
                    break;
                }

                trace!("Sent packet to NATS");
            }
            info!("Capturing stopped");
            drop(tx);
        }));

        tokio::spawn(async move {
            while let Ok(packet_data) = rx.recv() {
                broker_manager.send_packet(packet_data).await;
            }
            trace!("NATS sender thread destroyed");
        });


        Ok(())
    }

    pub fn stop_capturing(&mut self) -> Option<()> {
        debug!("Stopping capturing...");
        self.active.store(false, Ordering::SeqCst);
        self.handle.take()?.join().ok()
    }
}
