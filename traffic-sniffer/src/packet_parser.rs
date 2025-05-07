use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use log::{debug, info, trace, warn};
use pcap::{Capture, Error};
use crate::configuration::Configuration;
use crate::kafka::KafkaManager;

pub struct PacketCatcher {
    active: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    kafka_client: KafkaManager,
    interface: String,
    ports: Vec<u16>
}

impl PacketCatcher {
    pub fn new(config: &Configuration, kafka_manager: KafkaManager) -> PacketCatcher {
        PacketCatcher {
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
            kafka_client: kafka_manager,
            interface: config.interface.clone(),
            ports: config.ports.clone()
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Error>{
        let device_name = self.interface.clone();

        let mut cap = Capture::from_device(&*device_name)?
            .promisc(true)  // Режим промискуитета
            .snaplen(65535) // Максимальная длина захватываемых пакетов
            .timeout(1000)  // Таймаут в миллисекундах
            .open()?;

        let ports_filter = self.ports.iter()
            .map(|port| format!("tcp port {} or udp port {}", port, port))
            .collect::<Vec<_>>()
            .join(" or ");

        cap.filter(&ports_filter, true).unwrap_or_else(|e| {
            warn!("Failed to set filter on {}: {}", self.interface, e);
        });

        info!("Started capturing on interface {} with filter: {}", self.interface, ports_filter);

        let kafka_client = self.kafka_client.clone();
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        self.active.store(true, Ordering::SeqCst);
        let active = Arc::clone(&self.active);

        self.handle = Some(thread::spawn(move || {
            let mut capture = cap;
            debug!("Thread, capturing the packets, started!");
            while active.load(Ordering::SeqCst) {
                let packet: pcap::Packet<'_> = match capture.next_packet().ok() {
                    Some(packet) => packet,
                    None => {
                        debug!("Got error while gathering packets, stopping...");
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

                trace!("Sent packet!");
            }
            info!("Capturing stopped");
            drop(tx);
        }));

        tokio::spawn(async move {
            while let Ok(packet_data) = rx.recv() {
                kafka_client.send_packet(packet_data).await;
                debug!("Processed packet and sent to Kafka");
            }
            trace!("Kafka sender thread destroyed");
        });


        Ok(())
    }

    pub fn stop_capturing(&mut self) -> Option<()> {
        debug!("Stopping capturing...");
        self.active.store(false, Ordering::SeqCst);
        self.handle.take()?.join().ok()
    }
}
