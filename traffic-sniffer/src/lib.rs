use log::info;
use crate::kafka::KafkaManager;
use crate::packet_parser::PacketCatcher;

mod configuration;
mod packet_parser;
mod kafka;

fn init() {
    let mut clog = colog::default_builder();
    clog.filter(None, log::LevelFilter::Trace);
    clog.init();
}

pub async fn run() {
    init();
    let mut config = configuration::init_config();
    config.ports.push(31337);
    println!("{config:?}");
    let kafka: KafkaManager = KafkaManager::new(&config);
    let mut packet_capture = PacketCatcher::new(&config, kafka);

    packet_capture.start().await.expect("TODO: panic message");

    tokio::signal::ctrl_c().await.expect("TODO: panic message");
    info!("Shutdown signal received, exiting.");
    packet_capture.stop_capturing();
}