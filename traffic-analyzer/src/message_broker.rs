use bincode;
use tokio::task;
use std::sync::Arc;
use anyhow::{Context, Result};
use log::{trace, error, info, warn};
use futures::StreamExt;
use async_nats::{
    ConnectOptions, 
    Client, 
    jetstream::{self, stream, consumer::{pull, AckPolicy}}
};

use crate::db::DB;
use crate::cache::Cache;
use crate::ethernet::parse_packet;
use crate::process_packet::PacketProcessor;
use crate::configuration::ApplicationConfig;


pub async fn create_consumer(config: &ApplicationConfig) -> Result<Client> {
    let host = config.nats_host.clone();
    let client = ConnectOptions::with_user_and_password(
        config.nats_login.clone(), 
        config.nats_password.clone()
    ).connect(&host).await
    .with_context(|| format!("Error connecting to NATS: {}", &host))?;

    Ok(client)
}

pub async fn consume_raw_packets(client: Client, cache: Arc<Cache>, config: Arc<ApplicationConfig>, db: DB) -> Result<()> {
    let subject = config.nats_raw_packets_topic.clone();
    
    let js = jetstream::new(client.clone());  
    let stream = js
        .create_stream(stream::Config {
            name: "RAW_PACKETS".to_string(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await?;

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("raw_packets_consumer".to_string()),
            filter_subject: subject.clone(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await?;  

    let mut messages = consumer.messages().await?; 
    info!("Starting to consume packets from topic {}", &subject);

    let packet_processor = Arc::new(PacketProcessor::new(cache, config, db));

    while let Some(message) = messages.next().await {
        match message {
            Ok(message) => {
                // Get payload and clone it so it can live in a separate task
                let payload = message.payload.to_vec();

                // Processing packet in a separate task with cloned data
                let processor_clone = packet_processor.clone();
                task::spawn(async move {
                    trace!("Processing packet of size {} bytes: {:?}", payload.len(), payload); 
                    let packet: (i64, i64, Vec<u8>) = match bincode::decode_from_slice(&payload, bincode::config::standard()) {
                        Ok((u, _)) => u,
                        Err(e) => {
                            error!("Error decoding new packet: {}", e);
                            return;
                        }
                    };
                    // packet.0 = usec, packet.1 = sec
                    let parsed_packet = parse_packet((packet.0 as u64, packet.1 as u64), packet.2);
                    
                    if let Some(packet) = parsed_packet {
                        processor_clone.process_packet(packet).await.unwrap_or_else(|e| {
                            error!("Error processing packet: {}", e);
                        });
                    }
                });

                // Commit message after cloning payload
                match message.ack().await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error acknowledging message: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving message: {}", e);
            }
        }
    }

    Ok(())
}
