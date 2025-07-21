use log::{debug, error};
use async_nats::{
    ConnectOptions, 
    Client
};
use crate::configuration::ApplicationConfig;

#[derive(Clone)]
pub struct NATSManager {
    broker_client: Client,
    topic: String
}

impl NATSManager {
    pub async fn new(config: &ApplicationConfig) -> Self {
        let client = ConnectOptions::with_user_and_password(
            config.nats_login.clone(), 
            config.nats_password.clone())
            .connect(config.nats_host.clone())
            .await
            .expect("Error connecting to NATS");

        Self {
            broker_client: client,
            topic: config.nats_raw_packets_topic.clone()
        }
    }

    pub async fn send_packet<'a>(self: &Self, packet_data: Vec<u8>) {
        let topic = self.topic.clone();
        let broker_client = self.broker_client.clone();

        tokio::spawn(
            async move {
                let payload = packet_data;

                match broker_client.publish(topic, payload.into()).await {
                    Ok(_) => {
                        debug!("Succesfully sent to NATS");
                    },
                    Err(e) => {
                        error!("Failed to send message to NATS: {}", e);
                    },
                }
            }
        );


    }
}