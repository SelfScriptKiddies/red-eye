use async_nats::{Client, ConnectOptions};
use log::{debug, error};

use crate::config::AgentConfig;

#[derive(Clone)]
pub struct NatsClient {
    client: Client,
    pub raw_packets_topic: String,
    pub agent_id: String,
}

impl NatsClient {
    pub async fn connect(config: &AgentConfig, agent_id: &str) -> anyhow::Result<Self> {
        let client = ConnectOptions::with_user_and_password(
            config.nats_login.clone(),
            config.nats_password.clone(),
        )
        .connect(&config.nats_host)
        .await?;

        Ok(Self {
            client,
            raw_packets_topic: config.nats_raw_packets_topic.clone(),
            agent_id: agent_id.to_string(),
        })
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    pub async fn send_packet(&self, packet_data: Vec<u8>) {
        let topic = self.raw_packets_topic.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            if let Err(e) = client.publish(topic, packet_data.into()).await {
                error!("Failed to send packet to NATS: {}", e);
            } else {
                debug!("Packet sent to NATS");
            }
        });
    }

    pub async fn send_heartbeat(&self, status_json: Vec<u8>) {
        let topic = format!("agent_status.{}", self.agent_id);
        if let Err(e) = self.client.publish(topic, status_json.into()).await {
            error!("Failed to send heartbeat: {}", e);
        }
    }

    pub async fn subscribe_commands(&self) -> anyhow::Result<async_nats::Subscriber> {
        let topic = format!("agent_commands.{}", self.agent_id);
        let sub = self.client.subscribe(topic).await?;
        Ok(sub)
    }
}
