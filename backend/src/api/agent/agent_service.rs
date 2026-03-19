use std::collections::HashMap;
use std::sync::Arc;

use async_nats::Client;
use log::{debug, error, info};
use tokio::sync::RwLock;

use super::agent_dto::AgentInfo;

/// How long before an agent is considered offline (seconds)
const AGENT_TIMEOUT_SECS: u64 = 30;

pub struct AgentService {
    agents: Arc<RwLock<HashMap<String, AgentInfo>>>,
    nats_client: Client,
}

impl AgentService {
    pub fn new(nats_client: Client) -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            nats_client,
        }
    }

    /// Update agent status from heartbeat data
    pub async fn update_status(&self, mut status: AgentInfo) {
        status.online = true;
        debug!("Heartbeat from agent '{}' (uptime: {}s)", status.agent_id, status.uptime_secs);
        self.agents.write().await.insert(status.agent_id.clone(), status);
    }

    /// Get all known agents with online/offline status
    pub async fn list_agents(&self) -> Vec<AgentInfo> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let agents = self.agents.read().await;
        agents
            .values()
            .map(|a| {
                let mut info = a.clone();
                info.online = (now_ms - a.timestamp_ms) < (AGENT_TIMEOUT_SECS * 1000);
                info
            })
            .collect()
    }

    /// Get a specific agent by ID
    pub async fn get_agent(&self, agent_id: &str) -> Option<AgentInfo> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let agents = self.agents.read().await;
        agents.get(agent_id).map(|a| {
            let mut info = a.clone();
            info.online = (now_ms - a.timestamp_ms) < (AGENT_TIMEOUT_SECS * 1000);
            info
        })
    }

    /// Send a command to a specific agent
    pub async fn send_command(&self, agent_id: &str, command: &serde_json::Value) -> Result<(), String> {
        let topic = format!("agent_commands.{}", agent_id);
        let payload = serde_json::to_vec(command).map_err(|e| e.to_string())?;

        self.nats_client
            .publish(topic, payload.into())
            .await
            .map_err(|e| format!("NATS publish error: {}", e))?;

        info!("Sent command to agent '{}'", agent_id);
        Ok(())
    }

    /// Send a command to multiple agents (or all if agent_ids is empty)
    pub async fn broadcast_command(
        &self,
        agent_ids: &[String],
        command: &serde_json::Value,
    ) -> Vec<(String, Result<(), String>)> {
        let targets: Vec<String> = if agent_ids.is_empty() {
            self.agents.read().await.keys().cloned().collect()
        } else {
            agent_ids.to_vec()
        };

        let mut results = Vec::new();
        for id in &targets {
            let result = self.send_command(id, command).await;
            results.push((id.clone(), result));
        }

        info!("Broadcast command to {} agents", targets.len());
        results
    }

    /// Get the inner agents map for the heartbeat subscriber
    pub fn agents_store(&self) -> Arc<RwLock<HashMap<String, AgentInfo>>> {
        self.agents.clone()
    }
}

/// Spawn NATS subscriber for agent heartbeats
pub fn spawn_heartbeat_listener(nats_client: Client, agent_service: Arc<AgentService>) {
    tokio::spawn(async move {
        let mut subscriber = match nats_client.subscribe("agent_status.*").await {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to agent heartbeats: {}", e);
                return;
            }
        };

        info!("Listening for agent heartbeats on agent_status.*");

        use futures_util::StreamExt;
        while let Some(msg) = subscriber.next().await {
            match serde_json::from_slice::<AgentInfo>(&msg.payload) {
                Ok(status) => {
                    agent_service.update_status(status).await;
                }
                Err(e) => {
                    debug!("Failed to parse agent heartbeat: {}", e);
                }
            }
        }
    });
}
