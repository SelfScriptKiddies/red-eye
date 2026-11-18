use std::collections::HashMap;
use std::sync::Arc;

use async_nats::Client;
use log::{debug, info, warn};
use tokio::sync::RwLock;

use super::agent_dto::{AgentContainersResponse, AgentDockerStatus, AgentInfo};

/// How long before an agent is considered offline (seconds)
const AGENT_TIMEOUT_SECS: u64 = 30;

/// How long a cached Docker container listing is served without re-asking
/// the agent (the UI polls this only when the operator opens the
/// "add by container" modal, so 60s keeps agents undisturbed mid-game).
pub const CONTAINERS_FRESH_MS: u64 = 60_000;

/// Minimum spacing between DockerListContainers dispatches to the same
/// agent, so UI re-polls (and multiple operator tabs) cannot spam it.
const CONTAINERS_REQUEST_MIN_INTERVAL_MS: u64 = 2_000;

/// Subject the agents publish their Docker listings on (per-agent suffix).
const AGENT_DOCKER_SUBJECT_PREFIX: &str = "agent_docker.";

#[derive(Clone, Default)]
struct CachedDocker {
    /// Latest agent reply; `None` until the first reply arrives (the entry
    /// still exists then, purely to rate-limit command dispatches).
    status: Option<AgentDockerStatus>,
    /// When the latest reply arrived (0 = never).
    received_ms: u64,
    /// When we last dispatched a DockerListContainers command (0 = never).
    last_request_ms: u64,
}

pub struct AgentService {
    agents: Arc<RwLock<HashMap<String, AgentInfo>>>,
    docker_containers: Arc<RwLock<HashMap<String, CachedDocker>>>,
    /// Latest capture-source conflict counters per agent (diagnostic
    /// gauge source; std lock: tiny values, sync heartbeat path).
    capture_conflicts: Arc<std::sync::RwLock<HashMap<String, u64>>>,
    forwarder_drops: Arc<std::sync::RwLock<HashMap<String, u64>>>,
    nats_client: Client,
}

impl AgentService {
    pub fn new(nats_client: Client) -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            docker_containers: Arc::new(RwLock::new(HashMap::new())),
            capture_conflicts: Arc::new(std::sync::RwLock::new(HashMap::new())),
            forwarder_drops: Arc::new(std::sync::RwLock::new(HashMap::new())),
            nats_client,
        }
    }

    /// Record the agent's latest capture-source conflict counter.
    pub fn set_capture_conflicts(&self, agent_id: &str, value: u64) {
        self.capture_conflicts
            .write()
            .unwrap()
            .insert(agent_id.to_string(), value);
    }

    pub fn capture_conflicts_total(&self) -> u64 {
        self.capture_conflicts.read().unwrap().values().sum()
    }

    /// Record the agent's latest harpoon forwarder drop counter.
    pub fn set_forwarder_drops(&self, agent_id: &str, value: u64) {
        self.forwarder_drops
            .write()
            .unwrap()
            .insert(agent_id.to_string(), value);
    }

    pub fn forwarder_drops_total(&self) -> u64 {
        self.forwarder_drops.read().unwrap().values().sum()
    }

    /// Update agent status from heartbeat data
    pub async fn update_status(&self, mut status: AgentInfo) {
        status.online = true;
        debug!(
            "Heartbeat from agent '{}' (uptime: {}s)",
            status.agent_id, status.uptime_secs
        );
        self.agents
            .write()
            .await
            .insert(status.agent_id.clone(), status);
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
    pub async fn send_command(
        &self,
        agent_id: &str,
        command: &serde_json::Value,
    ) -> Result<(), String> {
        // NATS is fire-and-forget: a command for a dead agent vanishes
        // with nobody the wiser (the UI showed "sent"). Refuse instead —
        // no heartbeat within the timeout means nobody is listening.
        match self.get_agent(agent_id).await {
            None => {
                return Err(format!(
                    "agent '{agent_id}' is not known (never sent a heartbeat)"
                ));
            }
            Some(info) if !info.online => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let stale_secs = (now_ms.saturating_sub(info.timestamp_ms)) / 1000;
                return Err(format!(
                    "agent '{agent_id}' is offline (no heartbeat for {stale_secs}s)"
                ));
            }
            _ => {}
        }

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
            // Broadcast reaches only agents that can actually receive:
            // dead registry entries would swallow the command silently
            // (send_command now refuses them anyway) and inflate the
            // "N agents" count the UI reports.
            self.list_agents()
                .await
                .into_iter()
                .filter(|a| a.online)
                .map(|a| a.agent_id)
                .collect()
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

    /// Store a Docker containers reply received from an agent.
    pub async fn update_docker_status(&self, status: AgentDockerStatus) {
        debug!(
            "Docker listing from agent '{}' ({} containers{})",
            status.agent_id,
            status.containers.len(),
            status
                .error
                .as_ref()
                .map(|e| format!(", error: {}", e))
                .unwrap_or_default()
        );
        let received_ms = now_ms();
        let mut cache = self.docker_containers.write().await;
        let entry = cache.entry(status.agent_id.clone()).or_default();
        entry.status = Some(status);
        entry.received_ms = received_ms;
    }

    /// Serve the containers view for `GET /api/agent/{id}/containers`:
    /// returns cached data with its state, and dispatches a fresh
    /// `DockerListContainers` command when the cache is missing or stale
    /// (rate-limited per agent). Never blocks on the agent's reply — the
    /// UI polls once or twice with a spinner instead.
    pub async fn get_containers(&self, agent_id: &str) -> AgentContainersResponse {
        let now = now_ms();

        let cached = { self.docker_containers.read().await.get(agent_id).cloned() };

        // Dispatch a fresh request when there is nothing usable. Rate-limit
        // so polling UIs / multiple tabs cannot hammer the agent.
        let needs_request = match &cached {
            None => true,
            Some(c) => {
                c.received_ms == 0 || now.saturating_sub(c.received_ms) >= CONTAINERS_FRESH_MS
            }
        };
        if needs_request {
            let sent_recently = cached
                .as_ref()
                .map(|c| {
                    c.last_request_ms != 0
                        && now.saturating_sub(c.last_request_ms)
                            < CONTAINERS_REQUEST_MIN_INTERVAL_MS
                })
                .unwrap_or(false);
            if !sent_recently {
                self.docker_containers
                    .write()
                    .await
                    .entry(agent_id.to_string())
                    .or_default()
                    .last_request_ms = now;
                let cmd = serde_json::json!({ "type": "DockerListContainers" });
                if let Err(e) = self.send_command(agent_id, &cmd).await {
                    warn!(
                        "Failed to request containers from agent '{}': {}",
                        agent_id, e
                    );
                }
            }
        }

        match cached.and_then(|c| c.status.map(|s| (s, c.received_ms))) {
            None => AgentContainersResponse {
                agent_id: agent_id.to_string(),
                state: "pending",
                containers: Vec::new(),
                error: None,
                age_ms: None,
            },
            Some((status, received_ms)) => {
                let age = now.saturating_sub(received_ms);
                AgentContainersResponse {
                    agent_id: agent_id.to_string(),
                    state: classify_containers(age, status.error.is_some()),
                    containers: status.containers,
                    error: status.error,
                    age_ms: Some(age),
                }
            }
        }
    }
}

/// Pure classifier for the containers response state, kept standalone for
/// unit tests: fresh+ok → "ok", fresh+error → "error", old → "stale",
/// nothing cached → "pending" (handled by the caller).
pub(crate) fn classify_containers(age_ms: u64, has_error: bool) -> &'static str {
    if age_ms < CONTAINERS_FRESH_MS {
        if has_error { "error" } else { "ok" }
    } else {
        "stale"
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_listing_is_ok() {
        assert_eq!(classify_containers(0, false), "ok");
        assert_eq!(classify_containers(CONTAINERS_FRESH_MS - 1, false), "ok");
    }

    #[test]
    fn fresh_error_reply_is_error() {
        assert_eq!(classify_containers(100, true), "error");
    }

    #[test]
    fn old_listing_is_stale_regardless_of_error() {
        assert_eq!(classify_containers(CONTAINERS_FRESH_MS, false), "stale");
        assert_eq!(classify_containers(u64::MAX, true), "stale");
    }

    /// DTO symmetry with the agent: this JSON was produced by
    /// `cargo run -p red-eye-agent --example list_containers -- nats://127.0.0.1:4223`
    /// against the real Docker daemon and a scratch NATS broker on this
    /// machine (2026-10-08) — the exact bytes received back from
    /// `agent_docker.example-agent` through the broker, i.e. what the
    /// `agent_docker.*` subscriber below parses.
    #[test]
    fn agent_wire_payload_deserializes_into_backend_dto() {
        let wire = r#"{
          "agent_id": "example-agent",
          "timestamp_ms": 1791464379330,
          "containers": [
            {
              "name": "reg-nats",
              "image": "nats:alpine",
              "state": "running",
              "ports": [
                { "port": 4223, "protocol": "tcp", "published": true }
              ]
            },
            {
              "name": "mantis-wave5-pg",
              "image": "postgres:16-alpine",
              "state": "running",
              "ports": [
                { "port": 55433, "protocol": "tcp", "published": true }
              ]
            },
            {
              "name": "playwright-mcp",
              "image": "mcr.microsoft.com/playwright/mcp:latest",
              "state": "running",
              "ports": []
            }
          ]
        }"#;

        let status: AgentDockerStatus = serde_json::from_str(wire).expect("parse agent wire JSON");
        assert_eq!(status.agent_id, "example-agent");
        assert_eq!(status.error, None);
        assert_eq!(status.containers.len(), 3);

        let nats = &status.containers[0];
        assert_eq!(nats.name, "reg-nats");
        assert_eq!(nats.state, "running");
        assert_eq!(nats.ports.len(), 1);
        assert_eq!(nats.ports[0].port, 4223);
        assert_eq!(nats.ports[0].protocol, "tcp");
        assert!(nats.ports[0].published);

        let pg = &status.containers[1];
        assert_eq!(pg.name, "mantis-wave5-pg");
        assert_eq!(pg.ports.len(), 1);
        assert_eq!(pg.ports[0].port, 55433);
        assert!(pg.ports[0].published);

        assert!(status.containers[2].ports.is_empty());
    }

    /// The error shape the agent sends when Docker is unreachable — field
    /// names must match agent/src/docker.rs DockerContainersStatus.
    #[test]
    fn agent_error_payload_deserializes_into_backend_dto() {
        let wire = r#"{
          "agent_id": "a1",
          "timestamp_ms": 5,
          "error": "docker connect failed: SocketNotFoundError",
          "containers": []
        }"#;
        let status: AgentDockerStatus = serde_json::from_str(wire).unwrap();
        assert_eq!(
            status.error.as_deref(),
            Some("docker connect failed: SocketNotFoundError")
        );
        assert!(status.containers.is_empty());
    }

    /// Backward/forward tolerance: unknown extra fields must not break
    /// parsing (serde default behavior for these DTOs).
    #[test]
    fn agent_payload_with_unknown_fields_still_parses() {
        let wire = r#"{
          "agent_id": "a1",
          "timestamp_ms": 5,
          "extra_future_field": {"x": 1},
          "containers": []
        }"#;
        let status: AgentDockerStatus = serde_json::from_str(wire).unwrap();
        assert_eq!(status.agent_id, "a1");
    }
}
