pub mod agent_config;
pub mod agent_dto;
pub mod agent_service;

use std::sync::Arc;

use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, get, post, put, web};

use serde::Deserialize;

use self::agent_config::{ControlPlane, NetworkSettingsUpdate, SnapshotReason};
use self::agent_dto::{AgentCommandPayload, BroadcastCommandPayload};
use self::agent_service::AgentService;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_all_agents)
        .service(get_agent_by_id)
        .service(send_agent_command)
        .service(broadcast_agent_command)
        .service(get_agent_services)
        .service(bind_agent_service)
        .service(unbind_agent_service)
        .service(get_agent_containers)
        .service(get_network_settings)
        .service(put_network_settings)
        .service(reset_network_settings);
}

#[utoipa::path(
    get,
    path = "/api/agent",
    responses(
        (status = 200, description = "List of all known agents"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[get("/agent")]
async fn get_all_agents(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    control: web::Data<Arc<ControlPlane>>,
) -> impl Responder {
    let agents = agent_service.list_agents().await;
    // Enrich with persistent per-agent network settings; an agent with no
    // agent_config row yet simply carries no network section.
    let mut enriched = Vec::with_capacity(agents.len());
    for agent in agents {
        let view = control.network_settings_view(&agent.agent_id).await;
        let value = match serde_json::to_value(&agent) {
            Ok(mut v) => {
                if let Some(view) = view {
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert(
                            "network".into(),
                            serde_json::to_value(view).unwrap_or_default(),
                        );
                    }
                }
                v
            }
            Err(_) => serde_json::value::Value::Null,
        };
        enriched.push(value);
    }
    HttpResponse::Ok().json(enriched)
}

#[utoipa::path(
    get,
    path = "/api/agent/{id}",
    responses(
        (status = 200, description = "Agent details"),
        (status = 404, description = "Agent not found"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[get("/agent/{id}")]
async fn get_agent_by_id(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    control: web::Data<Arc<ControlPlane>>,
    id: web::Path<String>,
) -> impl Responder {
    let agent_id = id.into_inner();
    match agent_service.get_agent(&agent_id).await {
        Some(agent) => {
            let mut value = serde_json::to_value(&agent).unwrap_or(serde_json::value::Value::Null);
            if let Some(view) = control.network_settings_view(&agent_id).await {
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(
                        "network".into(),
                        serde_json::to_value(view).unwrap_or_default(),
                    );
                }
            }
            HttpResponse::Ok().json(value)
        }
        None => HttpResponse::NotFound().json(serde_json::json!({"error": "Agent not found"})),
    }
}

#[utoipa::path(
    post,
    path = "/api/agent/{id}/command",
    responses(
        (status = 200, description = "Command sent successfully"),
        (status = 400, description = "Invalid command"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[post("/agent/{id}/command")]
async fn send_agent_command(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    id: web::Path<String>,
    command: web::Json<AgentCommandPayload>,
) -> impl Responder {
    match agent_service.send_command(&id, &command.inner).await {
        Ok(_) => HttpResponse::Ok().json(serde_json::json!({"status": "sent"})),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({"error": e})),
    }
}

#[utoipa::path(
    post,
    path = "/api/agent/broadcast",
    responses(
        (status = 200, description = "Command broadcast results"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[post("/agent/broadcast")]
async fn broadcast_agent_command(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    payload: web::Json<BroadcastCommandPayload>,
) -> impl Responder {
    let results = agent_service
        .broadcast_command(&payload.agent_ids, &payload.command)
        .await;

    let response: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(id, result)| {
            serde_json::json!({
                "agent_id": id,
                "status": if result.is_ok() { "sent" } else { "error" },
                "error": result.err()
            })
        })
        .collect();

    HttpResponse::Ok().json(response)
}

/// Docker containers running on the agent's host — feeds the UI's
/// "add service by container" flow.
///
/// Contract (polled, never blocking):
/// - Each call dispatches a `DockerListContainers` command to the agent
///   when the cached reply is missing or older than 60s; dispatch is
///   rate-limited to one per 2s per agent, so polling cannot spam agents.
/// - Always answers 200 with a `state` field:
///   * "ok"      — fresh (<60s) listing, `containers` is usable
///   * "error"   — fresh reply in which the agent reported a Docker error
///                 (socket missing / daemon down); `error` holds the message
///   * "pending" — no reply yet, command dispatched; the UI should show a
///                 spinner and re-poll once or twice (~1s apart), not loop
///   * "stale"   — cached reply older than 60s, command re-dispatched; the
///                 stale `containers`/`error` are still returned for display
/// - 404 when the agent has never been seen (no heartbeat).
#[utoipa::path(
    get,
    path = "/api/agent/{id}/containers",
    responses(
        (status = 200, description = "Container listing with state (ok|error|pending|stale)"),
        (status = 404, description = "Agent not found"),
        (status = 401, description = "Unauthorized")
    ),
    tag = "agent"
)]
#[get("/agent/{id}/containers")]
async fn get_agent_containers(
    _identity: Identity,
    agent_service: web::Data<Arc<AgentService>>,
    id: web::Path<String>,
) -> impl Responder {
    let agent_id = id.into_inner();
    if agent_service.get_agent(&agent_id).await.is_none() {
        return HttpResponse::NotFound().json(serde_json::json!({"error": "Agent not found"}));
    }
    HttpResponse::Ok().json(agent_service.get_containers(&agent_id).await)
}

// Agent network settings

#[get("/agent/{id}/network-settings")]
async fn get_network_settings(
    _identity: Identity,
    control: web::Data<Arc<ControlPlane>>,
    id: web::Path<String>,
) -> impl Responder {
    match control.network_settings_view(&id.into_inner()).await {
        Some(view) => HttpResponse::Ok().json(view),
        None => HttpResponse::NotFound()
            .json(serde_json::json!({"error": "No configuration recorded for this agent yet"})),
    }
}

#[put("/agent/{id}/network-settings")]
async fn put_network_settings(
    _identity: Identity,
    control: web::Data<Arc<ControlPlane>>,
    id: web::Path<String>,
    body: web::Json<NetworkSettingsUpdate>,
) -> impl Responder {
    let agent_id = id.into_inner();
    let bad = |msg: String| HttpResponse::BadRequest().json(serde_json::json!({ "error": msg }));

    let interface = body.interface.as_deref();
    let machine_ip = body.machine_ip.as_deref();

    if let Some(value) = interface {
        if !agent_config::valid_interface(value) {
            return bad("interface must be a non-empty name (max 64 chars, no whitespace)".into());
        }
    }
    if let Some(value) = machine_ip {
        if !agent_config::valid_machine_ip(value) {
            return bad(
                "machine_ip must be an IPv4/IPv6 address or the wildcard 0.0.0.0/::".into(),
            );
        }
    }

    match control
        .set_network_overrides(&agent_id, interface, machine_ip)
        .await
    {
        Ok(revision) => {
            // Best-effort delivery: failure leaves the change pending sync
            // (heartbeat boot detection and periodic reconciliation
            // re-deliver); the operator still gets a 2xx for the committed
            // write, flagged as pending.
            let sync = if control
                .send_snapshot(&agent_id, SnapshotReason::StaleRevision)
                .await
            {
                "synced"
            } else {
                "pending"
            };
            HttpResponse::Ok().json(serde_json::json!({
                "status": "ok",
                "revision": revision,
                "sync": sync
            }))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Reset the server-side overrides: the agent returns to its reported
/// bootstrap values (announced via heartbeat).
#[post("/agent/{id}/network-settings/reset")]
async fn reset_network_settings(
    _identity: Identity,
    control: web::Data<Arc<ControlPlane>>,
    id: web::Path<String>,
) -> impl Responder {
    let agent_id = id.into_inner();
    match control.set_network_overrides(&agent_id, None, None).await {
        Ok(revision) => {
            let sync = if control
                .send_snapshot(&agent_id, SnapshotReason::StaleRevision)
                .await
            {
                "synced"
            } else {
                "pending"
            };
            HttpResponse::Ok().json(serde_json::json!({
                "status": "reset",
                "revision": revision,
                "sync": sync
            }))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

// Agent-Service bindings (explicit assignment editing)

#[get("/agent/{id}/services")]
async fn get_agent_services(
    _identity: Identity,
    id: web::Path<String>,
    pool: web::Data<sqlx::PgPool>,
) -> impl Responder {
    let agent_id = id.into_inner();
    let services =
        sqlx::query_scalar::<_, i64>("SELECT service_id FROM agent_service WHERE agent_id = $1")
            .bind(&agent_id)
            .fetch_all(pool.get_ref())
            .await;

    match services {
        Ok(ids) => HttpResponse::Ok().json(ids),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[derive(Deserialize)]
struct BindRequest {
    service_id: i64,
}

/// Bind a service to an agent explicitly. Global services apply to every
/// agent already, so binding one is a no-op reported as such.
#[post("/agent/{id}/services/bind")]
async fn bind_agent_service(
    _identity: Identity,
    id: web::Path<String>,
    body: web::Json<BindRequest>,
    service_svc: web::Data<crate::api::service::service_service::ServiceService>,
    control: web::Data<Arc<ControlPlane>>,
) -> impl Responder {
    let agent_id = id.into_inner();
    match service_svc
        .bind_agent_service(&agent_id, body.service_id)
        .await
    {
        Ok((changed, revision)) => {
            if changed {
                // The agent picks the new set up via a revisioned snapshot.
                control
                    .send_snapshot(&agent_id, SnapshotReason::StaleRevision)
                    .await;
                HttpResponse::Ok()
                    .json(serde_json::json!({"status": "bound", "revision": revision}))
            } else {
                HttpResponse::Ok()
                    .json(serde_json::json!({"status": "global", "revision": revision}))
            }
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

#[post("/agent/{id}/services/unbind")]
async fn unbind_agent_service(
    _identity: Identity,
    id: web::Path<String>,
    body: web::Json<BindRequest>,
    service_svc: web::Data<crate::api::service::service_service::ServiceService>,
    control: web::Data<Arc<ControlPlane>>,
) -> impl Responder {
    let agent_id = id.into_inner();
    match service_svc
        .unbind_agent_service(&agent_id, body.service_id)
        .await
    {
        Ok((changed, revision, _removed)) => {
            if changed {
                control
                    .send_snapshot(&agent_id, SnapshotReason::StaleRevision)
                    .await;
                HttpResponse::Ok()
                    .json(serde_json::json!({"status": "unbound", "revision": revision}))
            } else {
                HttpResponse::Ok()
                    .json(serde_json::json!({"status": "global", "revision": revision}))
            }
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Bound agent-reported proxy error strings before they enter the
/// database: fixed-size diagnostics, never payload data.
fn sanitize_diag(error: &str) -> String {
    let mut out: String = error.chars().take(300).collect();
    if error.len() > 300 {
        out.push_str("...");
    }
    out
}

/// Spawn NATS subscriber for agent heartbeats
pub fn spawn_heartbeat_listener(
    nats_client: async_nats::Client,
    agent_service: Arc<AgentService>,
    control: Arc<ControlPlane>,
) {
    tokio::spawn(async move {
        let mut subscriber = match nats_client.subscribe("agent_status.*").await {
            Ok(sub) => sub,
            Err(e) => {
                log::error!("Failed to subscribe to agent heartbeats: {}", e);
                return;
            }
        };

        log::info!("Listening for agent heartbeats on agent_status.*");

        use futures_util::StreamExt;
        while let Some(msg) = subscriber.next().await {
            match serde_json::from_slice::<agent_dto::AgentInfo>(&msg.payload) {
                Ok(status) => {
                    let is_new = !agent_service
                        .agents_store()
                        .read()
                        .await
                        .contains_key(&status.agent_id);
                    let agent_id = status.agent_id.clone();
                    agent_service.update_status(status.clone()).await;

                    // Record reported state and decide whether the agent
                    // needs a snapshot: new boot id, acked revision behind
                    // the current one, or an uncertain DB round.
                    let reason = control
                        .record_heartbeat(
                            &agent_id,
                            status.boot_id.as_deref(),
                            status.interface.as_deref(),
                            status.machine_ip.as_deref(),
                            status.applied_revision.and_then(|r| i64::try_from(r).ok()),
                            status.last_sync_error.as_deref(),
                        )
                        .await;
                    if reason != SnapshotReason::NotNeeded {
                        let control = control.clone();
                        let agent_id = agent_id.clone();
                        tokio::spawn(async move {
                            control.send_snapshot(&agent_id, reason).await;
                        });
                    }

                    // Proxy diagnostics: store the agent-reported capture
                    // phase per (agent, service). Rows the backend does not
                    // know are ignored - reported state can never create or
                    // alter operator desired state.
                    for report in &status.proxies {
                        let service_id = match i64::try_from(report.service_id) {
                            Ok(id) => id,
                            Err(_) => continue,
                        };
                        match crate::api::protect::protect_repository::ProtectRepository::new(
                            control.pool().clone(),
                        )
                        .record_effective_diag(
                            &agent_id,
                            service_id,
                            report.state == "effective",
                            report
                                .effective_revision
                                .and_then(|r| i64::try_from(r).ok())
                                .unwrap_or(0),
                            &report.state,
                            report.last_error.as_deref().map(sanitize_diag).as_deref(),
                        )
                        .await
                        {
                            Ok(0) => {
                                log::debug!(
                                    "Proxy report for unknown desired row {} on agent '{}' ignored",
                                    service_id,
                                    agent_id
                                );
                            }
                            Ok(_) => {
                                if report.state == "failed" || report.state == "capture_unavailable"
                                {
                                    crate::api::metrics::PROXY_TRANSITION_FAILURES.inc();
                                }
                            }
                            Err(e) => {
                                log::debug!(
                                    "Failed to store proxy diagnostics for agent '{}': {}",
                                    agent_id,
                                    e
                                );
                            }
                        }
                    }
                    if let Some(conflicts) = status.capture_source_conflicts {
                        agent_service.set_capture_conflicts(&agent_id, conflicts);
                    }
                    if let Some(drops) = status.forwarder_drops {
                        agent_service.set_forwarder_drops(&agent_id, drops);
                    }

                    // Freshly seen agent (backend restart or first contact):
                    // push block filters. Service assignment rides the
                    // snapshot flow, not per-agent binding.
                    if is_new {
                        let control = control.clone();
                        let agent_svc2 = agent_service.clone();
                        let aid = agent_id.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                            log::info!("New agent '{}' connected, syncing block filters", aid);
                            crate::api::block_sync::sync_block_filters(control.pool(), &agent_svc2)
                                .await;
                        });
                    }
                }
                Err(e) => {
                    log::debug!("Failed to parse agent heartbeat: {}", e);
                }
            }
        }
    });
}

/// Spawn NATS subscriber for agent Docker container listings
/// (replies to the DockerListContainers command, published by agents on
/// `agent_docker.{agent_id}`). Same infrastructure as the heartbeat
/// listener: malformed payloads are dropped, the subscriber never dies.
pub fn spawn_docker_containers_listener(
    nats_client: async_nats::Client,
    agent_service: Arc<AgentService>,
) {
    tokio::spawn(async move {
        let subject = "agent_docker.*".to_string();
        let mut subscriber = match nats_client.subscribe(subject.clone()).await {
            Ok(sub) => sub,
            Err(e) => {
                log::error!("Failed to subscribe to {}: {}", subject, e);
                return;
            }
        };

        log::info!("Listening for agent docker listings on {}", subject);

        use futures_util::StreamExt;
        while let Some(msg) = subscriber.next().await {
            match serde_json::from_slice::<agent_dto::AgentDockerStatus>(&msg.payload) {
                Ok(status) => agent_service.update_docker_status(status).await,
                Err(e) => {
                    log::debug!("Failed to parse agent docker listing: {}", e);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_listing_is_ok() {
        assert_eq!(agent_service::classify_containers(0, false), "ok");
        assert_eq!(
            agent_service::classify_containers(agent_service::CONTAINERS_FRESH_MS - 1, false),
            "ok"
        );
    }

    #[test]
    fn fresh_error_reply_is_error() {
        assert_eq!(agent_service::classify_containers(100, true), "error");
    }

    #[test]
    fn old_listing_is_stale_regardless_of_error() {
        assert_eq!(
            agent_service::classify_containers(agent_service::CONTAINERS_FRESH_MS, false),
            "stale"
        );
        assert_eq!(agent_service::classify_containers(u64::MAX, true), "stale");
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

        let status: agent_dto::AgentDockerStatus =
            serde_json::from_str(wire).expect("parse agent wire JSON");
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
        let status: agent_dto::AgentDockerStatus = serde_json::from_str(wire).unwrap();
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
        let status: agent_dto::AgentDockerStatus = serde_json::from_str(wire).unwrap();
        assert_eq!(status.agent_id, "a1");
    }
}
