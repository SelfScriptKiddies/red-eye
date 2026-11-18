use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, delete, get, web};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_config::ControlPlane;
use crate::api::block_sync;

/// Active temporary blocks: remote IP, agent, source filter, remaining
/// duration and manual unban.
#[utoipa::path(get, path = "/api/blocks", tag = "blocks",
    responses((status = 200, description = "Active blocks, newest first"),
              (status = 503, description = "Database pool exhausted")))]
#[get("/blocks")]
async fn get_blocks(
    _identity: Identity,
    pool: web::Data<PgPool>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100);
    match block_sync::list_blocks(&pool, limit).await {
        Ok(blocks) => HttpResponse::Ok().json(blocks),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Manual unban: removes the active block and advances the revision so the
/// agent drops its nftables rule promptly.
#[utoipa::path(delete, path = "/api/blocks/{id}", tag = "blocks",
    responses((status = 200, description = "Block removed and revision advanced"),
              (status = 404, description = "Block not found")))]
#[delete("/blocks/{id}")]
async fn unban_block(
    _identity: Identity,
    path: web::Path<(i64,)>,
    pool: web::Data<PgPool>,
    control: web::Data<Arc<ControlPlane>>,
) -> impl Responder {
    let _ = pool;
    let id = path.into_inner().0;
    match block_sync::unban_block(control.get_ref(), id).await {
        Ok(Some((agent_id, _revision))) => {
            // The revision bump schedules a fresh snapshot via the agent's
            // next heartbeat; force one immediately for prompt unban.
            control
                .send_snapshot(
                    &agent_id,
                    crate::api::agent::agent_config::SnapshotReason::Uncertain,
                )
                .await;
            HttpResponse::Ok().json(serde_json::json!({"status": "unbanned", "agent_id": agent_id}))
        }
        Ok(None) => HttpResponse::NotFound()
            .json(serde_json::json!({"error": "Block not found or already expired"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// Recent threshold alert/block events (reconnect and audit view).
#[utoipa::path(get, path = "/api/filter-events", tag = "blocks",
    responses((status = 200, description = "Recent filter events, newest first")))]
#[get("/filter-events")]
async fn get_filter_events(
    _identity: Identity,
    pool: web::Data<PgPool>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(100);
    match block_sync::list_filter_events(&pool, limit).await {
        Ok(events) => HttpResponse::Ok().json(events),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_blocks)
        .service(unban_block)
        .service(get_filter_events);
}
