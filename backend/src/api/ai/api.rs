use actix_identity::Identity;
use actix_web::{HttpResponse, Responder, get, web};
use sqlx::PgPool;
use std::sync::Arc;

use super::repository::AiRepository;
use super::scheduler::JevScheduler;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

#[derive(serde::Deserialize, utoipa::ToSchema)]
pub struct VerdictsQuery {
    stream_id: Option<i64>,
    only_attacks: Option<bool>,
    limit: Option<i64>,
    cursor: Option<i64>,
}

const MODELS_BODY_CAP: usize = 1 << 20;
const MODELS_MAX_ENTRIES: usize = 500;

#[derive(serde::Deserialize)]
struct CatalogModels {
    data: Vec<CatalogModel>,
}

#[derive(serde::Deserialize)]
struct CatalogModel {
    id: String,
    context_length: Option<i64>,
}

/// One shared response shape for both model catalogs:
/// {"models": [{id, context_length}]} or {"models": [], "note": ...}.
/// context_length stays optional: OpenAI-compatible endpoints omit it.
fn models_response(models: Vec<serde_json::Value>) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "models": models }))
}

fn models_note(note: &str) -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"models": [], "note": note}))
}

fn parse_catalog(body: &str) -> Vec<serde_json::Value> {
    match serde_json::from_str::<CatalogModels>(body) {
        Ok(catalog) => catalog
            .data
            .into_iter()
            .take(MODELS_MAX_ENTRIES)
            .map(|m| serde_json::json!({"id": m.id, "context_length": m.context_length}))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// GET /api/ai/verdicts?stream_id=&only_attacks=&limit=&cursor=
/// Bounded keyset listing (cursor = previous page's last id), defaults to
/// the most recent verdicts.
#[utoipa::path(get, path = "/api/ai/verdicts", tag = "ai",
    responses((status = 200, description = "Verdict list")))]
#[get("/ai/verdicts")]
async fn get_verdicts(
    _identity: Identity,
    pool: web::Data<PgPool>,
    query: web::Query<VerdictsQuery>,
) -> impl Responder {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let repo = AiRepository::new(pool.get_ref().clone());
    match repo
        .list_verdicts(
            query.stream_id,
            query.only_attacks.unwrap_or(false),
            limit,
            query.cursor,
        )
        .await
    {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/ai/verdict/{id}: typed answers, model, threshold, cost,
/// context bounds and the contributing packet range.
#[utoipa::path(get, path = "/api/ai/verdict/{id}", tag = "ai",
    responses((status = 200, description = "Verdict detail"), (status = 404, description = "Not found")))]
#[get("/ai/verdict/{id}")]
async fn get_verdict(
    _identity: Identity,
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
) -> impl Responder {
    let repo = AiRepository::new(pool.get_ref().clone());
    match repo.get_verdict(path.into_inner()).await {
        Ok(Some(row)) => HttpResponse::Ok().json(row),
        Ok(None) => {
            HttpResponse::NotFound().json(serde_json::json!({"error": "verdict not found"}))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/ai/status: enablement, configuration, provider health,
/// today's cost usage, backlog and in-flight counts. No secrets: the API
/// key appears only as `configured`.
#[utoipa::path(get, path = "/api/ai/status", tag = "ai",
    responses((status = 200, description = "JEV status")))]
#[get("/ai/status")]
async fn get_status(
    _identity: Identity,
    pool: web::Data<PgPool>,
    scheduler: web::Data<Arc<JevScheduler>>,
) -> impl Responder {
    let settings = super::settings::AiSettings::load(pool.get_ref()).await;
    let repo = AiRepository::new(pool.get_ref().clone());
    let (recorded_cost, _) = repo.cost_today().await.unwrap_or((0.0, 0));
    let totals = repo.totals().await.unwrap_or((0, 0));
    let snap = scheduler.snapshot(recorded_cost).await;

    HttpResponse::Ok().json(serde_json::json!({
        "enabled": settings.enabled,
        "configured": settings.api_key_configured,
        "run_state": snap.run_state,
        "model": settings.model,
        "threshold": settings.threshold,
        "in_flight": snap.in_flight,
        "backlog": snap.backlog,
        "oldest_candidate_age_secs": snap.oldest_candidate_age_secs,
        "cost_spent_today": snap.cost_spent_today.max(recorded_cost),
        "cost_limit": settings.daily_cost_limit,
        "verdicts_total": totals.0,
        "attack_verdicts_total": totals.1,
        "calls_total": crate::api::metrics::AI_CALLS_TOTAL.get() as i64,
        "calls_failed": crate::api::metrics::AI_TRANSPORT_FAILURES.get() as i64,
        "invalid_responses": crate::api::metrics::AI_INVALID_RESPONSES.get() as i64,
        "last_success_unix": snap.last_success_unix,
        "last_error": snap.last_error,
    }))
}

/// GET /api/ai/models: OpenRouter catalog proxied with the stored key
/// (write-only stays write-only: the key only signs this upstream call).
/// No key means an empty list plus a note; upstream trouble degrades to
/// free text instead of an error page.
#[utoipa::path(get, path = "/api/ai/models", tag = "ai",
    responses((status = 200, description = "OpenRouter model catalog")))]
#[get("/ai/models")]
async fn get_models(_identity: Identity, pool: web::Data<PgPool>) -> impl Responder {
    let settings = super::settings::AiSettings::load(pool.get_ref()).await;
    if !settings.api_key_configured {
        return models_note("no_key");
    }
    match super::openrouter::fetch_json_bounded(
        "https://openrouter.ai/api/v1/models",
        Some(&settings.api_key),
        MODELS_BODY_CAP,
    )
    .await
    {
        Ok(body) => models_response(parse_catalog(&body)),
        Err(_) => models_note("upstream"),
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_verdicts)
        .service(get_verdict)
        .service(get_status)
        .service(get_models);
}
