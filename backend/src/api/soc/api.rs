use actix_identity::Identity;
use actix_web::http::header::RETRY_AFTER;
use actix_web::{HttpRequest, HttpResponse, Responder, get, post, web};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;

use super::bearer;
use super::matrix::{self, MatrixInput};
use super::repository::{EnqueueOutcome, ResultRejection, SocRepository};
use super::settings::{self, SocSettings};
use super::{ManualEnqueueError, broadcast_result};
use crate::api::realtime::BroadcastSender;

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;
/// Retry hint handed to manual submitters when the queue is saturated.
const QUEUE_FULL_RETRY_AFTER: &str = "30";
/// Model catalog proxy bounds: body cap, entry cap (shared shape with the
/// JEV /models endpoint).
const MODELS_BODY_CAP: usize = 1 << 20;
const MODELS_MAX_ENTRIES: usize = 500;

#[derive(Deserialize)]
struct CatalogModels {
    data: Vec<CatalogModel>,
}

#[derive(Deserialize)]
struct CatalogModel {
    id: String,
    context_length: Option<i64>,
}

// ---------- Operator endpoints ----------

#[derive(Deserialize, utoipa::ToSchema)]
pub struct TriageRequest {
    pub stream_id: i64,
}

/// GET /api/soc/models: OpenAI-compatible /models on the configured
/// endpoint, signed with the stored key when present. Failure degrades to
/// an empty list plus a terse note; the frontend falls back to free text.
#[utoipa::path(get, path = "/api/soc/models", tag = "soc",
    responses((status = 200, description = "Endpoint model catalog")))]
#[get("/soc/models")]
async fn get_models(_identity: Identity, pool: web::Data<PgPool>) -> impl Responder {
    let settings = SocSettings::load(pool.get_ref()).await;
    let url = format!("{}/models", settings.base_url.trim_end_matches('/'));
    let bearer = if settings.api_key_configured {
        Some(settings.api_key.as_str())
    } else {
        None
    };
    match crate::api::ai::openrouter::fetch_json_bounded(&url, bearer, MODELS_BODY_CAP).await {
        Ok(body) => match serde_json::from_str::<CatalogModels>(&body) {
            Ok(catalog) => {
                let models: Vec<serde_json::Value> = catalog
                    .data
                    .into_iter()
                    .take(MODELS_MAX_ENTRIES)
                    .map(|m| serde_json::json!({"id": m.id, "context_length": m.context_length}))
                    .collect();
                HttpResponse::Ok().json(json!({ "models": models }))
            }
            Err(_) => HttpResponse::Ok().json(json!({"models": [], "note": "upstream"})),
        },
        Err(_) => HttpResponse::Ok().json(json!({"models": [], "note": "upstream"})),
    }
}

/// POST /api/soc/triage?fresh=true {stream_id}: manual triage. Works with
/// only soc_enabled (no JEV requirement). Duplicate submissions against
/// an active job for the same revision collapse into the existing job;
/// fresh=true forces a new one.
#[utoipa::path(post, path = "/api/soc/triage", tag = "soc",
    responses((status = 200, description = "Job queued or deduplicated"),
              (status = 404, description = "Stream not found"),
              (status = 409, description = "SOC disabled or stream has no revision"),
              (status = 503, description = "Queue at capacity")))]
#[post("/soc/triage")]
async fn submit_triage(
    identity: Identity,
    pool: web::Data<PgPool>,
    query: web::Query<FreshQuery>,
    body: web::Json<TriageRequest>,
) -> impl Responder {
    let requested_by = identity.id().unwrap_or_default();
    match super::enqueue_manual(
        pool.get_ref(),
        body.stream_id,
        query.fresh.unwrap_or(false),
        Some(&requested_by),
    )
    .await
    {
        Ok(EnqueueOutcome::Created(job_id)) => HttpResponse::Ok().json(json!({
            "job_id": job_id, "deduplicated": false,
        })),
        Ok(EnqueueOutcome::DuplicateExisting(job_id)) => HttpResponse::Ok().json(json!({
            "job_id": job_id, "deduplicated": true,
        })),
        Ok(EnqueueOutcome::DroppedQueueFull) => queue_full_response(),
        Err(ManualEnqueueError::SocDisabled(reason)) => {
            HttpResponse::Conflict().json(json!({"error": reason}))
        }
        Err(ManualEnqueueError::StreamNotFound) => {
            HttpResponse::NotFound().json(json!({"error": "stream not found"}))
        }
        Err(ManualEnqueueError::NoRevision) => HttpResponse::Conflict().json(json!({
            "error": "stream has no analyzable revision (no flushed packets yet)"
        })),
        Err(ManualEnqueueError::Db(e)) => crate::api::sqlx_error_response(&e),
    }
}

fn queue_full_response() -> HttpResponse {
    HttpResponse::ServiceUnavailable()
        .insert_header((RETRY_AFTER, QUEUE_FULL_RETRY_AFTER))
        .json(json!({"error": "SOC queue at capacity; try again later"}))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FreshQuery {
    pub fresh: Option<bool>,
}

/// GET /api/soc/status: effective configuration-matrix state, queue
/// depth, leases, daily usage, worker/MCP last-seen and sanitized recent
/// failures. No secrets: every credential appears only as a configured
/// flag.
#[utoipa::path(get, path = "/api/soc/status", tag = "soc",
    responses((status = 200, description = "SOC status")))]
#[get("/soc/status")]
async fn get_status(
    pool: web::Data<PgPool>,
    jev_scheduler: web::Data<Arc<crate::api::ai::scheduler::JevScheduler>>,
) -> impl Responder {
    let soc = SocSettings::load(pool.get_ref()).await;
    let ai = crate::api::ai::settings::AiSettings::load(pool.get_ref()).await;
    let repo = SocRepository::new(pool.get_ref().clone());

    let (queue_depth, oldest_wait, active_leases) = repo.queue_stats().await.unwrap_or((0, 0, 0));
    let used_today = repo.jobs_used_today().await.unwrap_or(0);

    // Claims pause on disabled SOC or the daily limit (matrix rows 7-9
    // surface through this reason).
    let claims_paused = repo
        .claim_gate(soc.enabled, soc.daily_job_limit)
        .await
        .ok()
        .filter(|(allowed, _)| !allowed)
        .map(|(_, reason)| reason.to_string());

    // JEV provider failing: the scheduler's run state (backoff) is the
    // observed signal; a stale config alone is not.
    let jev_failing = ai.enabled && ai.api_key_configured && {
        let snap = jev_scheduler.snapshot(0.0).await;
        snap.run_state == "backoff"
    };

    let state = matrix::effective_state(&MatrixInput {
        jev_enabled: ai.enabled,
        jev_configured: ai.api_key_configured,
        jev_provider_failing: jev_failing,
        soc_enabled: soc.enabled,
        soc_auto_enabled: soc.auto_enabled,
        claims_paused_reason: claims_paused.clone(),
    });

    let worker_last_seen = read_last_seen(pool.get_ref(), "soc_worker_last_seen").await;
    let mcp_last_seen = read_last_seen(pool.get_ref(), "mcp_service_last_seen").await;
    let mcp_capability =
        crate::api::settings::get_setting(pool.get_ref(), settings::MCP_SERVICE_CAPABILITY)
            .await
            .unwrap_or_else(|| "read_only".to_string());
    let recent_errors: Vec<serde_json::Value> = repo
        .recent_errors(5)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(job_id, error, at)| json!({"job_id": job_id, "error": error, "at_unix": at}))
        .collect();

    HttpResponse::Ok().json(json!({
        "enabled": soc.enabled,
        "auto_enabled": soc.auto_enabled,
        "model": soc.model,
        "base_url": soc.base_url,
        "api_key_configured": soc.api_key_configured,
        "worker_token_configured": configured(pool.get_ref(), settings::SOC_WORKER_TOKEN).await,
        "mcp_service_token_configured": configured(pool.get_ref(), settings::MCP_SERVICE_TOKEN).await,
        "mcp_service_capability": mcp_capability,
        "alert_threshold": soc.alert_threshold,
        "auto_min_jev_severity": soc.auto_min_jev_severity,
        "limits": {
            "max_context_tokens": soc.max_context_tokens,
            "max_output_tokens": soc.max_output_tokens,
            "max_tool_calls": soc.max_tool_calls,
            "max_tool_result_bytes": soc.max_tool_result_bytes,
            "job_timeout_secs": soc.job_timeout_secs,
            "max_attempts": soc.max_attempts,
            "concurrency": soc.concurrency,
            "daily_job_limit": soc.daily_job_limit,
            "queue_cap": settings::queue_cap(),
        },
        "prompt_version": settings::SOC_PROMPT_VERSION,
        "settings_generation": soc.settings_generation,
        "matrix": state,
        "claims_paused_reason": claims_paused,
        "queue": {
            "depth": queue_depth,
            "oldest_wait_secs": oldest_wait,
            "active_leases": active_leases,
            "jobs_used_today": used_today,
        },
        "worker_last_seen_unix": worker_last_seen,
        "mcp_last_seen_unix": mcp_last_seen,
        "recent_errors": recent_errors,
    }))
}

async fn configured(pool: &PgPool, key: &str) -> bool {
    crate::api::settings::get_setting(pool, key)
        .await
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

async fn read_last_seen(pool: &PgPool, key: &str) -> Option<i64> {
    crate::api::settings::get_setting(pool, key)
        .await
        .and_then(|v| v.trim().parse::<i64>().ok())
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ResultsQuery {
    stream_id: Option<i64>,
    limit: Option<i64>,
    cursor: Option<i64>,
}

/// GET /api/soc/results?stream_id=&limit=&cursor=: validated triage
/// output only; no chain-of-thought field exists anywhere.
#[utoipa::path(get, path = "/api/soc/results", tag = "soc",
    responses((status = 200, description = "Result list")))]
#[get("/soc/results")]
async fn list_results(
    _identity: Identity,
    pool: web::Data<PgPool>,
    query: web::Query<ResultsQuery>,
) -> impl Responder {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let repo = SocRepository::new(pool.get_ref().clone());
    match repo
        .list_results(query.stream_id, limit, query.cursor)
        .await
    {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// GET /api/soc/result/{id}
#[utoipa::path(get, path = "/api/soc/result/{id}", tag = "soc",
    responses((status = 200, description = "Result detail"), (status = 404, description = "Not found")))]
#[get("/soc/result/{id}")]
async fn get_result(
    _identity: Identity,
    pool: web::Data<PgPool>,
    path: web::Path<i64>,
) -> impl Responder {
    let repo = SocRepository::new(pool.get_ref().clone());
    match repo.get_result(path.into_inner()).await {
        Ok(Some(row)) => HttpResponse::Ok().json(row),
        Ok(None) => HttpResponse::NotFound().json(json!({"error": "result not found"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// POST /api/soc/worker-token/generate[?rotate=true]: one-time plaintext
/// display of the soc-worker job credential. Stored only as a SHA-256
/// hash; rotate invalidates the previous value.
#[utoipa::path(post, path = "/api/soc/worker-token/generate", tag = "soc",
    responses((status = 200, description = "Generated token (plaintext shown once)")))]
#[post("/soc/worker-token/generate")]
async fn generate_worker_token(
    pool: web::Data<PgPool>,
    query: web::Query<RotateQuery>,
) -> impl Responder {
    generate_credential(
        pool,
        settings::SOC_WORKER_TOKEN,
        query.rotate.unwrap_or(false),
    )
    .await
}

/// POST /api/soc/mcp-service-token/generate[?rotate=true]: the red-eye-mcp
/// service credential for backend API calls.
#[utoipa::path(post, path = "/api/soc/mcp-service-token/generate", tag = "soc",
    responses((status = 200, description = "Generated token (plaintext shown once)")))]
#[post("/soc/mcp-service-token/generate")]
async fn generate_mcp_service_token(
    pool: web::Data<PgPool>,
    query: web::Query<RotateQuery>,
) -> impl Responder {
    generate_credential(
        pool,
        settings::MCP_SERVICE_TOKEN,
        query.rotate.unwrap_or(false),
    )
    .await
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct RotateQuery {
    pub rotate: Option<bool>,
}

async fn generate_credential(pool: web::Data<PgPool>, key: &str, rotate: bool) -> HttpResponse {
    let existing = crate::api::settings::get_setting(pool.get_ref(), key)
        .await
        .unwrap_or_default();
    if !existing.trim().is_empty() && !rotate {
        return HttpResponse::Conflict().json(json!({
            "error": "credential already generated; call again with rotate=true to replace it",
            "configured": true,
        }));
    }
    let plaintext = match bearer::generate_token() {
        Ok(t) => t,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(json!({"error": format!("token generation failed: {}", e)}));
        }
    };
    let hash = bearer::token_hash(&plaintext);
    let write = sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2",
    )
    .bind(key)
    .bind(&hash)
    .execute(pool.get_ref())
    .await;
    match write {
        Ok(_) => {
            log::info!("{} regenerated (plaintext shown once)", key);
            HttpResponse::Ok().json(json!({
                "token": plaintext,
                "note": "store this value now; it is never shown again",
            }))
        }
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

// ---------- Worker endpoints (Bearer soc_worker_token) ----------

/// GET /api/soc/worker-config: the worker's runtime configuration. This
/// is the ONLY place the LLM API key leaves the backend (write-only
/// everywhere else, operator UI included); it travels to the
/// authenticated worker over the internal compose network.
#[get("/soc/worker-config")]
async fn worker_config(pool: web::Data<PgPool>, req: HttpRequest) -> impl Responder {
    if let Err(resp) = bearer::require_service_token(
        pool.get_ref(),
        &req,
        settings::SOC_WORKER_TOKEN,
        Some("soc_worker_last_seen"),
    )
    .await
    {
        return resp;
    }
    let soc = SocSettings::load(pool.get_ref()).await;
    HttpResponse::Ok().json(json!({
        "enabled": soc.enabled,
        "base_url": soc.base_url,
        "api_key": soc.api_key,
        "model": soc.model,
        "prompt_version": settings::SOC_PROMPT_VERSION,
        "settings_generation": soc.settings_generation,
        "limits": {
            "max_context_tokens": soc.max_context_tokens,
            "max_output_tokens": soc.max_output_tokens,
            "max_tool_calls": soc.max_tool_calls,
            "max_tool_result_bytes": soc.max_tool_result_bytes,
            "job_timeout_secs": soc.job_timeout_secs,
            "max_attempts": soc.max_attempts,
            "concurrency": soc.concurrency,
        },
    }))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct ClaimRequest {
    pub worker_id: String,
    pub lease_secs: Option<i64>,
}

/// POST /api/soc/jobs/claim: transactional claim with a bounded lease.
/// Claims pause (204, explicit) while SOC is disabled or the daily job
/// limit is spent - visible, never an error loop on the worker side.
#[utoipa::path(post, path = "/api/soc/jobs/claim", tag = "soc",
    responses((status = 200, description = "Claimed job"),
              (status = 204, description = "No claimable job now"),
              (status = 409, description = "Claims paused (disabled or daily limit)")))]
#[post("/soc/jobs/claim")]
async fn claim_job(
    pool: web::Data<PgPool>,
    req: HttpRequest,
    body: web::Json<ClaimRequest>,
) -> impl Responder {
    if let Err(resp) = bearer::require_service_token(
        pool.get_ref(),
        &req,
        settings::SOC_WORKER_TOKEN,
        Some("soc_worker_last_seen"),
    )
    .await
    {
        return resp;
    }
    let soc = SocSettings::load(pool.get_ref()).await;
    let repo = SocRepository::new(pool.get_ref().clone());
    match repo.claim_gate(soc.enabled, soc.daily_job_limit).await {
        Ok((true, _)) => {}
        Ok((false, reason)) => {
            crate::api::metrics::SOC_CLAIMS_REJECTED.inc();
            return HttpResponse::Conflict().json(json!({"paused": true, "reason": reason}));
        }
        Err(e) => return crate::api::sqlx_error_response(&e),
    }
    let worker_id = sanitize_worker_id(&body.worker_id);
    let lease_secs = body
        .lease_secs
        .unwrap_or(soc.job_timeout_secs)
        .clamp(10, 3600);
    match repo.claim_job(&worker_id, lease_secs).await {
        Ok(Some(job)) => HttpResponse::Ok().json(json!({
            "job": job,
            "lease_secs": lease_secs,
            "max_attempts": soc.max_attempts,
        })),
        Ok(None) => HttpResponse::NoContent().finish(),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

fn sanitize_worker_id(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
        .take(64)
        .collect::<String>()
        .trim_start_matches(['-', '.'])
        .to_string()
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FetchedRequest {
    pub entries: Vec<String>,
}

/// POST /api/soc/jobs/{id}/fetched: the worker records every packet or
/// message id it actually received via MCP. Result evidence ids must be a
/// subset of this set; the model cannot invent one the worker never saw.
#[post("/soc/jobs/{id}/fetched")]
async fn record_fetched(
    pool: web::Data<PgPool>,
    req: HttpRequest,
    path: web::Path<i64>,
    body: web::Json<FetchedRequest>,
) -> impl Responder {
    if let Err(resp) =
        bearer::require_service_token(pool.get_ref(), &req, settings::SOC_WORKER_TOKEN, None).await
    {
        return resp;
    }
    let entries: Vec<String> = body
        .entries
        .iter()
        .filter(|e| {
            let e = e.trim();
            (e.starts_with("packet:") || e.starts_with("message:"))
                && e[7..].chars().all(|c| c.is_ascii_digit())
                && e.len() <= 32
        })
        .map(|e| e.trim().to_string())
        .take(100)
        .collect();
    let repo = SocRepository::new(pool.get_ref().clone());
    match repo
        .record_fetched_evidence(path.into_inner(), entries)
        .await
    {
        Ok(Some(set)) => HttpResponse::Ok().json(json!({"fetched": set.len()})),
        Ok(None) => HttpResponse::NotFound().json(json!({"error": "job not found"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

/// POST /api/soc/jobs/{id}/result: strict validation pipeline - active
/// job state, revision/model/prompt/generation identity, schema bounds,
/// evidence subset - then insert + complete. A rejection is a typed 422
/// the worker may retry within its attempts budget; nothing is ever
/// stored for a rejected payload.
#[post("/soc/jobs/{id}/result")]
async fn submit_result(
    pool: web::Data<PgPool>,
    broadcaster: web::Data<BroadcastSender>,
    req: HttpRequest,
    path: web::Path<i64>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    if let Err(resp) =
        bearer::require_service_token(pool.get_ref(), &req, settings::SOC_WORKER_TOKEN, None).await
    {
        return resp;
    }
    let job_id = path.into_inner();
    let soc = SocSettings::load(pool.get_ref()).await;
    if !soc.enabled {
        // Disabled between claim and submit: the job's generation is stale
        // by definition; reject without storing.
        crate::api::metrics::SOC_RESULT_REJECTIONS.inc();
        return HttpResponse::UnprocessableEntity().json(json!({
            "error": {"code": "stale_generation", "message": "SOC is disabled"}
        }));
    }
    let repo = SocRepository::new(pool.get_ref().clone());
    let fetched = match repo.get_job(job_id).await {
        Ok(Some(job)) => job
            .fetched_evidence
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        Ok(None) => {
            crate::api::metrics::SOC_RESULT_REJECTIONS.inc();
            return HttpResponse::UnprocessableEntity().json(json!({
                "error": {"code": "job_not_active", "message": "job not found"}
            }));
        }
        Err(e) => return crate::api::sqlx_error_response(&e),
    };

    let validated = super::validation::validate_result_payload(job_id, &body, &fetched);
    let validated = match validated {
        Ok(v) => v,
        Err(rej) => return rejection_response(rej),
    };

    match repo
        .submit_result(&validated, soc.settings_generation)
        .await
    {
        Ok(Ok(result_id)) => {
            let row = repo.get_result(result_id).await.ok().flatten();
            if let Some(row) = row {
                broadcast_result(&broadcaster, &row, soc.alert_threshold);
            }
            HttpResponse::Ok().json(json!({"result_id": result_id, "status": "succeeded"}))
        }
        Ok(Err(rej)) => rejection_response(rej),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

fn rejection_response(rej: ResultRejection) -> HttpResponse {
    crate::api::metrics::SOC_RESULT_REJECTIONS.inc();
    if rej.message().contains("was not fetched") {
        crate::api::metrics::SOC_EVIDENCE_REJECTIONS.inc();
    }
    HttpResponse::UnprocessableEntity().json(json!({
        "error": {"code": rej.code(), "message": rej.message()}
    }))
}

#[derive(Deserialize, utoipa::ToSchema)]
pub struct FailureRequest {
    pub error: String,
    /// permanent = provider 4xx schema/authentication: do not retry the
    /// identical request until settings change.
    pub permanent: Option<bool>,
}

/// POST /api/soc/jobs/{id}/failure: bounded retries with exponential
/// backoff; attempt exhaustion (or a permanent classification) fails the
/// job visibly. Failure never means benign.
#[post("/soc/jobs/{id}/failure")]
async fn submit_failure(
    pool: web::Data<PgPool>,
    req: HttpRequest,
    path: web::Path<i64>,
    body: web::Json<FailureRequest>,
) -> impl Responder {
    if let Err(resp) =
        bearer::require_service_token(pool.get_ref(), &req, settings::SOC_WORKER_TOKEN, None).await
    {
        return resp;
    }
    let soc = SocSettings::load(pool.get_ref()).await;
    let repo = SocRepository::new(pool.get_ref().clone());
    match repo
        .record_failure(
            path.into_inner(),
            &body.error,
            body.permanent.unwrap_or(false),
            soc.max_attempts,
        )
        .await
    {
        Ok(Some(status)) => {
            if status == "failed" {
                crate::api::metrics::SOC_JOBS_FAILED_TOTAL.inc();
            } else {
                crate::api::metrics::SOC_JOBS_RETRIED.inc();
            }
            HttpResponse::Ok().json(json!({"status": status}))
        }
        Ok(None) => HttpResponse::NotFound().json(json!({"error": "no active job to fail"})),
        Err(e) => crate::api::sqlx_error_response(&e),
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(submit_triage)
        .service(get_models)
        .service(get_status)
        .service(list_results)
        .service(get_result)
        .service(generate_worker_token)
        .service(generate_mcp_service_token)
        .service(worker_config)
        .service(claim_job)
        .service(record_fetched)
        .service(submit_result)
        .service(submit_failure);
}

#[cfg(test)]
mod tests {
    use super::sanitize_worker_id;

    #[test]
    fn worker_id_is_sanitized_and_bounded() {
        assert_eq!(sanitize_worker_id("worker-1_alpha.x"), "worker-1_alpha.x");
        assert_eq!(sanitize_worker_id("bad id\nwith;sql"), "badidwithsql");
        let long = sanitize_worker_id(&"w".repeat(200));
        assert_eq!(long.len(), 64);
        assert_eq!(sanitize_worker_id("--x"), "x");
        assert_eq!(sanitize_worker_id(""), "");
    }
}
