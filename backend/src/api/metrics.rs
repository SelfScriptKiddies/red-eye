use actix_web::{HttpResponse, Responder, get, web};
use prometheus::{Encoder, IntCounter, IntGauge, Opts, Registry, TextEncoder};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::agent::agent_service::AgentService;

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Counters (monotonically increasing)
    pub static ref PACKETS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_packets_total", "Total packets processed")
    ).unwrap();
    pub static ref STREAMS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_streams_total", "Total streams created")
    ).unwrap();
    pub static ref HTTP_REQUESTS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_http_requests_total", "Total HTTP requests to backend")
    ).unwrap();

    // Gauges (current values)
    pub static ref ACTIVE_STREAMS: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_active_streams", "Currently active streams")
    ).unwrap();
    pub static ref AGENTS_ONLINE: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_agents_online", "Number of online agents")
    ).unwrap();
    pub static ref SERVICES_COUNT: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_services_count", "Number of configured services")
    ).unwrap();
    pub static ref DB_SIZE_BYTES: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_db_size_bytes", "Database size in bytes")
    ).unwrap();
    pub static ref PARTITIONS_COUNT: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_partitions_count", "Number of active table partitions")
    ).unwrap();
    pub static ref GRPC_MESSAGES_TOTAL: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_grpc_messages_total", "Total gRPC messages stored")
    ).unwrap();
    pub static ref HTTP_MESSAGES_TOTAL: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_http_messages_total", "Total HTTP messages stored")
    ).unwrap();
    pub static ref TAG_TRIGGERS_TOTAL: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_tag_triggers_total", "Total tag triggers")
    ).unwrap();
    pub static ref CONTROL_PLANE_REVISION: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_control_plane_revision", "Current control-plane configuration revision")
    ).unwrap();
    pub static ref AGENTS_SYNC_PENDING: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_agents_sync_pending", "Online agents whose acknowledged revision lags the current one")
    ).unwrap();
    pub static ref AGENTS_SYNC_ERROR: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_agents_sync_error", "Agents reporting a snapshot application error")
    ).unwrap();
    pub static ref PROXY_TRANSITION_FAILURES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_proxy_transition_failures_total", "Proxy enable/disable transition failures reported by agents")
    ).unwrap();
    pub static ref CAPTURE_SOURCE_CONFLICTS: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_capture_source_conflicts", "Frames captured passively for keys that are effectively proxied (capture-source state bug)")
    ).unwrap();
    pub static ref FORWARDER_DROPS: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_proxy_forwarder_drops", "Captured proxy packets dropped by the capture forwarder (lag)")
    ).unwrap();
    pub static ref PROXY_REVISION_LAG: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_proxy_effective_revision_lag", "Max gap between desired and agent-effective proxy revision across enabled rules")
    ).unwrap();
    pub static ref FAVORITE_STAR_REJECTIONS: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_favorite_star_rejections", "Star requests rejected by the capacity guard")
    ).unwrap();
    pub static ref FAVORITE_ARCHIVE_FAILURES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_favorite_archive_batch_failures", "Archive batch transactions that failed and will retry")
    ).unwrap();
    pub static ref FAVORITE_CLEANUP_FAILURES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_favorite_cleanup_failures", "Saved-content cleanup batches that failed and will retry")
    ).unwrap();
    pub static ref FAVORITE_CLEANUP_SKIPPED: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_favorite_cleanup_skipped", "Destructive cleanup operations skipped to protect unarchived favorites")
    ).unwrap();
    pub static ref FAVORITE_SAVED_BYTES: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_favorite_saved_bytes", "Catalog estimate of physical bytes stored in saved_* tables")
    ).unwrap();
    pub static ref FAVORITE_RESERVED_BYTES: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_favorite_reserved_bytes", "Saved bytes plus worst-case reservations of active favorites")
    ).unwrap();
    pub static ref FAVORITE_CAPACITY_BYTES: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_favorite_capacity_bytes", "Configured favorite_max_total_bytes")
    ).unwrap();
    pub static ref FAVORITE_ACTIVE_SEGMENTS: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_favorite_active_segments", "Starred segments still accepting packets")
    ).unwrap();
    pub static ref FAVORITE_CLEANUP_BACKLOG: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_favorite_cleanup_backlog", "Streams with saved content awaiting post-unstar deletion")
    ).unwrap();
    pub static ref UA_BUCKET_CLEANUP_FAILURES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ua_bucket_cleanup_failures", "User-Agent bucket cleanup passes that failed and will retry")
    ).unwrap();
    pub static ref UA_CLEANUP_BACKLOG: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ua_cleanup_backlog", "User-Agent aggregate rows past the retention cutoff awaiting deletion")
    ).unwrap();

    // JEV (external stream analysis). No labels anywhere: stream ids,
    // payload hashes, remote IPs and raw answers never become metric
    // dimensions.
    pub static ref AI_STATE: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ai_state", "JEV run state: 0 disabled, 1 unconfigured, 2 active, 3 backoff, 4 cost limited")
    ).unwrap();
    pub static ref AI_BACKLOG: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ai_backlog", "Candidate streams waiting for analysis")
    ).unwrap();
    pub static ref AI_INFLIGHT: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ai_inflight", "External calls currently in flight")
    ).unwrap();
    pub static ref AI_CALLS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_calls_total", "Decisions API calls started")
    ).unwrap();
    pub static ref AI_TRANSPORT_FAILURES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_transport_failures_total", "Network, timeout and 5xx provider failures")
    ).unwrap();
    pub static ref AI_RATE_LIMITS: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_rate_limits_total", "Provider 429 responses")
    ).unwrap();
    pub static ref AI_INVALID_RESPONSES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_invalid_responses_total", "Rejected requests and malformed provider answers")
    ).unwrap();
    pub static ref AI_RETRIES: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_retries_total", "Candidates deferred by backoff")
    ).unwrap();
    pub static ref AI_VERDICTS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_verdicts_total", "Stored verdicts (validated responses only)")
    ).unwrap();
    pub static ref AI_ATTACK_VERDICTS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_attack_verdicts_total", "Stored attack verdicts")
    ).unwrap();
    pub static ref AI_SKIPPED_NO_CONTENT: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_skipped_no_content_total", "Analyses skipped: no new analyzable content (deleted or ACK-only)")
    ).unwrap();
    pub static ref AI_TRANSCRIPTS_TRUNCATED: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_ai_transcripts_truncated_total", "Transcripts built with truncation")
    ).unwrap();
    pub static ref AI_COST_SPENT: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ai_cost_spent_micro_usd", "Today's recorded plus conservatively estimated provider spend, micro-USD")
    ).unwrap();
    pub static ref AI_LAST_SUCCESS_UNIX: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_ai_last_success_unix", "Unix time of the last successful verdict insert")
    ).unwrap();
    pub static ref AI_PROVIDER_LATENCY: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "red_eye_ai_provider_latency_seconds",
            "Decisions API call latency"
        ).buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
    ).unwrap();

    // LLM-SOC (triage queue). No labels except the bounded verdict class;
    // stream ids, prompts, payloads and remote IPs never become
    // dimensions.
    pub static ref SOC_QUEUE_DEPTH: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_queue_depth", "Nonterminal triage jobs (queued plus running)")
    ).unwrap();
    pub static ref SOC_OLDEST_WAIT_SECS: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_oldest_wait_secs", "Age of the oldest queued triage job")
    ).unwrap();
    pub static ref SOC_ACTIVE_LEASES: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_active_leases", "Held worker leases")
    ).unwrap();
    pub static ref SOC_DAILY_JOBS_USED: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_daily_jobs_used", "Jobs counted against today's UTC limit")
    ).unwrap();
    pub static ref SOC_DAILY_JOB_LIMIT: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_daily_job_limit", "Configured daily job limit")
    ).unwrap();
    pub static ref SOC_QUEUE_CAPACITY: IntGauge = IntGauge::with_opts(
        Opts::new("red_eye_soc_queue_capacity", "Nonterminal queue cap")
    ).unwrap();
    pub static ref SOC_JOBS_ENQUEUED: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_jobs_enqueued_total", "Jobs created (manual plus automatic)")
    ).unwrap();
    pub static ref SOC_AUTO_DROPPED_QUEUE_FULL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_auto_dropped_queue_full_total", "Automatic jobs dropped at queue saturation")
    ).unwrap();
    pub static ref SOC_CLAIMS_REJECTED: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_claims_rejected_total", "Claims refused (disabled or daily limit)")
    ).unwrap();
    pub static ref SOC_JOBS_RETRIED: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_jobs_retried_total", "Transient failures requeued with backoff")
    ).unwrap();
    pub static ref SOC_JOBS_FAILED_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_jobs_failed_total", "Jobs exhausted as failed (never benign)")
    ).unwrap();
    pub static ref SOC_RESULT_REJECTIONS: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_result_rejections_total", "Submitted results rejected by validation")
    ).unwrap();
    pub static ref SOC_EVIDENCE_REJECTIONS: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_evidence_rejections_total", "Evidence ids outside the job's fetched set")
    ).unwrap();
    pub static ref SOC_RESULTS_TOTAL: prometheus::IntCounterVec = prometheus::IntCounterVec::new(
        Opts::new("red_eye_soc_results_total", "Validated triage results by verdict class"),
        &["verdict"]
    ).unwrap();
    pub static ref SOC_ALERTS_TOTAL: IntCounter = IntCounter::with_opts(
        Opts::new("red_eye_soc_alerts_total", "Alert-threshold soc_triage broadcasts")
    ).unwrap();
}

pub fn register_metrics() {
    let r = &*REGISTRY;
    let _ = r.register(Box::new(PACKETS_TOTAL.clone()));
    let _ = r.register(Box::new(STREAMS_TOTAL.clone()));
    let _ = r.register(Box::new(HTTP_REQUESTS_TOTAL.clone()));
    let _ = r.register(Box::new(ACTIVE_STREAMS.clone()));
    let _ = r.register(Box::new(AGENTS_ONLINE.clone()));
    let _ = r.register(Box::new(SERVICES_COUNT.clone()));
    let _ = r.register(Box::new(DB_SIZE_BYTES.clone()));
    let _ = r.register(Box::new(PARTITIONS_COUNT.clone()));
    let _ = r.register(Box::new(GRPC_MESSAGES_TOTAL.clone()));
    let _ = r.register(Box::new(HTTP_MESSAGES_TOTAL.clone()));
    let _ = r.register(Box::new(TAG_TRIGGERS_TOTAL.clone()));
    let _ = r.register(Box::new(CONTROL_PLANE_REVISION.clone()));
    let _ = r.register(Box::new(AGENTS_SYNC_PENDING.clone()));
    let _ = r.register(Box::new(AGENTS_SYNC_ERROR.clone()));
    let _ = r.register(Box::new(PROXY_TRANSITION_FAILURES.clone()));
    let _ = r.register(Box::new(CAPTURE_SOURCE_CONFLICTS.clone()));
    let _ = r.register(Box::new(FORWARDER_DROPS.clone()));
    let _ = r.register(Box::new(PROXY_REVISION_LAG.clone()));
    let _ = r.register(Box::new(FAVORITE_STAR_REJECTIONS.clone()));
    let _ = r.register(Box::new(FAVORITE_ARCHIVE_FAILURES.clone()));
    let _ = r.register(Box::new(FAVORITE_CLEANUP_FAILURES.clone()));
    let _ = r.register(Box::new(FAVORITE_CLEANUP_SKIPPED.clone()));
    let _ = r.register(Box::new(FAVORITE_SAVED_BYTES.clone()));
    let _ = r.register(Box::new(FAVORITE_RESERVED_BYTES.clone()));
    let _ = r.register(Box::new(FAVORITE_CAPACITY_BYTES.clone()));
    let _ = r.register(Box::new(FAVORITE_ACTIVE_SEGMENTS.clone()));
    let _ = r.register(Box::new(FAVORITE_CLEANUP_BACKLOG.clone()));
    let _ = r.register(Box::new(UA_BUCKET_CLEANUP_FAILURES.clone()));
    let _ = r.register(Box::new(UA_CLEANUP_BACKLOG.clone()));
    let _ = r.register(Box::new(AI_STATE.clone()));
    let _ = r.register(Box::new(AI_BACKLOG.clone()));
    let _ = r.register(Box::new(AI_INFLIGHT.clone()));
    let _ = r.register(Box::new(AI_CALLS_TOTAL.clone()));
    let _ = r.register(Box::new(AI_TRANSPORT_FAILURES.clone()));
    let _ = r.register(Box::new(AI_RATE_LIMITS.clone()));
    let _ = r.register(Box::new(AI_INVALID_RESPONSES.clone()));
    let _ = r.register(Box::new(AI_RETRIES.clone()));
    let _ = r.register(Box::new(AI_VERDICTS_TOTAL.clone()));
    let _ = r.register(Box::new(AI_ATTACK_VERDICTS_TOTAL.clone()));
    let _ = r.register(Box::new(AI_SKIPPED_NO_CONTENT.clone()));
    let _ = r.register(Box::new(AI_TRANSCRIPTS_TRUNCATED.clone()));
    let _ = r.register(Box::new(AI_COST_SPENT.clone()));
    let _ = r.register(Box::new(AI_LAST_SUCCESS_UNIX.clone()));
    let _ = r.register(Box::new(AI_PROVIDER_LATENCY.clone()));
    let _ = r.register(Box::new(SOC_QUEUE_DEPTH.clone()));
    let _ = r.register(Box::new(SOC_OLDEST_WAIT_SECS.clone()));
    let _ = r.register(Box::new(SOC_ACTIVE_LEASES.clone()));
    let _ = r.register(Box::new(SOC_DAILY_JOBS_USED.clone()));
    let _ = r.register(Box::new(SOC_DAILY_JOB_LIMIT.clone()));
    let _ = r.register(Box::new(SOC_QUEUE_CAPACITY.clone()));
    let _ = r.register(Box::new(SOC_JOBS_ENQUEUED.clone()));
    let _ = r.register(Box::new(SOC_AUTO_DROPPED_QUEUE_FULL.clone()));
    let _ = r.register(Box::new(SOC_CLAIMS_REJECTED.clone()));
    let _ = r.register(Box::new(SOC_JOBS_RETRIED.clone()));
    let _ = r.register(Box::new(SOC_JOBS_FAILED_TOTAL.clone()));
    let _ = r.register(Box::new(SOC_RESULT_REJECTIONS.clone()));
    let _ = r.register(Box::new(SOC_EVIDENCE_REJECTIONS.clone()));
    let _ = r.register(Box::new(SOC_RESULTS_TOTAL.clone()));
    let _ = r.register(Box::new(SOC_ALERTS_TOTAL.clone()));
}

/// Refresh gauge metrics from DB and agent state.
async fn refresh_gauges(
    pool: &PgPool,
    agent_service: &AgentService,
    control: &crate::api::agent::agent_config::ControlPlane,
) {
    // Active streams
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM stream WHERE active = true")
        .fetch_one(pool)
        .await
    {
        ACTIVE_STREAMS.set(row);
    }

    // Total streams
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM stream")
        .fetch_one(pool)
        .await
    {
        STREAMS_TOTAL.reset();
        let _ = STREAMS_TOTAL.inc_by(row as u64);
    }

    // Services
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM service")
        .fetch_one(pool)
        .await
    {
        SERVICES_COUNT.set(row);
    }

    // DB size
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await
    {
        DB_SIZE_BYTES.set(row);
    }

    // Partitions
    if let Ok(row) = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_inherits JOIN pg_class ON inhrelid = oid WHERE inhparent = 'packets'::regclass"
    ).fetch_one(pool).await
    {
        PARTITIONS_COUNT.set(row);
    }

    // HTTP messages
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM http_message")
        .fetch_one(pool)
        .await
    {
        HTTP_MESSAGES_TOTAL.set(row);
    }

    // gRPC messages
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM grpc_message")
        .fetch_one(pool)
        .await
    {
        GRPC_MESSAGES_TOTAL.set(row);
    }

    // Tag triggers
    if let Ok(row) = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tag")
        .fetch_one(pool)
        .await
    {
        TAG_TRIGGERS_TOTAL.set(row);
    }

    // Total packets (approximate via pg_stat for performance)
    if let Ok(row) = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(SUM(n_live_tup), 0)::bigint FROM pg_stat_user_tables WHERE relname LIKE 'packets_%'"
    ).fetch_one(pool).await
    {
        PACKETS_TOTAL.reset();
        let _ = PACKETS_TOTAL.inc_by(row as u64);
    }

    // Favorite retention: capacity, usage and population. One catalog +
    // stream aggregates query; failures just leave the previous values.
    // Settings fall back to the built-in defaults (env values are seeded
    // into app_settings at startup, so this is a cold-start guard only).
    {
        let limits = crate::api::favorite::favorite_service::FavoriteLimits::builtins();
        if let Ok(row) = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
            r#"
            SELECT
                (SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
                 FROM pg_class c
                 WHERE c.relkind = 'r'
                   AND c.relname IN ('saved_packet', 'saved_tag',
                                     'saved_http_message', 'saved_grpc_message')),
                (SELECT COUNT(*) FROM stream WHERE favorite AND active),
                (SELECT COUNT(*) FROM stream WHERE favorite),
                (SELECT COUNT(*) FROM stream
                 WHERE NOT favorite AND retention_state IN ('archiving', 'archived')),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE favorite AND NOT active AND retention_state <> 'archived'),
                (SELECT COALESCE(SUM(retention_bytes), 0) FROM stream
                 WHERE NOT favorite AND retention_state IN ('archiving', 'archived'))
            "#,
        )
        .fetch_one(pool)
        .await
        {
            let snapshot = crate::api::favorite::favorite_service::UsageSnapshot {
                saved_bytes: row.0,
                active_starred: row.1,
                closed_unarchived_bytes: row.4,
                pending_cleanup_bytes: row.5,
                starred_streams: row.2,
                pending_cleanup_streams: row.3,
            };
            FAVORITE_CAPACITY_BYTES.set(limits.max_total_bytes);
            FAVORITE_SAVED_BYTES.set(snapshot.saved_bytes);
            FAVORITE_RESERVED_BYTES.set(snapshot.reserved_bytes(limits.segment_bytes));
            FAVORITE_ACTIVE_SEGMENTS.set(snapshot.active_starred);
            FAVORITE_CLEANUP_BACKLOG.set(snapshot.pending_cleanup_streams);
        }
    }

    // Agents online
    let agents = agent_service.list_agents().await;
    AGENTS_ONLINE.set(agents.iter().filter(|a| a.online).count() as i64);

    // Control-plane revision + per-agent sync state
    if let Some(rev) = control.current_revision().await {
        CONTROL_PLANE_REVISION.set(rev);
    }
    let mut pending = 0;
    let mut errored = 0;
    for agent in agents.iter().filter(|a| a.online) {
        if let Some(view) = control.network_settings_view(&agent.agent_id).await {
            match view.sync_state.as_str() {
                "error" => errored += 1,
                "pending" => pending += 1,
                _ => {}
            }
        }
    }
    AGENTS_SYNC_PENDING.set(pending);
    AGENTS_SYNC_ERROR.set(errored);

    // Proxy diagnostics: conflicts + forwarder drops come from the last
    // heartbeats; the revision lag from desired vs agent-effective rows.
    CAPTURE_SOURCE_CONFLICTS.set(agent_service.capture_conflicts_total() as i64);
    FORWARDER_DROPS.set(agent_service.forwarder_drops_total() as i64);
    if let Ok(lag) = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(revision - COALESCE(effective_revision, 0)), 0)
         FROM proxy_protection WHERE enabled",
    )
    .fetch_one(pool)
    .await
    {
        PROXY_REVISION_LAG.set(lag);
    }
}

/// GET /metrics — Prometheus scrape endpoint (no auth required)
#[get("/metrics")]
async fn prometheus_metrics(
    pool: web::Data<PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
    control: web::Data<Arc<crate::api::agent::agent_config::ControlPlane>>,
) -> impl Responder {
    refresh_gauges(pool.get_ref(), &agent_service, &control).await;

    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(buffer)
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(prometheus_metrics);
}
