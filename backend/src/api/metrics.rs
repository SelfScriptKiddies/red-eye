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
