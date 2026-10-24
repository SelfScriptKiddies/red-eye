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
}

/// Refresh gauge metrics from DB and agent state.
async fn refresh_gauges(pool: &PgPool, agent_service: &AgentService) {
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

    // Agents online
    let agents = agent_service.list_agents().await;
    AGENTS_ONLINE.set(agents.iter().filter(|a| a.online).count() as i64);
}

/// GET /metrics — Prometheus scrape endpoint (no auth required)
#[get("/metrics")]
async fn prometheus_metrics(
    pool: web::Data<PgPool>,
    agent_service: web::Data<Arc<AgentService>>,
) -> impl Responder {
    refresh_gauges(pool.get_ref(), &agent_service).await;

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
