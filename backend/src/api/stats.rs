use actix_identity::Identity;
use actix_web::{get, web, HttpResponse, Responder};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
pub struct RateStats {
    pub packets_per_minute: i64,
}

/// GET /api/stats/rate — packets ingested in the last 60 seconds
#[get("/stats/rate")]
async fn get_rate(
    _identity: Identity,
    pool: web::Data<PgPool>,
) -> impl Responder {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM packets WHERE timestamp >= $1"
    )
    .bind(now_secs - 60)
    .fetch_one(pool.get_ref())
    .await
    {
        Ok(count) => HttpResponse::Ok().json(RateStats { packets_per_minute: count }),
        Err(_) => HttpResponse::Ok().json(RateStats { packets_per_minute: 0 }),
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_rate);
}
