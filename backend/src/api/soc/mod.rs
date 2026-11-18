pub mod api;
pub mod bearer;
pub mod matrix;
pub mod repository;
pub mod settings;
pub mod validation;

use log::{debug, info, warn};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::interval;

use crate::api::realtime::BroadcastSender;
pub use api::config;
pub use repository::{EnqueueOutcome, SocRepository};

/// Increment the SOC settings generation. Called on the soc_enabled
/// false->true transition (settings PUT path): jobs enqueued before a
/// disable carry the older generation and their late results are
/// rejected after re-enable. Rare operator action; read + write is fine.
pub async fn advance_generation(pool: &PgPool) {
    let current: i64 =
        sqlx::query_scalar::<_, String>("SELECT value FROM app_settings WHERE key = $1")
            .bind(settings::SOC_SETTINGS_GENERATION)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
    let _ = sqlx::query(
        "INSERT INTO app_settings (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = $2",
    )
    .bind(settings::SOC_SETTINGS_GENERATION)
    .bind((current + 1).to_string())
    .execute(pool)
    .await;
}

/// Enqueue one automatic job from the JEV verdict insert path. Fires only
/// for NEW verdicts (the scheduler calls this right after a successful
/// insert), so enabling SOC later never retro-enqueues old verdicts; the
/// partial UNIQUE index makes exactly-once per verdict a database
/// guarantee, not a hope. Every gate failure is silent-by-design: SOC
/// problems must never surface inside JEV analysis.
pub async fn maybe_enqueue_automatic(
    pool: &PgPool,
    verdict_id: i64,
    stream_id: i64,
    stream_revision: i64,
    is_attack: bool,
    severity: f64,
) {
    let soc = settings::SocSettings::load(pool).await;
    if !soc.enabled || !soc.auto_enabled {
        return;
    }
    let jev = crate::api::ai::settings::AiSettings::load(pool).await;
    if !jev.enabled || !jev.api_key_configured {
        return;
    }
    if !is_attack || (severity as i64) < soc.auto_min_jev_severity {
        return;
    }
    let repo = SocRepository::new(pool.clone());
    // Automatic priority is the bounded JEV severity (0..3); manual jobs
    // start at 100 but aging fairness outranks both.
    let priority = severity.clamp(0.0, 3.0) as i32;
    match repo
        .enqueue_automatic(
            stream_id,
            stream_revision,
            verdict_id,
            &soc.model,
            settings::SOC_PROMPT_VERSION,
            soc.settings_generation,
            priority,
            settings::queue_cap(),
        )
        .await
    {
        Ok(EnqueueOutcome::Created(id)) => {
            debug!(
                "SOC auto job {} enqueued for stream {} rev {}",
                id, stream_id, stream_revision
            );
            crate::api::metrics::SOC_JOBS_ENQUEUED.inc();
        }
        Ok(EnqueueOutcome::DuplicateExisting(_)) => {}
        Ok(EnqueueOutcome::DroppedQueueFull) => {
            // Saturation drops automatic jobs with a metric, never an
            // error loop into the JEV scheduler.
            crate::api::metrics::SOC_AUTO_DROPPED_QUEUE_FULL.inc();
            warn!(
                "SOC queue full: automatic job for verdict {} dropped",
                verdict_id
            );
        }
        Err(e) => debug!("SOC auto enqueue failed (verdict {}): {}", verdict_id, e),
    }
}

/// Outcome of a manual triage submission.
pub enum ManualEnqueueError {
    SocDisabled(String),
    StreamNotFound,
    NoRevision,
    Db(sqlx::Error),
}

/// Enqueue one manual job. Manual triage needs only soc_enabled: it works
/// without JEV, with failing JEV providers and with invalid JEV
/// credentials, because it reads streams directly.
pub async fn enqueue_manual(
    pool: &PgPool,
    stream_id: i64,
    fresh: bool,
    requested_by: Option<&str>,
) -> Result<EnqueueOutcome, ManualEnqueueError> {
    let soc = settings::SocSettings::load(pool).await;
    if !soc.enabled {
        return Err(ManualEnqueueError::SocDisabled(
            "LLM-SOC is disabled (soc_enabled=false); enable it in Settings to submit manual triage"
                .to_string(),
        ));
    }
    let revision: Option<i64> =
        sqlx::query_scalar("SELECT last_packet_id FROM stream WHERE id = $1")
            .bind(stream_id)
            .fetch_optional(pool)
            .await
            .map_err(ManualEnqueueError::Db)?;
    let Some(revision) = revision.filter(|r| *r > 0) else {
        // Zero means the flusher never attributed packets to this stream:
        // nothing analyzable exists at any revision.
        return Err(if revision.is_none() {
            ManualEnqueueError::StreamNotFound
        } else {
            ManualEnqueueError::NoRevision
        });
    };
    let repo = SocRepository::new(pool.clone());
    let outcome = repo
        .enqueue_manual(
            stream_id,
            revision,
            requested_by,
            &soc.model,
            settings::SOC_PROMPT_VERSION,
            soc.settings_generation,
            fresh,
            settings::queue_cap(),
        )
        .await
        .map_err(ManualEnqueueError::Db)?;
    if matches!(outcome, EnqueueOutcome::Created(_)) {
        crate::api::metrics::SOC_JOBS_ENQUEUED.inc();
    }
    Ok(outcome)
}

/// The soc_triage alert event (spec contract) for a completed result that
/// crosses the alert threshold. Payload carries verdict facts only.
pub fn alert_event(result: &repository::SocResultRow) -> String {
    serde_json::json!({
        "type": "soc_triage",
        "job_id": result.job_id,
        "result_id": result.id,
        "stream_id": result.stream_id,
        "stream_revision": result.stream_revision,
        "verdict": result.verdict,
        "confidence": result.confidence,
        "severity": result.severity,
        "summary": result.summary,
    })
    .to_string()
}

/// State-update event for every completed result (attack or not): the
/// stream view refreshes, the toast does not fire for this one.
pub fn result_event(result: &repository::SocResultRow) -> String {
    serde_json::json!({
        "type": "soc_result",
        "job_id": result.job_id,
        "result_id": result.id,
        "stream_id": result.stream_id,
        "stream_revision": result.stream_revision,
        "verdict": result.verdict,
        "confidence": result.confidence,
        "severity": result.severity,
    })
    .to_string()
}

pub fn should_alert(result: &repository::SocResultRow, threshold: f64) -> bool {
    result.verdict == "likely_attack" && result.confidence >= threshold
}

/// Broadcast helper shared by the result-submit endpoint. `threshold` is
/// the loaded soc_alert_threshold; an alert additionally fires the
/// soc_triage event the toast listens to.
pub fn broadcast_result(
    broadcaster: &BroadcastSender,
    result: &repository::SocResultRow,
    threshold: f64,
) {
    let _ = broadcaster.send(result_event(result));
    crate::api::metrics::SOC_RESULTS_TOTAL
        .with_label_values(&[result.verdict.as_str()])
        .inc();
    if should_alert(result, threshold) {
        let _ = broadcaster.send(alert_event(result));
        crate::api::metrics::SOC_ALERTS_TOTAL.inc();
    }
}

/// Supervised maintenance loop: publish queue gauges and trim terminal
/// job rows in bounded batches. A failing tick never terminates the
/// loop; SOC outages cannot touch ingestion, JEV or the UI.
pub fn spawn_maintenance(pool: PgPool) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;
            if let Err(e) = maintenance_tick(&pool).await {
                debug!("SOC maintenance tick error: {}", e);
            }
        }
    });
}

const TERMINAL_JOB_RETENTION_SECS: f64 = 7.0 * 86_400.0;

async fn maintenance_tick(pool: &PgPool) -> Result<(), sqlx::Error> {
    let repo = SocRepository::new(pool.clone());
    let (depth, oldest_wait, leases) = repo.queue_stats().await?;
    crate::api::metrics::SOC_QUEUE_DEPTH.set(depth);
    crate::api::metrics::SOC_OLDEST_WAIT_SECS.set(oldest_wait);
    crate::api::metrics::SOC_ACTIVE_LEASES.set(leases);
    let used = repo.jobs_used_today().await?;
    crate::api::metrics::SOC_DAILY_JOBS_USED.set(used);
    let soc = settings::SocSettings::load(pool).await;
    crate::api::metrics::SOC_DAILY_JOB_LIMIT.set(soc.daily_job_limit);
    crate::api::metrics::SOC_QUEUE_CAPACITY.set(settings::queue_cap());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let trimmed = repo
        .trim_terminal_jobs_batch(now - TERMINAL_JOB_RETENTION_SECS)
        .await?;
    if trimmed > 0 {
        info!("SOC maintenance: trimmed {} terminal job rows", trimmed);
    }
    Ok(())
}
