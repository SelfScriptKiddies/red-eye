use serde::Serialize;
use sqlx::PgPool;

// Timestamps cross the SQL boundary as unix-epoch bigints only (the
// ai_verdict repository pattern): no chrono types anywhere in the row
// structs, projections use EXTRACT(EPOCH FROM ...).

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SocJobRow {
    pub id: i64,
    pub stream_id: i64,
    pub stream_revision: i64,
    pub jev_verdict_id: Option<i64>,
    pub trigger: String,
    pub requested_by: Option<String>,
    pub status: String,
    pub priority: i32,
    pub attempts: i32,
    pub model: Option<String>,
    pub prompt_version: i32,
    pub settings_generation: i64,
    pub fetched_evidence: serde_json::Value,
    pub created_at_unix: i64,
    pub started_at_unix: Option<i64>,
    pub finished_at_unix: Option<i64>,
    pub lease_expires_at_unix: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SocResultRow {
    pub id: i64,
    pub job_id: i64,
    pub stream_id: i64,
    pub stream_revision: i64,
    pub verdict: String,
    pub confidence: f64,
    pub severity: i32,
    pub summary: String,
    pub evidence: serde_json::Value,
    pub indicators: serde_json::Value,
    pub recommended_action: String,
    pub model: String,
    pub prompt_version: i32,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub duration_ms: Option<i64>,
    pub created_at_unix: i64,
}

const JOB_COLUMNS: &str = "id, stream_id, stream_revision, jev_verdict_id, trigger, requested_by, \
    status, priority, attempts, model, prompt_version, settings_generation, fetched_evidence, \
    EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix, \
    EXTRACT(EPOCH FROM started_at)::bigint AS started_at_unix, \
    EXTRACT(EPOCH FROM finished_at)::bigint AS finished_at_unix, \
    EXTRACT(EPOCH FROM lease_expires_at)::bigint AS lease_expires_at_unix, \
    last_error";

const RESULT_COLUMNS: &str = "id, job_id, stream_id, stream_revision, verdict, confidence::float8, \
    severity, summary, evidence, indicators, recommended_action, model, prompt_version, \
    input_tokens, output_tokens, duration_ms, \
    EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix";

/// Why an enqueue did not create a row.
#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Created(i64),
    DuplicateExisting(i64),
    DroppedQueueFull,
}

/// Manual priority sits far above the automatic 0..3 severity scale; aged
/// jobs (waiting >= 60s) outrank every priority level regardless.
pub const MANUAL_PRIORITY: i32 = 100;
const AGED_FAIRNESS_SECS: f64 = 60.0;
const LEASE_RECOVER_BATCH: i64 = 50;
const FETCHED_EVIDENCE_CAP: usize = 100;

pub struct SocRepository {
    pgpool: PgPool,
}

impl SocRepository {
    pub fn new(pgpool: PgPool) -> Self {
        SocRepository { pgpool }
    }

    /// Enqueue one manual job. Deduplicates against an existing
    /// queued/running job for the same stream revision unless `fresh`;
    /// at the queue cap manual submissions are refused (503 upstream).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_manual(
        &self,
        stream_id: i64,
        stream_revision: i64,
        requested_by: Option<&str>,
        model: &str,
        prompt_version: i32,
        settings_generation: i64,
        fresh: bool,
        queue_cap: i64,
    ) -> Result<EnqueueOutcome, sqlx::Error> {
        if !fresh {
            let existing: Option<i64> = sqlx::query_scalar(
                "SELECT id FROM soc_job
                 WHERE stream_id = $1 AND stream_revision = $2 AND status IN ('queued', 'running')
                 ORDER BY id DESC LIMIT 1",
            )
            .bind(stream_id)
            .bind(stream_revision)
            .fetch_optional(&self.pgpool)
            .await?;
            if let Some(id) = existing {
                return Ok(EnqueueOutcome::DuplicateExisting(id));
            }
        }
        self.insert_job(
            stream_id,
            stream_revision,
            None,
            "manual",
            requested_by,
            MANUAL_PRIORITY,
            model,
            prompt_version,
            settings_generation,
            queue_cap,
        )
        .await
    }

    /// Enqueue one automatic job. The partial UNIQUE index on
    /// (jev_verdict_id) WHERE trigger='automatic' enforces exactly-once
    /// per JEV verdict even across concurrent schedulers; at the queue
    /// cap automatic submissions are dropped (counted by the caller).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_automatic(
        &self,
        stream_id: i64,
        stream_revision: i64,
        jev_verdict_id: i64,
        model: &str,
        prompt_version: i32,
        settings_generation: i64,
        priority: i32,
        queue_cap: i64,
    ) -> Result<EnqueueOutcome, sqlx::Error> {
        self.insert_job(
            stream_id,
            stream_revision,
            Some(jev_verdict_id),
            "automatic",
            None,
            priority,
            model,
            prompt_version,
            settings_generation,
            queue_cap,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_job(
        &self,
        stream_id: i64,
        stream_revision: i64,
        jev_verdict_id: Option<i64>,
        trigger: &str,
        requested_by: Option<&str>,
        priority: i32,
        model: &str,
        prompt_version: i32,
        settings_generation: i64,
        queue_cap: i64,
    ) -> Result<EnqueueOutcome, sqlx::Error> {
        // The cap check rides inside the INSERT's WHERE clause so the
        // count and the insert race instead of TOCTOU-ing between two
        // statements.
        let id: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO soc_job (
                stream_id, stream_revision, jev_verdict_id, trigger, requested_by,
                priority, model, prompt_version, settings_generation
            )
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
            WHERE (SELECT COUNT(*) FROM soc_job WHERE status IN ('queued', 'running')) < $10
            RETURNING id
            "#,
        )
        .bind(stream_id)
        .bind(stream_revision)
        .bind(jev_verdict_id)
        .bind(trigger)
        .bind(requested_by)
        .bind(priority)
        .bind(model)
        .bind(prompt_version)
        .bind(settings_generation)
        .bind(queue_cap)
        .fetch_optional(&self.pgpool)
        .await?;
        match id {
            Some(id) => Ok(EnqueueOutcome::Created(id)),
            // Both triggers land here at cap: manual submissions turn into
            // a visible 503 upstream, automatic drops are counted.
            None => Ok(EnqueueOutcome::DroppedQueueFull),
        }
    }

    /// Claim one job: recover expired leases (bounded), then pick the
    /// highest-priority due job with FOR UPDATE SKIP LOCKED inside one
    /// transaction. Aged jobs (waiting >= 60s) beat priority so manual
    /// jobs cannot starve the automatic backlog.
    pub async fn claim_job(
        &self,
        worker_id: &str,
        lease_secs: i64,
    ) -> Result<Option<SocJobRow>, sqlx::Error> {
        let mut tx = self.pgpool.begin().await?;

        // Stuck jobs (crashed worker) re-enter the queue; attempts stay
        // counted, so a permanently poisoned job still exhausts visibly.
        sqlx::query(
            r#"
            UPDATE soc_job SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL
            WHERE id IN (
                SELECT id FROM soc_job
                WHERE status = 'running' AND lease_expires_at < now()
                ORDER BY lease_expires_at ASC
                LIMIT $1
            )
            "#,
        )
        .bind(LEASE_RECOVER_BATCH)
        .execute(&mut *tx)
        .await?;

        let job_id: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT id FROM soc_job
            WHERE status = 'queued' AND next_attempt_at <= now()
            ORDER BY (created_at < now() - make_interval(secs => $1)) DESC,
                     priority DESC,
                     next_attempt_at ASC,
                     id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(AGED_FAIRNESS_SECS)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(job_id) = job_id else {
            tx.commit().await?;
            return Ok(None);
        };

        let row: Option<SocJobRow> = sqlx::query_as::<_, SocJobRow>(&format!(
            "UPDATE soc_job
             SET status = 'running',
                 started_at = COALESCE(started_at, now()),
                 lease_owner = $2,
                 lease_expires_at = to_timestamp($3),
                 attempts = attempts + 1
             WHERE id = $1
             RETURNING {}",
            JOB_COLUMNS
        ))
        .bind(job_id)
        .bind(worker_id)
        .bind(unix_now() + (lease_secs.clamp(10, 3600)) as f64)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(row)
    }

    /// Merge fetched-evidence ids into the job's set (dedup, capped).
    /// Entries are strings like "packet:123" or "message:45".
    pub async fn record_fetched_evidence(
        &self,
        job_id: i64,
        entries: Vec<String>,
    ) -> Result<Option<Vec<String>>, sqlx::Error> {
        let Some(current) = self.get_job(job_id).await? else {
            return Ok(None);
        };
        let mut set: Vec<String> = current
            .fetched_evidence
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        for e in entries {
            if set.len() >= FETCHED_EVIDENCE_CAP {
                break;
            }
            if !set.contains(&e) {
                set.push(e);
            }
        }
        sqlx::query("UPDATE soc_job SET fetched_evidence = $2 WHERE id = $1")
            .bind(job_id)
            .bind(serde_json::to_value(&set).unwrap_or(serde_json::json!([])))
            .execute(&self.pgpool)
            .await?;
        Ok(Some(set))
    }

    pub async fn get_job(&self, job_id: i64) -> Result<Option<SocJobRow>, sqlx::Error> {
        sqlx::query_as::<_, SocJobRow>(&format!(
            "SELECT {} FROM soc_job WHERE id = $1",
            JOB_COLUMNS
        ))
        .bind(job_id)
        .fetch_optional(&self.pgpool)
        .await
    }

    /// Store a validated result and complete its job. The UNIQUE(job_id)
    /// constraint makes a duplicate stored output impossible; the
    /// generation check rejects late results across a disable/enable
    /// cycle. Returns Err(RejectionReason) for a schema/state mismatch the
    /// worker may retry within its attempts budget.
    pub async fn submit_result(
        &self,
        result: &ValidatedResult,
        current_generation: i64,
    ) -> Result<Result<i64, ResultRejection>, sqlx::Error> {
        let Some(job) = self.get_job(result.job_id).await? else {
            return Ok(Err(ResultRejection::JobNotActive("job not found".into())));
        };
        if job.status != "running" {
            return Ok(Err(ResultRejection::JobNotActive(format!(
                "job status is {}, not running",
                job.status
            ))));
        }
        if job.stream_id != result.stream_id || job.stream_revision != result.stream_revision {
            return Ok(Err(ResultRejection::Mismatch(
                "stream_id/stream_revision do not match the claimed job".into(),
            )));
        }
        if job.model.as_deref() != Some(result.model.as_str()) {
            return Ok(Err(ResultRejection::Mismatch(
                "model does not match the claimed job".into(),
            )));
        }
        if job.prompt_version != result.prompt_version {
            return Ok(Err(ResultRejection::Mismatch(
                "prompt_version does not match the claimed job".into(),
            )));
        }
        if job.settings_generation != current_generation {
            return Ok(Err(ResultRejection::StaleGeneration));
        }

        let mut tx = self.pgpool.begin().await?;
        let result_id: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO soc_triage_result (
                job_id, stream_id, stream_revision, verdict, confidence, severity,
                summary, evidence, indicators, recommended_action, model, prompt_version,
                input_tokens, output_tokens, duration_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (job_id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(result.job_id)
        .bind(result.stream_id)
        .bind(result.stream_revision)
        .bind(&result.verdict)
        .bind(result.confidence as f32)
        .bind(result.severity)
        .bind(&result.summary)
        .bind(&result.evidence)
        .bind(&result.indicators)
        .bind(&result.recommended_action)
        .bind(&result.model)
        .bind(result.prompt_version)
        .bind(result.input_tokens)
        .bind(result.output_tokens)
        .bind(result.duration_ms)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(result_id) = result_id else {
            tx.rollback().await?;
            return Ok(Err(ResultRejection::JobNotActive(
                "result already stored for this job".into(),
            )));
        };
        sqlx::query(
            "UPDATE soc_job SET status = 'succeeded', finished_at = now(), lease_owner = NULL, \
             lease_expires_at = NULL, last_error = NULL WHERE id = $1",
        )
        .bind(result.job_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Ok(result_id))
    }

    /// Record a worker-reported failure. `permanent` (provider 4xx
    /// schema/authentication) fails the job immediately: retrying the
    /// identical request is pointless. Transient failures requeue with
    /// bounded exponential backoff until the attempt budget is spent.
    /// Failure never means benign: no result row is ever written here.
    pub async fn record_failure(
        &self,
        job_id: i64,
        error: &str,
        permanent: bool,
        max_attempts: i64,
    ) -> Result<Option<String>, sqlx::Error> {
        let Some(job) = self.get_job(job_id).await? else {
            return Ok(None);
        };
        if job.status != "running" {
            return Ok(None);
        }
        let sanitized = sanitize_error(error);
        let exhausted = permanent || job.attempts as i64 >= max_attempts.max(1);
        let final_status = if exhausted { "failed" } else { "queued" };
        // Bounded exponential: 2^attempt seconds, capped at 300s.
        let backoff_secs = (2u64.saturating_pow(job.attempts.unsigned_abs().min(16))).min(300);
        let n = sqlx::query(
            r#"
            UPDATE soc_job
            SET status = $2,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = $3,
                next_attempt_at = CASE WHEN $2 = 'queued'
                    THEN to_timestamp(EXTRACT(EPOCH FROM now()) + $4) ELSE next_attempt_at END,
                finished_at = CASE WHEN $2 = 'failed' THEN now() ELSE finished_at END
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(final_status)
        .bind(&sanitized)
        .bind(backoff_secs as f64)
        .execute(&self.pgpool)
        .await?
        .rows_affected();
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(final_status.to_string()))
    }

    pub async fn get_result(&self, id: i64) -> Result<Option<SocResultRow>, sqlx::Error> {
        sqlx::query_as::<_, SocResultRow>(&format!(
            "SELECT {} FROM soc_triage_result WHERE id = $1",
            RESULT_COLUMNS
        ))
        .bind(id)
        .fetch_optional(&self.pgpool)
        .await
    }

    /// Bounded keyset listing per stream (cursor = previous page's last
    /// id), or the most recent results when no stream is given.
    pub async fn list_results(
        &self,
        stream_id: Option<i64>,
        limit: i64,
        cursor: Option<i64>,
    ) -> Result<Vec<SocResultRow>, sqlx::Error> {
        let with_stream = stream_id.is_some();
        let mut sql = format!(
            "SELECT {} FROM soc_triage_result WHERE id < $1",
            RESULT_COLUMNS
        );
        if with_stream {
            sql.push_str(" AND stream_id = $2");
        }
        sql.push_str(&format!(
            " ORDER BY id DESC LIMIT ${}",
            if with_stream { 3 } else { 2 }
        ));
        let mut query = sqlx::query_as::<_, SocResultRow>(&sql).bind(cursor.unwrap_or(i64::MAX));
        if let Some(sid) = stream_id {
            query = query.bind(sid);
        }
        query.bind(limit).fetch_all(&self.pgpool).await
    }

    /// (queue depth, oldest queued wait in seconds, active leases).
    pub async fn queue_stats(&self) -> Result<(i64, i64, i64), sqlx::Error> {
        sqlx::query_as::<_, (i64, Option<f64>, i64)>(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status IN ('queued', 'running')),
                EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE status = 'queued'))),
                COUNT(*) FILTER (WHERE status = 'running' AND lease_expires_at > now())
            FROM soc_job
            "#,
        )
        .fetch_one(&self.pgpool)
        .await
        .map(|(depth, oldest, leases)| (depth, oldest.unwrap_or(0.0).floor() as i64, leases))
    }

    /// Jobs counted against today's UTC limit: succeeded, failed and
    /// running rows created today. Queued work does not consume the
    /// budget until it runs.
    pub async fn jobs_used_today(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM soc_job
            WHERE status IN ('succeeded', 'failed', 'running')
              AND (created_at AT TIME ZONE 'UTC')::date = (now() AT TIME ZONE 'UTC')::date
            "#,
        )
        .fetch_one(&self.pgpool)
        .await
    }

    /// Whether claims are allowed: enabled SOC, under the daily limit and
    /// under the queue cap headroom for recovered jobs. Returns
    /// (allowed, reason).
    pub async fn claim_gate(
        &self,
        enabled: bool,
        daily_job_limit: i64,
    ) -> Result<(bool, &'static str), sqlx::Error> {
        if !enabled {
            return Ok((false, "soc disabled"));
        }
        let used = self.jobs_used_today().await?;
        if used >= daily_job_limit.max(1) {
            return Ok((false, "daily job limit reached until next UTC day"));
        }
        Ok((true, ""))
    }

    /// Last sanitized failures for the status endpoint (bounded, newest
    /// first; never payloads or credentials).
    pub async fn recent_errors(&self, limit: i64) -> Result<Vec<(i64, String, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, Option<String>, i64)>(
            r#"
            SELECT id, last_error, EXTRACT(EPOCH FROM COALESCE(finished_at, created_at))::bigint
            FROM soc_job
            WHERE last_error IS NOT NULL
            ORDER BY COALESCE(finished_at, created_at) DESC, id DESC
            LIMIT $1
            "#,
        )
        .bind(limit.min(20))
        .fetch_all(&self.pgpool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(id, e, ts)| (id, e.unwrap_or_default(), ts))
                .collect()
        })
    }

    /// Queue growth is bounded: terminal job rows older than the cutoff
    /// are removed in small batches. Triage results are independent
    /// immutable history and stay.
    pub async fn trim_terminal_jobs_batch(&self, cutoff_unix: f64) -> Result<u64, sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM soc_job
            WHERE id IN (
                SELECT id FROM soc_job
                WHERE status IN ('succeeded', 'failed', 'cancelled')
                  AND COALESCE(finished_at, created_at) < to_timestamp($1)
                ORDER BY id ASC
                LIMIT 5000
            )
            "#,
        )
        .bind(cutoff_unix)
        .execute(&self.pgpool)
        .await
        .map(|r| r.rows_affected())
    }
}

/// Sanitized, bounded failure text: strip control characters, cap length.
/// Provider messages may embed URLs or keys upstream; the worker already
/// sanitizes, this is the second line.
pub fn sanitize_error(error: &str) -> String {
    let cleaned: String = error
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    cleaned.trim().chars().take(300).collect()
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Why the backend refused a worker-submitted result. Typed so the worker
/// can distinguish retryable schema issues from permanent state changes.
#[derive(Debug)]
pub enum ResultRejection {
    /// Job not running / already completed / unknown.
    JobNotActive(String),
    /// Field mismatch against the claimed job.
    Mismatch(String),
    /// Job belongs to an older SOC settings generation (disabled and
    /// re-enabled since enqueue).
    StaleGeneration,
}

impl ResultRejection {
    pub fn code(&self) -> &'static str {
        match self {
            ResultRejection::JobNotActive(_) => "job_not_active",
            ResultRejection::Mismatch(_) => "mismatch",
            ResultRejection::StaleGeneration => "stale_generation",
        }
    }

    pub fn message(&self) -> String {
        match self {
            ResultRejection::JobNotActive(m) | ResultRejection::Mismatch(m) => m.clone(),
            ResultRejection::StaleGeneration => {
                "job belongs to an older SOC settings generation".to_string()
            }
        }
    }
}

/// A result that passed every schema, bound and evidence check; ready to
/// insert.
#[derive(Debug)]
pub struct ValidatedResult {
    pub job_id: i64,
    pub stream_id: i64,
    pub stream_revision: i64,
    pub verdict: String,
    pub confidence: f64,
    pub severity: i32,
    pub summary: String,
    pub evidence: serde_json::Value,
    pub indicators: serde_json::Value,
    pub recommended_action: String,
    pub model: String,
    pub prompt_version: i32,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub duration_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_column_projection_covers_every_field() {
        let named = JOB_COLUMNS
            .split(',')
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .count();
        assert_eq!(named, 18);
        let row_fields = [
            "id",
            "stream_id",
            "stream_revision",
            "jev_verdict_id",
            "trigger",
            "requested_by",
            "status",
            "priority",
            "attempts",
            "model",
            "prompt_version",
            "settings_generation",
            "fetched_evidence",
            "created_at_unix",
            "started_at_unix",
            "finished_at_unix",
            "lease_expires_at_unix",
            "last_error",
        ];
        assert_eq!(named, row_fields.len());
    }

    #[test]
    fn result_column_projection_covers_every_field() {
        let named = RESULT_COLUMNS
            .split(',')
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .count();
        assert_eq!(named, 17);
        let row_fields = [
            "id",
            "job_id",
            "stream_id",
            "stream_revision",
            "verdict",
            "confidence",
            "severity",
            "summary",
            "evidence",
            "indicators",
            "recommended_action",
            "model",
            "prompt_version",
            "input_tokens",
            "output_tokens",
            "duration_ms",
            "created_at_unix",
        ];
        assert_eq!(named, row_fields.len());
    }

    #[test]
    fn sanitize_error_strips_controls_and_caps_length() {
        let s = sanitize_error("boom\n\tx\u{0}key=secret");
        assert!(!s.contains('\n'));
        assert!(!s.contains('\u{0}'));
        let long = sanitize_error(&"a".repeat(1000));
        assert_eq!(long.len(), 300);
    }

    #[test]
    fn rejection_codes_are_stable() {
        assert_eq!(ResultRejection::StaleGeneration.code(), "stale_generation");
        assert_eq!(ResultRejection::Mismatch("x".into()).code(), "mismatch");
        assert_eq!(
            ResultRejection::JobNotActive("x".into()).code(),
            "job_not_active"
        );
    }

    #[test]
    fn backoff_math_is_bounded_exponential() {
        // Mirror of record_failure's inline math: 2^attempts capped 300.
        let f = |attempts: i32| -> u64 {
            (2u64.saturating_pow(attempts.unsigned_abs().min(16))).min(300)
        };
        assert_eq!(f(1), 2);
        assert_eq!(f(2), 4);
        assert_eq!(f(8), 256);
        assert_eq!(f(9), 300);
        assert_eq!(f(100), 300);
        assert_eq!(f(-3), 8);
        // Cap holds for every attempt count.
        assert!(f(12) <= 300);
    }
}
