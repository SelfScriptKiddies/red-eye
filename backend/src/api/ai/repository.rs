use serde::Serialize;
use sqlx::PgPool;

/// Verdict persistence. Only VALIDATED responses reach insert; the UNIQUE
/// identity rule makes duplicate storage for one revision impossible
/// (insert returns None when the identity already exists).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct AiVerdictRow {
    pub id: i64,
    pub stream_id: i64,
    pub first_packet_id: i64,
    pub last_packet_id: i64,
    pub model: String,
    pub prompt_version: i32,
    pub transcript_sha256: String,
    pub attack_class: String,
    pub confidence: f32,
    pub severity: f32,
    pub is_attack: bool,
    pub ai_threshold: f32,
    pub answers: serde_json::Value,
    pub input_tokens: Option<i64>,
    pub cost: Option<f32>,
    pub analyzed_packet_count: i32,
    pub omitted_packet_count: i32,
    pub omitted_byte_count: i64,
    pub truncated: bool,
    #[sqlx(default)]
    pub created_at_unix: i64,
}

const VERDICT_COLUMNS: &str = "id, stream_id, first_packet_id, last_packet_id, model, prompt_version, transcript_sha256, \
     attack_class, confidence, severity, is_attack, ai_threshold, answers, input_tokens, cost, \
     analyzed_packet_count, omitted_packet_count, omitted_byte_count, truncated, \
     EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix";

/// Keyset listing SQL with dynamically numbered parameters: $1 cursor,
/// optional $2 stream_id, last param LIMIT. Bounded by limit; the
/// (stream_id, id DESC) index serves the stream page, the PK the rest.
fn list_sql(with_stream: bool, only_attacks: bool) -> String {
    let mut sql = format!("SELECT {} FROM ai_verdict WHERE id < $1", VERDICT_COLUMNS);
    let mut next = 2;
    if with_stream {
        sql.push_str(&format!(" AND stream_id = ${}", next));
        next += 1;
    }
    if only_attacks {
        sql.push_str(" AND is_attack = true");
    }
    sql.push_str(&format!(" ORDER BY id DESC LIMIT ${}", next));
    sql
}

#[derive(Debug)]
pub struct NewVerdict<'a> {
    pub stream_id: i64,
    pub first_packet_id: i64,
    pub last_packet_id: i64,
    pub model: &'a str,
    pub prompt_version: i32,
    pub transcript_sha256: &'a str,
    pub attack_class: &'a str,
    pub confidence: f64,
    pub severity: f64,
    pub is_attack: bool,
    pub ai_threshold: f64,
    pub answers: &'a serde_json::Value,
    pub input_tokens: Option<i64>,
    pub cost: Option<f64>,
    pub analyzed_packet_count: i32,
    pub omitted_packet_count: i32,
    pub omitted_byte_count: i64,
    pub truncated: bool,
}

pub struct AiRepository {
    pgpool: PgPool,
}

impl AiRepository {
    pub fn new(pgpool: PgPool) -> Self {
        AiRepository { pgpool }
    }

    /// Insert one verdict; Ok(None) means the identity was already stored
    /// (concurrent duplicate or replay after restart) - nothing written.
    pub async fn insert_verdict(&self, v: &NewVerdict<'_>) -> Result<Option<i64>, sqlx::Error> {
        let id: Option<i64> = sqlx::query_scalar(
            r#"
            INSERT INTO ai_verdict (
                stream_id, first_packet_id, last_packet_id, model, prompt_version,
                transcript_sha256, attack_class, confidence, severity, is_attack,
                ai_threshold, answers, input_tokens, cost, analyzed_packet_count,
                omitted_packet_count, omitted_byte_count, truncated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (stream_id, last_packet_id, model, prompt_version) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(v.stream_id)
        .bind(v.first_packet_id)
        .bind(v.last_packet_id)
        .bind(v.model)
        .bind(v.prompt_version)
        .bind(v.transcript_sha256)
        .bind(v.attack_class)
        .bind(v.confidence as f32)
        .bind(v.severity as f32)
        .bind(v.is_attack)
        .bind(v.ai_threshold as f32)
        .bind(v.answers)
        .bind(v.input_tokens)
        .bind(v.cost.map(|c| c as f32))
        .bind(v.analyzed_packet_count)
        .bind(v.omitted_packet_count)
        .bind(v.omitted_byte_count)
        .bind(v.truncated)
        .fetch_optional(&self.pgpool)
        .await?;
        Ok(id)
    }

    /// Bounded keyset listing: cursor is the previous page's last id,
    /// `stream_id` narrows to one stream's history, `only_attacks` to
    /// attack verdicts. A stream's latest verdict is
    /// `list_verdicts(Some(stream_id), false, 1, None)` - the same page
    /// query the stream view badge uses.
    pub async fn list_verdicts(
        &self,
        stream_id: Option<i64>,
        only_attacks: bool,
        limit: i64,
        cursor: Option<i64>,
    ) -> Result<Vec<AiVerdictRow>, sqlx::Error> {
        // Params are numbered dynamically so the optional stream filter
        // never shifts the LIMIT parameter (a static $3 would bind the
        // stream id into LIMIT when present, or leave a hole when absent).
        let sql = list_sql(stream_id.is_some(), only_attacks);
        let mut query = sqlx::query_as::<_, AiVerdictRow>(&sql).bind(cursor.unwrap_or(i64::MAX));
        if let Some(sid) = stream_id {
            query = query.bind(sid);
        }
        query.bind(limit).fetch_all(&self.pgpool).await
    }

    pub async fn get_verdict(&self, id: i64) -> Result<Option<AiVerdictRow>, sqlx::Error> {
        let sql = format!("SELECT {} FROM ai_verdict WHERE id = $1", VERDICT_COLUMNS);
        sqlx::query_as::<_, AiVerdictRow>(&sql)
            .bind(id)
            .fetch_optional(&self.pgpool)
            .await
    }

    /// Recorded provider cost of the current UTC day plus today's verdict
    /// count. NULL costs (provider sent no usage) are skipped here: the
    /// scheduler adds its conservative estimates for those separately.
    pub async fn cost_today(&self) -> Result<(f64, i64), sqlx::Error> {
        sqlx::query_as::<_, (f64, i64)>(
            r#"
            SELECT COALESCE(SUM(cost)::float8, 0), COUNT(*)
            FROM ai_verdict
            WHERE (created_at AT TIME ZONE 'UTC')::date = (now() AT TIME ZONE 'UTC')::date
            "#,
        )
        .fetch_one(&self.pgpool)
        .await
    }

    /// Bounded retention delete: one batch of verdict rows older than the
    /// cutoff. Never blocks anything; callers loop in small batches.
    pub async fn delete_expired_batch(&self, cutoff_unix: i64) -> Result<u64, sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM ai_verdict
            WHERE id IN (
                SELECT id FROM ai_verdict
                WHERE EXTRACT(EPOCH FROM created_at) < $1
                ORDER BY id ASC
                LIMIT 5000
            )
            "#,
        )
        .bind(cutoff_unix as f64)
        .execute(&self.pgpool)
        .await
        .map(|r| r.rows_affected())
    }

    /// Totals for the status endpoint (bounded window = whole table; the
    /// table is retention-bounded by design).
    pub async fn totals(&self) -> Result<(i64, i64), sqlx::Error> {
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_attack) FROM ai_verdict",
        )
        .fetch_one(&self.pgpool)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_sql_numbers_params_dynamically() {
        // The optional stream filter must not shift the LIMIT parameter.
        let plain = list_sql(false, false);
        assert!(plain.contains("WHERE id < $1"));
        assert!(plain.contains("LIMIT $2"));
        assert!(!plain.contains("$3"));

        let streamed = list_sql(true, false);
        assert!(streamed.contains("AND stream_id = $2"));
        assert!(streamed.contains("LIMIT $3"));

        let attacks = list_sql(true, true);
        assert!(attacks.contains("AND is_attack = true"));
        assert!(attacks.contains("LIMIT $3"));
    }

    #[test]
    fn verdict_column_projection_covers_every_field() {
        // SQL strings are not compile-checked: keep the projection and the
        // DTO in lockstep by counting fields on both sides.
        let named = VERDICT_COLUMNS
            .split(',')
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .count();
        // EXTRACT(...) AS created_at_unix counts as one column.
        assert_eq!(named, 20);
        let row_fields = [
            "id",
            "stream_id",
            "first_packet_id",
            "last_packet_id",
            "model",
            "prompt_version",
            "transcript_sha256",
            "attack_class",
            "confidence",
            "severity",
            "is_attack",
            "ai_threshold",
            "answers",
            "input_tokens",
            "cost",
            "analyzed_packet_count",
            "omitted_packet_count",
            "omitted_byte_count",
            "truncated",
            "created_at_unix",
        ]
        .len();
        assert_eq!(named, row_fields);
    }
}
