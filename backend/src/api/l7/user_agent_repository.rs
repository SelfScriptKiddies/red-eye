use sqlx::PgPool;

use super::user_agent_dto::{UaCursorKey, UaOrder, UaSort, UserAgentItem};

pub struct UserAgentRepository {
    pgpool: PgPool,
}

/// Sort-key columns per sort mode. Count sorts by
/// (message_count, last_seen, user_agent), last_seen by
/// (last_seen, message_count, user_agent); the exact value is always the
/// deterministic final tiebreaker, and the cursor predicate compares the
/// SAME tuple the ORDER BY uses.
fn sort_columns(sort: UaSort) -> &'static str {
    match sort {
        UaSort::Count => "g.message_count, g.last_seen, g.user_agent",
        UaSort::LastSeen => "g.last_seen, g.message_count, g.user_agent",
    }
}

fn cursor_key_tuple(sort: UaSort) -> &'static str {
    match sort {
        UaSort::Count => "(g.message_count, g.last_seen, g.user_agent)",
        UaSort::LastSeen => "(g.last_seen, g.message_count, g.user_agent)",
    }
}

/// Row-value comparison operator matching the sort direction: PostgreSQL
/// row comparison is lexicographic in the natural element order, so `>`
/// with an all-DESC key (or `<` with an all-ASC key) is the strict
/// "next page" predicate. Same keyset pattern as stream_repository.
fn cursor_op(order: UaOrder) -> &'static str {
    match order {
        UaOrder::Desc => ">",
        UaOrder::Asc => "<",
    }
}

/// Direction suffix for one ORDER BY element.
fn dir(order: UaOrder) -> &'static str {
    match order {
        UaOrder::Desc => "DESC",
        UaOrder::Asc => "ASC",
    }
}

impl UserAgentRepository {
    pub fn new(pgpool: PgPool) -> Self {
        Self { pgpool }
    }

    /// PK probe, so an unknown service is a plain client error and never
    /// falls through to a scan.
    pub async fn service_exists(&self, service_id: i64) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM service WHERE id = $1)")
            .bind(service_id)
            .fetch_one(&self.pgpool)
            .await
    }

    /// First successful aggregate write (analyzer-written). None while the
    /// value is unset or empty: aggregation has not started.
    pub async fn aggregation_started(&self) -> Result<Option<i64>, sqlx::Error> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT value FROM app_settings WHERE key = 'http_user_agent_aggregation_start'",
        )
        .fetch_optional(&self.pgpool)
        .await?;
        Ok(raw.and_then(|v| v.trim().parse::<i64>().ok()))
    }

    /// One keyset page of grouped ordinary values. The aggregate groups
    /// live in this small table only; no HTTP partition is touched.
    pub async fn list_user_agents(
        &self,
        service_id: i64,
        agent_id: Option<&str>,
        sort: UaSort,
        order: UaOrder,
        limit: i64,
        cursor: Option<&UaCursorKey>,
    ) -> Result<Vec<UserAgentItem>, sqlx::Error> {
        let mut sql = String::from(
            r#"
            SELECT g.user_agent, g.message_count, g.first_seen, g.last_seen, g.agent_ids
            FROM (
                SELECT b.user_agent AS user_agent,
                       SUM(b.message_count)::bigint AS message_count,
                       MIN(b.first_seen)::bigint AS first_seen,
                       MAX(b.last_seen)::bigint AS last_seen,
                       array_agg(DISTINCT b.agent_id) AS agent_ids
                FROM http_user_agent_bucket b
                WHERE b.service_id = $1
                  AND ($2::varchar IS NULL OR b.agent_id = $2)
                  AND b.is_overflow = FALSE
                GROUP BY b.user_agent
            ) g
            "#,
        );

        if cursor.is_some() {
            sql.push_str(&format!(
                " WHERE {} {} ($4::bigint, $5::bigint, $6::text) ",
                cursor_key_tuple(sort),
                cursor_op(order)
            ));
        }

        let ordered = sort_columns(sort)
            .split(", ")
            .map(|col| format!("{} {}", col, dir(order)))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" ORDER BY {} LIMIT $3 ", ordered));

        let mut query = sqlx::query_as::<_, (String, i64, i64, i64, Vec<String>)>(&sql)
            .bind(service_id)
            .bind(agent_id)
            .bind(limit);
        if let Some(key) = cursor {
            query = query
                .bind(key.message_count)
                .bind(key.last_seen)
                .bind(&key.user_agent);
        }
        let rows = query.fetch_all(&self.pgpool).await?;
        Ok(rows
            .into_iter()
            .map(
                |(user_agent, message_count, first_seen, last_seen, agent_ids)| UserAgentItem {
                    user_agent: Some(user_agent),
                    message_count,
                    first_seen,
                    last_seen,
                    agent_ids,
                    is_overflow: false,
                },
            )
            .collect())
    }

    /// The overflow aggregate for the same scope: one summary item whose
    /// value stays in the database, plus a presence flag that is reported
    /// on every page.
    pub async fn overflow_summary(
        &self,
        service_id: i64,
        agent_id: Option<&str>,
    ) -> Result<Option<UserAgentItem>, sqlx::Error> {
        let (count, first, last, agents, present): (i64, i64, i64, Vec<String>, bool) =
            sqlx::query_as(
                r#"
                SELECT COALESCE(SUM(b.message_count), 0)::bigint,
                       COALESCE(MIN(b.first_seen), 0)::bigint,
                       COALESCE(MAX(b.last_seen), 0)::bigint,
                       COALESCE(array_agg(DISTINCT b.agent_id), '{}'::varchar[]),
                       count(*) > 0
                FROM http_user_agent_bucket b
                WHERE b.service_id = $1
                  AND ($2::varchar IS NULL OR b.agent_id = $2)
                  AND b.is_overflow = TRUE
                "#,
            )
            .bind(service_id)
            .bind(agent_id)
            .fetch_one(&self.pgpool)
            .await?;
        if !present {
            return Ok(None);
        }
        Ok(Some(UserAgentItem {
            user_agent: None,
            message_count: count,
            first_seen: first,
            last_seen: last,
            agent_ids: agents,
            is_overflow: true,
        }))
    }

    /// Delete buckets whose whole window is at or before the cutoff, in
    /// bounded batches (bucket list subquery LIMIT). Returns deleted rows.
    pub async fn delete_expired_buckets(
        &self,
        cutoff: i64,
        batch_size: i64,
    ) -> Result<u64, sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM http_user_agent_bucket
            WHERE bucket_start IN (
                SELECT bucket_start FROM http_user_agent_bucket
                WHERE bucket_start <= $1
                ORDER BY bucket_start
                LIMIT $2
            )
            "#,
        )
        .bind(cutoff)
        .bind(batch_size)
        .execute(&self.pgpool)
        .await
        .map(|r| r.rows_affected())
    }

    /// Rows still expired past the cutoff (cleanup lag metric).
    pub async fn count_expired_buckets(&self, cutoff: i64) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT count(*) FROM http_user_agent_bucket WHERE bucket_start <= $1")
            .bind(cutoff)
            .fetch_one(&self.pgpool)
            .await
    }

    /// Rows still present for a deleted service (bounded-cleanup lag).
    pub async fn count_service_buckets(&self, service_id: i64) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT count(*) FROM http_user_agent_bucket WHERE service_id = $1")
            .bind(service_id)
            .fetch_one(&self.pgpool)
            .await
    }

    /// Bounded-batch cleanup of one service's aggregate rows (service
    /// deletion). Returns deleted rows; the caller loops until 0.
    pub async fn delete_service_buckets(
        &self,
        service_id: i64,
        batch_size: i64,
    ) -> Result<u64, sqlx::Error> {
        sqlx::query(
            r#"
            DELETE FROM http_user_agent_bucket
            WHERE service_id = $1
              AND ctid IN (
                SELECT ctid FROM http_user_agent_bucket
                WHERE service_id = $1
                LIMIT $2
            )
            "#,
        )
        .bind(service_id)
        .bind(batch_size)
        .execute(&self.pgpool)
        .await
        .map(|r| r.rows_affected())
    }
}
