use crate::api::stream::stream_dto::{Stream, StreamListRow};
use sqlx::PgPool;

pub struct StreamRepository {
    pgpool: PgPool,
}

impl StreamRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        StreamRepository { pgpool }
    }

    pub async fn get_packet_by_id(&self, stream_id: i64) -> Result<Stream, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol, agent_id,
                   favorite_revision, archived_through_packet_id, retention_state, continuation_of, segment_index, retention_bytes, archive_error
            FROM stream
            WHERE id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    /// The unstarred continuation created by segment rotation, if any.
    pub async fn get_next_segment_id(&self, stream_id: i64) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query_scalar("SELECT id FROM stream WHERE continuation_of = $1 ORDER BY id LIMIT 1")
            .bind(stream_id)
            .fetch_optional(&self.pgpool)
            .await
    }

    pub async fn get_all_streams(&self) -> Result<Vec<Stream>, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol, agent_id,
                   favorite_revision, archived_through_packet_id, retention_state, continuation_of, segment_index, retention_bytes, archive_error
            FROM stream
            "#)
            .fetch_all(&self.pgpool)
            .await
    }

    /// Streams list with tags. Reads `stream` LEFT JOIN aggregated
    /// `stream_tag` (maintained incrementally by the traffic-analyzer flush)
    /// instead of the old streams_list_view materialized view — no history
    /// recomputation, so the list can't freeze on statement_timeout at game
    /// volumes. Output shape is IDENTICAL to the old matview-based query
    /// (same columns, same types, same INNER service join).
    ///
    /// Pagination: keyset-first. `cursor` is the opaque "last_time:id" of the
    /// last row of the previous page; the predicate
    /// `(last_time, stream_id) < (cursor)` + `ORDER BY last_time DESC, id DESC`
    /// is a stable page even while `last_time` keeps changing under load
    /// (OFFSET pagination on a continuously-updated sort key re-serves
    /// shifted rows on every refetch). The row-value comparison uses the
    /// expression index idx_stream_last_time_id (see
    /// database/migrations/stream_keyset_index.sql). `offset` is kept for
    /// legacy callers (deprecated) and ignored when `cursor` is set.
    ///
    /// `favorite` is a server-side SQL predicate applied BEFORE keyset
    /// pagination (served by the partial idx_stream_favorite_list), so the
    /// favorites view paginates over all matching streams, not one page.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_streams_with_tags(
        &self,
        limit: i64,
        offset: i64,
        cursor: Option<(i64, i64)>,
        tags: Option<Vec<i64>>,
        services: Option<Vec<i64>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        favorite: Option<bool>,
    ) -> Result<Vec<StreamListRow>, sqlx::Error> {
        let mut sql = String::from(
            r#"
            SELECT
                stream_id,
                first_time,
                last_time,
                packet_count::bigint,
                COALESCE(tags, ARRAY[]::text[]) as tags,
                service_name,
                service_id,
                favorite,
                l7_protocol,
                src_ip::text,
                dst_ip::text,
                agent_id,
                retention_state,
                segment_index,
                retention_bytes,
                continuation_of
            FROM (
                SELECT
                    str.id AS stream_id,
                    COALESCE(str.first_packet_time, 0) AS first_time,
                    COALESCE(str.last_packet_time, 0) AS last_time,
                    str.packet_count,
                    st.tags AS tags,
                    s.name AS service_name,
                    s.id AS service_id,
                    str.favorite,
                    str.l7_protocol,
                    str.src_ip::text AS src_ip,
                    str.dst_ip::text AS dst_ip,
                    str.agent_id::text AS agent_id,
                    str.retention_state AS retention_state,
                    str.segment_index AS segment_index,
                    str.retention_bytes AS retention_bytes,
                    str.continuation_of AS continuation_of
                FROM stream str
                JOIN service s ON s.id = str.service_id
                LEFT JOIN LATERAL (
                    SELECT array_agg(DISTINCT stg.tag_name) AS tags
                    FROM stream_tag stg
                    WHERE stg.stream_id = str.id
                ) st ON true
            ) streams_list"#,
        );

        let mut where_clauses = Vec::new();
        let mut param_idx: usize = 0;

        // Tag filtering via subquery against tag_name (tags are stored as text[])
        if let Some(ref tag_ids) = tags {
            if !tag_ids.is_empty() {
                param_idx += 1;
                where_clauses.push(format!(
                    r#"EXISTS (
                        SELECT 1 FROM unnest(tags) t
                        JOIN tag_name tn ON tn.name = t
                        WHERE tn.id = ANY(${param_idx})
                        HAVING COUNT(DISTINCT tn.id) = array_length(${param_idx}, 1)
                    )"#
                ));
            }
        }

        // Service filtering
        if let Some(ref service_ids) = services {
            if !service_ids.is_empty() {
                param_idx += 1;
                where_clauses.push(format!("service_id = ANY(${param_idx})"));
            }
        }

        // Server-side favorites filter: composed with tags/services/time
        // and applied before keyset pagination.
        if let Some(favorite) = favorite {
            where_clauses.push(format!("favorite = {}", favorite));
        }

        // Time filtering on the view's aggregated columns
        if start_time.is_some() {
            param_idx += 1;
            where_clauses.push(format!("last_time >= ${param_idx}"));
        }
        if end_time.is_some() {
            param_idx += 1;
            where_clauses.push(format!("first_time <= ${param_idx}"));
        }

        // Keyset cursor: strictly older than the last row of the previous
        // page. Row-value comparison on the SAME expressions as the ORDER BY
        // (COALESCE(last_packet_time,0), id) so the expression index
        // idx_stream_last_time_id serves both the filter and the sort.
        if cursor.is_some() {
            param_idx += 1;
            let cursor_time_idx = param_idx;
            param_idx += 1;
            let cursor_id_idx = param_idx;
            where_clauses.push(format!(
                "(last_time, stream_id) < (${cursor_time_idx}, ${cursor_id_idx})"
            ));
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        param_idx += 1;
        let limit_idx = param_idx;
        param_idx += 1;
        let offset_idx = param_idx;
        // `stream_id` tiebreaker makes even the legacy OFFSET path
        // deterministic when several rows share one last_time.
        sql.push_str(&format!(
            " ORDER BY last_time DESC, stream_id DESC LIMIT ${limit_idx} OFFSET ${offset_idx}"
        ));

        log::debug!("SQL Query: {}", sql);
        log::debug!(
            "Params: tags={:?}, services={:?}, start_time={:?}, end_time={:?}, cursor={:?}, limit={}, offset={}",
            tags,
            services,
            start_time,
            end_time,
            cursor,
            limit,
            offset
        );

        let mut query = sqlx::query_as::<_, StreamListRow>(&sql);

        // Bind tag IDs
        if let Some(ref tag_ids) = tags {
            if !tag_ids.is_empty() {
                query = query.bind(tag_ids);
            }
        }

        // Bind service IDs
        if let Some(ref service_ids) = services {
            if !service_ids.is_empty() {
                query = query.bind(service_ids);
            }
        }

        // Bind time params
        if let Some(start) = start_time {
            query = query.bind(start);
        }
        if let Some(end) = end_time {
            query = query.bind(end);
        }

        // Bind cursor params (before limit/offset, matching clause order)
        if let Some((cursor_last_time, cursor_id)) = cursor {
            query = query.bind(cursor_last_time).bind(cursor_id);
        }

        // Bind limit and offset (offset is 0 on the keyset path)
        let effective_offset = if cursor.is_some() { 0 } else { offset };
        query = query.bind(limit).bind(effective_offset);

        query.fetch_all(&self.pgpool).await
    }

    /// Histogram over time buckets: TOTAL packet count + flag-trigger count
    /// per bucket (no per-service split — the timeline shows two series,
    /// packets and flags). Packets are counted from the `packets` table
    /// (idx_packets_timestamp); flags are tag triggers whose tag_name has
    /// special_type='flag'. FULL JOIN so buckets with only flags (or only
    /// packets) are kept.
    pub async fn get_stream_histogram(
        &self,
        bucket_secs: i32,
    ) -> Result<Vec<(i64, i64, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            WITH packets AS (
                SELECT (timestamp / $1) * $1 AS bucket_ts, COUNT(*) AS cnt
                FROM packets
                GROUP BY bucket_ts
            ),
            flags AS (
                SELECT (t.timestamp / $1) * $1 AS bucket_ts, COUNT(*) AS cnt
                FROM tag t
                JOIN tag_name tn ON tn.id = t.tag_id
                WHERE tn.special_type = 'flag'
                GROUP BY bucket_ts
            )
            SELECT COALESCE(p.bucket_ts, f.bucket_ts) AS bucket_ts,
                   COALESCE(p.cnt, 0) AS packet_count,
                   COALESCE(f.cnt, 0) AS flag_count
            FROM packets p
            FULL OUTER JOIN flags f ON f.bucket_ts = p.bucket_ts
            ORDER BY bucket_ts ASC
            "#,
        )
        .bind(bucket_secs as i64)
        .fetch_all(&self.pgpool)
        .await
    }
}
