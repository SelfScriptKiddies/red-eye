use sqlx::PgPool;
use crate::api::stream::stream_dto::Stream;

pub struct StreamRepository {
    pgpool: PgPool
}

impl StreamRepository {
    pub(super) fn new(pgpool: PgPool) -> Self {
        StreamRepository {
            pgpool
        }
    }

    pub async fn get_packet_by_id(&self, stream_id: i32) -> Result<Stream, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol
            FROM stream
            WHERE id = $1
            "#)
            .bind(stream_id)
            .fetch_one(&self.pgpool)
            .await
    }

    pub async fn get_all_streams(&self) -> Result<Vec<Stream>, sqlx::Error> {
        sqlx::query_as::<_, Stream>(
            r#"
            SELECT id, src_ip, src_port, dst_ip, dst_port, protocol, active, packet_count, favorite, l7_protocol
            FROM stream
            "#)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn get_streams_with_tags(&self, limit: i32, offset: i32, tags: Option<Vec<i32>>, services: Option<Vec<i32>>, start_time: Option<i64>, end_time: Option<i64>) -> Result<Vec<(i32, Option<i64>, Option<i64>, i64, Vec<String>, Option<String>, Option<i32>, bool, String, Option<String>, Option<String>)>, sqlx::Error> {
        let mut sql = String::from(r#"
            SELECT
                stream_id,
                first_time,
                last_time,
                packet_count,
                COALESCE(tags, ARRAY[]::text[]) as tags,
                service_name,
                service_id,
                favorite,
                l7_protocol,
                src_ip::text,
                dst_ip::text
            FROM streams_list_view"#);

        let mut where_clauses = Vec::new();
        let mut param_idx: usize = 0;

        // Tag filtering via subquery against tag_name (view stores names as text[])
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

        // Time filtering on the view's aggregated columns
        if start_time.is_some() {
            param_idx += 1;
            where_clauses.push(format!("last_time >= ${param_idx}"));
        }
        if end_time.is_some() {
            param_idx += 1;
            where_clauses.push(format!("first_time <= ${param_idx}"));
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        param_idx += 1;
        let limit_idx = param_idx;
        param_idx += 1;
        let offset_idx = param_idx;
        sql.push_str(&format!(
            " ORDER BY last_time DESC LIMIT ${limit_idx} OFFSET ${offset_idx}"
        ));

        log::debug!("SQL Query: {}", sql);
        log::debug!("Params: tags={:?}, services={:?}, start_time={:?}, end_time={:?}, limit={}, offset={}", tags, services, start_time, end_time, limit, offset);

        let mut query = sqlx::query_as::<_, (i32, Option<i64>, Option<i64>, i64, Vec<String>, Option<String>, Option<i32>, bool, String, Option<String>, Option<String>)>(&sql);

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

        // Bind limit and offset
        query = query.bind(limit).bind(offset);

        query.fetch_all(&self.pgpool).await
    }

    pub async fn get_stream_histogram(&self, bucket_secs: i32) -> Result<Vec<(i64, i32, String, i64)>, sqlx::Error> {
        sqlx::query_as::<_, (i64, i32, String, i64)>(
            r#"
            SELECT (last_time / $1) * $1 AS bucket_ts,
                   service_id,
                   service_name,
                   COUNT(*) AS count
            FROM streams_list_view
            WHERE last_time > 0
            GROUP BY bucket_ts, service_id, service_name
            ORDER BY bucket_ts ASC
            "#)
            .bind(bucket_secs as i64)
            .fetch_all(&self.pgpool)
            .await
    }

    pub async fn set_favorite(&self, stream_id: i32, favorite: bool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar::<_, bool>(
            r#"
            UPDATE stream
            SET favorite = $2
            WHERE id = $1
            RETURNING favorite
            "#,
        )
        .bind(stream_id)
        .bind(favorite)
        .fetch_one(&self.pgpool)
        .await
    }
}


